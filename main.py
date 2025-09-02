# main.py — reports-bot (з комбінованим “дівчат” звітом)
import asyncio
import html
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import BotCommand, Message, Update
from zoneinfo import ZoneInfo

# ------------------------ Settings ------------------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
BITRIX_WEBHOOK_BASE = os.environ["BITRIX_WEBHOOK_BASE"].rstrip("/")
WEBHOOK_BASE = os.environ["WEBHOOK_BASE"].rstrip("/")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "secret")

REPORT_TZ_NAME = os.environ.get("REPORT_TZ", "Europe/Kyiv")
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")  # HH:MM

_raw_report_chats = os.environ.get("REPORT_CHATS", "")
REPORT_CHATS: Dict[str, int] = json.loads(_raw_report_chats) if _raw_report_chats.strip() else {}
REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))  # якщо 0 — беремо REPORT_CHATS["all"]

B24_DOMAIN = os.environ.get("B24_DOMAIN", "").strip()  # опційно для лінків

# ------------------------ Logging -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("report_bot")

# ------------------------ App/Bot -------------------------
app = FastAPI()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
HTTP: aiohttp.ClientSession

# ------------------------ Health --------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ------------------------ Bitrix helpers ------------------
async def _sleep_backoff(attempt: int, base: float = 0.5, cap: float = 8.0):
    await asyncio.sleep(min(cap, base * (2 ** attempt)))

async def b24(method: str, **params) -> Any:
    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"
    for attempt in range(6):
        try:
            async with HTTP.post(url, json=params) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"]; desc = data.get("error_description")
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS"):
                        log.warning("Bitrix rate-limit: %s (%s), retry #%s", err, desc, attempt+1)
                        await _sleep_backoff(attempt); continue
                    raise RuntimeError(f"B24 error: {err}: {desc}")
                return data.get("result")
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")

async def b24_list(method: str, *, page_size: int = 200, throttle: float = 0.15, **params) -> List[Dict[str, Any]]:
    start = 0
    out: List[Dict[str, Any]] = []
    while True:
        payload = dict(params); payload["start"] = start
        res = await b24(method, **payload)
        chunk = res or []
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        out.extend(chunk)
        if len(chunk) < page_size: break
        start += page_size
        if throttle: await asyncio.sleep(throttle)
    return out

# ------------------------ Caches / mappings ---------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] DEAL_TYPE: %s", len(_DEAL_TYPE_MAP))
    return _DEAL_TYPE_MAP

# класифікація назв типів у наші кошики
def normalize_type(type_name: str) -> str:
    t = (type_name or "").strip().lower()
    mapping_exact = {
        "підключення": "connection", "подключение": "connection",
        "ремонт": "repair",
        "сервісні роботи": "service", "сервисные работы": "service", "сервіс": "service", "сервис": "service",
        "перепідключення": "reconnection", "переподключение": "reconnection",
        "аварія": "accident", "авария": "accident",
        "будівництво": "construction", "строительство": "construction",
        "роботи по лінії": "linework", "работы по линии": "linework",
        "звернення в кц": "cc_request", "обращение в кц": "cc_request",
        "не выбран": "other", "не вибрано": "other", "інше": "other", "прочее": "other",
    }
    if t in mapping_exact: return mapping_exact[t]
    if any(k in t for k in ("підключ", "подключ")): return "connection"
    if "ремонт" in t: return "repair"
    if any(k in t for k in ("сервіс", "сервис")): return "service"
    if any(k in t for k in ("перепідключ", "переподключ")): return "reconnection"
    if "авар" in t: return "accident"
    if any(k in t for k in ("будівниц", "строит")): return "construction"
    if any(k in t for k in ("ліні", "линии")): return "linework"
    if any(k in t for k in ("кц", "контакт-центр", "колл-центр", "call")): return "cc_request"
    return "other"

# “кошики” для рядків звіту
BUCKETS = [
    ("connection", "📡 Підключення"),
    ("service", "🛠️ Сервіс"),
    ("reconnection", "🔄 Перепідключення"),
    # нижче — додаткові, використовуються у розширених метриках
    ("accident", "⚠️ Аварії"),
    ("cc_request", "📞 Пропущені / КЦ"),
]

# бригадні колонки (щоб порахувати “Активні” сервісні)
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}
_BRIGADE_EXEC_OPTION_ID = {1: 5494, 2: 5496, 3: 5498, 4: 5500, 5: 5502}

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ Helpers for buckets -------------
async def _bucket_codes() -> Dict[str, List[str]]:
    """
    Повертає {bucket_key: [TYPE_ID,...]} згідно з поточною довідниковою мапою Bitrix.
    """
    m = await get_deal_type_map()
    out: Dict[str, List[str]] = {}
    for code, name in m.items():
        cls = normalize_type(name)
        out.setdefault(cls, []).append(code)
    return out

# ------------------------ Brigade daily report (already had) ---------------
async def build_daily_report(brigade: int, offset_days: int) -> Tuple[str, Dict[str, int], int]:
    label, frm, to = _day_bounds(offset_days)
    m = await get_deal_type_map()
    exec_opt = _BRIGADE_EXEC_OPTION_ID.get(brigade)
    filter_closed = {"STAGE_ID": "C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to}
    if exec_opt: filter_closed["UF_CRM_1611995532420"] = exec_opt
    closed = await b24_list("crm.deal.list", order={"DATE_MODIFY":"ASC"}, filter=filter_closed, select=["ID","TYPE_ID"])

    counts = {"connection":0,"repair":0,"service":0,"reconnection":0,"accident":0,"construction":0,"linework":0,"cc_request":0,"other":0}
    for d in closed:
        tname = m.get(d.get("TYPE_ID") or "", "")
        counts[normalize_type(tname)] = counts.get(normalize_type(tname),0)+1

    stage_code = _BRIGADE_STAGE[brigade]
    active = await b24_list("crm.deal.list", order={"ID":"DESC"}, filter={"CLOSED":"N","STAGE_ID":f"C20:{stage_code}"}, select=["ID"])
    return label, counts, len(active)

def format_brigade_report(brigade: int, date_label: str, counts: Dict[str, int], active_left: int) -> str:
    total = sum(counts.get(k,0) for k,_ in BUCKETS)
    return "\n".join([
        f"<b>👷 Бригада №{brigade} — {date_label}</b>",
        "",
        f"<b>✅ Закрито задач:</b> {total}",
        f"{' | '.join([f'{title} — {counts.get(key,0)}' for key,title in BUCKETS[:3]])}",
        "",
        f"<b>📌 Активних задач у колонці бригади:</b> {active_left}",
    ])

# ------------------------ Company summary (“дівчат” звіт) ------------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    """
    Збірка комбінованого звіту по CATEGORY_ID=20:
      - “Всього подали” = створені за добу (DATE_CREATE у діапазоні)
      - “Закрили” = переміщені у WON за добу (DATE_MODIFY у діапазоні)
      - Сервіс: додатково відкриті, активні (у бригадних стадіях), сервісні виїзди (відкриті з UF_CRM_1611995532420 ненуль)
      - Пропущені (cc_request): створені за добу
      - Аварії: created/closed за добу
    """
    label, frm, to = _day_bounds(offset_days)
    codes = await _bucket_codes()

    # створені за добу
    created_all = await b24_list(
        "crm.deal.list",
        order={"ID":"DESC"},
        filter={">=DATE_CREATE": frm, "<DATE_CREATE": to, "CATEGORY_ID": 20},
        select=["ID","TYPE_ID","STAGE_ID","CLOSED","UF_CRM_1611995532420"],
    )

    # закриті (WON) за добу
    closed_all = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY":"ASC"},
        filter={"STAGE_ID":"C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to},
        select=["ID","TYPE_ID"],
    )

    # відкриті сервісні (для extended метрик)
    open_service = await b24_list(
        "crm.deal.list",
        order={"ID":"DESC"},
        filter={"CLOSED":"N","CATEGORY_ID":20, "TYPE_ID": codes.get("service", [])},
        select=["ID","STAGE_ID","UF_CRM_1611995532420"],
    )

    # рахунки по кошиках
    def _count_created(bucket: str) -> int:
        valid = set(codes.get(bucket, []))
        return sum(1 for d in created_all if (d.get("TYPE_ID") in valid))
    def _count_closed(bucket: str) -> int:
        valid = set(codes.get(bucket, []))
        return sum(1 for d in closed_all if (d.get("TYPE_ID") in valid))

    # сервіс розширений
    service_open_total = len(open_service)
    brigade_stage_ids = {f"C20:{v}" for v in _BRIGADE_STAGE.values()}
    service_active = sum(1 for d in open_service if (str(d.get("STAGE_ID") or "") in brigade_stage_ids))
    service_trips = sum(1 for d in open_service if (d.get("UF_CRM_1611995532420") or []))

    # пропущені / КЦ (за добу створено)
    cc_created = _count_created("cc_request")

    # аварії: X/Y (закрито/створено за добу)
    accidents_created = _count_created("accident")
    accidents_closed = _count_closed("accident")

    return {
        "date_label": label,
        "connections": {"created": _count_created("connection"), "closed": _count_closed("connection")},
        "service": {
            "created": _count_created("service"),
            "closed": _count_closed("service"),
            "open_total": service_open_total,
            "active": service_active,
            "service_trips": service_trips,
            "moved": 0,          # немає надійного критерію — залишаю 0
            "overdue": 0,        # потребує окремого поля-дедлайну — залишаю 0
        },
        "reconnections": {"created": _count_created("reconnection"), "closed": _count_closed("reconnection")},
        "cc_requests": cc_created,
        "accidents": {"created": accidents_created, "closed": accidents_closed},
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; s = d["service"]; r = d["reconnections"]
    cc = d["cc_requests"]; acc = d["accidents"]

    lines = [
        f"<b>📆 Дата: {dl}</b>",
        "",
        "📌 <b>Підключення</b>",
        f"Всього подали — <b>{c['created']}</b>",
        f"Закрили — <b>{c['closed']}</b>",
        # “Запл. на завтра” — ігноруємо
        "",
        "📌 <b>Сервіс</b>",
        f"Подали — <b>{s['created']}</b>",
        f"Закрили — <b>{s['closed']}</b>",
        "",
        f"Не закритих всього — <b>{s['open_total']}</b>",
        f"Активних — <b>{s['active']}</b>",
        f"Сервісний виїзд — <b>{s['service_trips']}</b>",
        f"Перенесених — <b>{s['moved']}</b>",
        f"Протермінованих — <b>{s['overdue']}</b>",
        "",
        "📌 <b>Перепідключення</b>",
        f"Закрили — <b>{r['closed']}</b>",
        # “Запл. на завтра” — ігноруємо
        "",
        f"📞 Пропущені — <b>{cc}</b>",
        f"⚠️ Аварії — <b>{acc['closed']}</b>/<b>{acc['created']}</b>",
    ]
    return "\n".join(lines)

# ------------------------ Send helpers -------------------
async def _safe_send(chat_id: int, text: str):
    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            log.warning("telegram send failed: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    log.error("telegram send failed permanently")

def _resolve_chat_for_brigade(b: int) -> Optional[int]:
    if str(b) in REPORT_CHATS: return int(REPORT_CHATS[str(b)])
    if b in REPORT_CHATS: return int(REPORT_CHATS[b])  # якщо JSON передали числами
    if "all" in REPORT_CHATS: return int(REPORT_CHATS["all"])
    return None

def _resolve_summary_chat() -> Optional[int]:
    if REPORT_SUMMARY_CHAT: return REPORT_SUMMARY_CHAT
    if "all" in REPORT_CHATS: return int(REPORT_CHATS["all"])
    return None

async def send_all_brigades_report(offset_days: int = 0) -> None:
    tasks = []
    for b in (1,2,3,4,5):
        chat_id = _resolve_chat_for_brigade(b)
        if not chat_id:
            log.warning("No chat configured for brigade %s", b); continue
        async def run(b_=b, chat_=chat_id):
            label, counts, left = await build_daily_report(b_, offset_days)
            await _safe_send(chat_, format_brigade_report(b_, label, counts, left))
        tasks.append(run())
    if tasks: await asyncio.gather(*tasks, return_exceptions=True)

async def send_company_summary(offset_days: int = 0) -> None:
    chat_id = _resolve_summary_chat()
    if not chat_id:
        log.warning("No REPORT_SUMMARY_CHAT nor REPORT_CHATS['all'] configured")
        return
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(chat_id, format_company_summary(data))
    except Exception:
        log.exception("company summary failed")
        await _safe_send(chat_id, "❗️Помилка формування комбінованого звіту")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    """
    /report_now            — бригадні звіти + комбінований
    /report_now 1          — за вчора
    /report_now summary    — тільки комбінований за сьогодні
    /report_now summary 1  — тільки комбінований за вчора
    """
    parts = (m.text or "").split()
    if len(parts) >= 2 and parts[1].lower() == "summary":
        offset = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
        await m.answer("🔄 Формую комбінований звіт…")
        await send_company_summary(offset)
        await m.answer("✅ Готово")
        return

    offset = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
    await m.answer("🔄 Формую звіти…")
    await asyncio.gather(send_all_brigades_report(offset), send_company_summary(offset))
    await m.answer("✅ Готово")

# ------------------------ Scheduler ----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
    hh, mm = map(int, REPORT_TIME.split(":", 1))
    now_local = now_utc.astimezone(REPORT_TZ)
    target_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target_local <= now_local: target_local += timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def scheduler_loop():
    log.info("[scheduler] started")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            nxt = _next_run_dt(now_utc)
            sleep_sec = max(1, (nxt - now_utc).total_seconds())
            log.info("[scheduler] next run at %s in %ss", nxt.isoformat(), int(sleep_sec))
            await asyncio.sleep(sleep_sec)
            log.info("[scheduler] tick -> sending daily (brigades + summary)")
            await asyncio.gather(send_all_brigades_report(0), send_company_summary(0))
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ---------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    await bot.set_my_commands([
        BotCommand(command="report_now", description="Ручний запуск звітів (/report_now [summary] [offset])"),
    ])
    url = f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}"
    await bot.set_webhook(url)
    asyncio.create_task(scheduler_loop())
    log.info("[startup] webhook set to %s", url)

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook()
    await HTTP.close()
    await bot.session.close()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        return {"ok": False}
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}
