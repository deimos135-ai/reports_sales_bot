# main.py — Fiber Reports (summary-only)
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

# Куди слати сумарний звіт
REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))

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

def _is_connection(type_id: str, type_name: Optional[str] = None) -> bool:
    """Визначає, чи TYPE_ID належить до 'Підключення'."""
    name = (type_name or "").strip().lower()
    if not name and _DEAL_TYPE_MAP:
        name = (_DEAL_TYPE_MAP.get(type_id, "") or "").strip().lower()
    if name in ("підключення", "подключение"):
        return True
    return ("підключ" in name) or ("подключ" in name)

# Бригадні стадії у категорії 20 (для активних)
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ CAT0 stage resolving ------------
_CAT0_STAGES: Optional[Dict[str, str]] = None
async def _cat0_stages() -> Dict[str, str]:
    """
    Вертає мапу status_id->name для категорії 0 (ENTITY_ID='DEAL_STAGE_0').
    """
    global _CAT0_STAGES
    if _CAT0_STAGES is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_STAGE_0"})
        _CAT0_STAGES = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] CAT0 stages: %s", len(_CAT0_STAGES))
    return _CAT0_STAGES

async def _resolve_cat0_stage_ids() -> Tuple[str, str]:
    """
    Знаходимо повні ідентифікатори стадій для:
      - На конкретний день
      - Думають
    Повертаємо у вигляді 'C0:<STATUS_ID>' (наприклад 'C0:5' / 'C0:DETAILS').
    """
    st = await _cat0_stages()
    exact_id = None
    think_id = None

    for sid, nm in st.items():
        n = (nm or "").strip().lower()
        if n == "на конкретний день":
            exact_id = sid
        if n == "думають":
            think_id = sid

    # safety fallback по відомим SID (з твого дампу)
    if not exact_id:
        exact_id = "5"
    if not think_id:
        think_id = "DETAILS"

    return f"C0:{exact_id}", f"C0:{think_id}"

async def _count_open_in_stage(cat_id: int, stage_full: str) -> int:
    """
    Рахує відкриті угоди у заданій стадії.
    1) Питаємо з повним STAGE_ID (типу 'C0:5')
    2) Якщо 0 — пробуємо ті самі фільтри зі стисненим STAGE_ID ('5')
    Це покриває обидві поведінки Bitrix.
    """
    deals = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": stage_full},
        select=["ID"],
    )
    if deals:
        return len(deals)

    short = stage_full.split(":", 1)[-1]
    deals_fb = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": short},
        select=["ID"],
    )
    return len(deals_fb)

# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    """
    Підключення:
      - created_conn: скільки створено сьогодні у CATEGORY_ID=20 з типом "Підключення"
      - closed_conn: скільки сьогодні закрито (WON) у CATEGORY_ID=20 з типом "Підключення"
      - active_conn: скільки зараз відкрито у CATEGORY_ID=20 з типом "Підключення" саме в бригадних стадіях
    Категорія 0:
      - exact_day: відкритих у «На конкретний день»
      - think: відкритих у «Думають»
    """
    label, frm, to = _day_bounds(offset_days)
    type_map = await get_deal_type_map()

    # 1) created today (cat20, by TYPE)
    created_conn = 0
    created = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={">=DATE_CREATE": frm, "<DATE_CREATE": to, "CATEGORY_ID": 20},
        select=["ID", "TYPE_ID"],
    )
    for d in created:
        tid = d.get("TYPE_ID") or ""
        if _is_connection(tid, type_map.get(tid)):
            created_conn += 1

    # 2) closed today (cat20 WON) by TYPE
    closed_conn = 0
    closed = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={"STAGE_ID": "C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to},
        select=["ID", "TYPE_ID"],
    )
    for d in closed:
        tid = d.get("TYPE_ID") or ""
        if _is_connection(tid, type_map.get(tid)):
            closed_conn += 1

    # 3) active on brigades (cat20 open in brigade columns) by TYPE
    brigade_stage_ids = {f"C20:{v}" for v in _BRIGADE_STAGE.values()}
    active_conn = 0
    open_cat20 = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={"CLOSED": "N", "CATEGORY_ID": 20},
        select=["ID", "TYPE_ID", "STAGE_ID"],
    )
    for d in open_cat20:
        if str(d.get("STAGE_ID") or "") in brigade_stage_ids:
            tid = d.get("TYPE_ID") or ""
            if _is_connection(tid, type_map.get(tid)):
                active_conn += 1

    # 4) category 0 counts
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage)
    think_cnt = await _count_open_in_stage(0, c0_think_stage)

    return {
        "date_label": label,
        "connections": {
            "created": created_conn,
            "closed": closed_conn,
            "active": active_conn,
        },
        "cat0": {
            "exact_day": exact_cnt,
            "think": think_cnt,
        },
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; c0 = d["cat0"]

    lines = [
        f"📅 <b>Дата: {dl}</b>",
        "",
        "📌 <b>Підключення</b>",
        f"Всього подали (кат.20) — <b>{c['created']}</b>",
        f"Закрили сьогодні (кат.20) — <b>{c['closed']}</b>",
        f"Активних на бригадах (кат.20) — <b>{c['active']}</b>",
        "",
        f"На конкретний день (кат.0) — <b>{c0['exact_day']}</b>",
        f"Думають (кат.0) — <b>{c0['think']}</b>",
    ]
    return "\n".join(lines)

# ------------------------ Send helpers -------------------
async def _safe_send(chat_id: int, text: str):
    for attempt in range(6):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            log.warning("telegram send failed: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    log.error("telegram send failed permanently")

def _summary_chat() -> Optional[int]:
    return REPORT_SUMMARY_CHAT or None

async def send_company_summary(offset_days: int = 0) -> None:
    chat_id = _summary_chat()
    if not chat_id:
        log.warning("REPORT_SUMMARY_CHAT is not configured")
        return
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(chat_id, format_company_summary(data))
    except Exception:
        log.exception("company summary failed")
        await _safe_send(chat_id, "❗️Помилка формування сумарного звіту")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    """
    /report_now           — сумарний за сьогодні
    /report_now 1         — сумарний за вчора
    """
    parts = (m.text or "").split()
    offset = 0
    if len(parts) >= 2 and parts[1].lstrip("-").isdigit():
        offset = int(parts[1])
    await m.answer("🔄 Формую сумарний звіт…")
    await send_company_summary(offset)
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
            log.info("[scheduler] tick -> sending summary")
            await send_company_summary(0)
        except Exception:
            log.exception("[scheduler] loop error")
            await asyncio.sleep(5)

# ------------------------ Webhook plumbing ---------------
@app.on_event("startup")
async def on_startup():
    global HTTP
    HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    await bot.set_my_commands([
        BotCommand(command="report_now", description="Сумарний звіт (/report_now [offset])"),
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
