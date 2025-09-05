# main.py — Fiber Reports (summary + service + telephony via Bitrix voximplant.statistic.get)
import asyncio, html, json, logging, os
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

REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))  # опційно

# Мапа операторів телефонії (PORTAL_USER_ID -> Name)
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "").strip()
try:
    TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else {}
except Exception:
    TELEPHONY_OPERATORS = {}

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
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "INTERNAL_SERVER_ERROR"):
                        log.warning("Bitrix temp error: %s (%s), retry #%s", err, desc, attempt+1)
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
        payload = dict(params)
        payload["start"] = start
        res = await b24(method, **payload)
        chunk = res or []
        # деякі методи повертають {'items': [...]} — підтримуємо це
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        if isinstance(chunk, dict) and "result" in chunk:
            chunk = chunk.get("result", [])
        if isinstance(chunk, list):
            out.extend(chunk)
        else:
            # на випадок коли метод повертає не список — вийдемо
            break
        # Bitrix REST пагінація: якщо менше page_size — значить кінець
        if len(chunk) < page_size:
            break
        start += page_size
        if throttle:
            await asyncio.sleep(throttle)
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
    name = (type_name or "").strip().lower()
    if not name and _DEAL_TYPE_MAP:
        name = (_DEAL_TYPE_MAP.get(type_id, "") or "").strip().lower()
    return (
        name in ("підключення", "подключение")
        or ("підключ" in name)
        or ("подключ" in name)
    )

async def _connection_type_ids() -> List[str]:
    m = await get_deal_type_map()
    return [tid for tid, nm in m.items() if _is_connection(tid, nm)]

# Бригадні стадії в кат.20
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}
_BRIGADE_STAGE_FULL = {f"C20:{v}" for v in _BRIGADE_STAGE.values()}

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
    global _CAT0_STAGES
    if _CAT0_STAGES is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_STAGE_0"})
        _CAT0_STAGES = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] CAT0 stages: %s", len(_CAT0_STAGES))
    return _CAT0_STAGES

async def _resolve_cat0_stage_ids() -> Tuple[str, str]:
    st = await _cat0_stages()
    exact_id = None; think_id = None
    for sid, nm in st.items():
        n = (nm or "").strip().lower()
        if n == "на конкретний день": exact_id = sid
        if n == "думають": think_id = sid
    if not exact_id: exact_id = "5"         # fallback
    if not think_id: think_id = "DETAILS"   # fallback
    return f"C0:{exact_id}", f"C0:{think_id}"

async def _count_open_in_stage(cat_id: int, stage_full: str, type_ids: Optional[List[str]] = None) -> int:
    flt: Dict[str, Any] = {"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": stage_full}
    if type_ids: flt["TYPE_ID"] = type_ids
    deals = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    if deals: return len(deals)
    # fallback: короткий STAGE_ID
    short = stage_full.split(":", 1)[-1]
    flt["STAGE_ID"] = short
    deals_fb = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    return len(deals_fb)

# ------------------------ Telephony (Bitrix REST) --------
# --- Телефонія (Bitrix telephony.statistic.list) -----------------------------

# мапа операторів з ENV (як уже робили)
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "").strip()
TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else {}
VIRTUAL_MANAGER_NAME = "Віртуальний Менеджер"

def _kyiv_day_bounds_strings(offset_days: int = 0) -> Tuple[str, str]:
    """Повертає строки 'YYYY-MM-DD HH:MM:SS' у часовій зоні Києва для CALL_START_DATE."""
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start_local.strftime(fmt), end_local.strftime(fmt)

async def _b24_tel_stat_list(day_from: str, day_to: str) -> List[Dict[str, Any]]:
    """Витягує ВСІ записи телефонії за діапазон (CALL_START_DATE) з пагінацією."""
    start = 0
    page_size = 50
    items: List[Dict[str, Any]] = []
    while True:
        res = await b24(
            "telephony.statistic.list",
            FILTER={
                ">=CALL_START_DATE": day_from,
                "<CALL_START_DATE": day_to,
            },
            ORDER={"CALL_START_DATE": "ASC"},
            start=start,
        )
        chunk = res or []
        # у деяких порталах результат приходить як {"items":[...], "next":N}
        if isinstance(chunk, dict):
            items.extend(chunk.get("items", []))
            nxt = chunk.get("next")
            if nxt is None:
                break
            start = nxt
        else:
            items.extend(chunk)
            if len(chunk) < page_size:
                break
            start += page_size
    return items

def _name_for_uid(uid: Optional[int]) -> str:
    if uid is None or int(uid) <= 0:
        return VIRTUAL_MANAGER_NAME
    return TELEPHONY_OPERATORS.get(str(uid), f"ID {uid}")

def _is_incoming(row: Dict[str, Any]) -> bool:
    # CALL_TYPE: 1=incoming, 2=outgoing, 3=callback
    return int(row.get("CALL_TYPE", 0)) == 1

def _is_outgoing(row: Dict[str, Any]) -> bool:
    return int(row.get("CALL_TYPE", 0)) == 2

def _answered(row: Dict[str, Any]) -> bool:
    # Найнадійніше — CALL_DURATION > 0 або CALL_STATUS == 200
    dur = int(row.get("CALL_DURATION", 0) or 0)
    st = int(row.get("CALL_STATUS", 0) or 0)
    return dur > 0 or st == 200

def _missed(row: Dict[str, Any]) -> bool:
    # Вхідні, не прийняті (duration==0 або статус не 200)
    if not _is_incoming(row):
        return False
    return not _answered(row)

async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    day_from, day_to = _kyiv_day_bounds_strings(offset_days)
    rows = await _b24_tel_stat_list(day_from, day_to)
    log.info("[telephony] fetched %s calls for %s..%s", len(rows), day_from, day_to)

    missed = 0
    incoming_answered = 0
    outgoing_total = 0

    # Розбивки по операторам
    per_op_incoming: Dict[str, int] = {}
    per_op_outgoing: Dict[str, int] = {}

    for r in rows:
        uid_raw = r.get("PORTAL_USER_ID")
        # Bitrix інколи віддає строку
        uid = int(uid_raw) if str(uid_raw).strip() not in ("", "None", "null") else 0
        op_name = _name_for_uid(uid)

        if _is_incoming(r):
            if _missed(r):
                missed += 1
                # пропущені не кладемо в «вхідні (прийняті)»
            else:
                incoming_answered += 1
                per_op_incoming[op_name] = per_op_incoming.get(op_name, 0) + 1

        elif _is_outgoing(r):
            outgoing_total += 1
            per_op_outgoing[op_name] = per_op_outgoing.get(op_name, 0) + 1

        # інші типи (callback) ігноруємо або додай окремо, якщо потрібно

    return {
        "missed": missed,
        "incoming_ok": incoming_answered,
        "outgoing": outgoing_total,
        "per_op_in": dict(sorted(per_op_incoming.items(), key=lambda x: (-x[1], x[0]))),
        "per_op_out": dict(sorted(per_op_outgoing.items(), key=lambda x: (-x[1], x[0]))),
    }

def format_telephony_block(t: Dict[str, Any]) -> str:
    lines = [
        "📞 <b>Телефонія</b>",
        f"🔕 Пропущених: <b>{t['missed']}</b>",
        f"📥 Вхідних (прийнятих): <b>{t['incoming_ok']}</b>",
        f"📤 Вихідних: <b>{t['outgoing']}</b>",
        "",
    ]
    if t["per_op_in"]:
        lines.append("👥 По операторам (вхідні):")
        for name, cnt in list(t["per_op_in"].items()):
            lines.append(f"• {name}: {cnt}")
        lines.append("")
    if t["per_op_out"]:
        lines.append("👥 По операторам (вихідні):")
        for name, cnt in list(t["per_op_out"].items()):
            lines.append(f"• {name}: {cnt}")
    return "\n".join(lines).rstrip()


# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    type_map = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()

    # A) "🆕 Подали сьогодні":
    #   A1: кат.0, стадія "На конкретний день", TYPE=Підключення, DATE_CREATE у діапазоні
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()
    created_c0_exact = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={
            "CATEGORY_ID": 0,
            "STAGE_ID": c0_exact_stage,
            "TYPE_ID": conn_type_ids,
            ">=DATE_CREATE": frm, "<DATE_CREATE": to,
        },
        select=["ID"]
    )
    #   A2: кат.20, переміщені у будь-яку бригадну стадію сьогодні (DATE_MODIFY),
    #       TYPE=Підключення
    created_to_brigades = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "TYPE_ID": conn_type_ids,
            ">=DATE_MODIFY": frm, "<DATE_MODIFY": to,
        },
        select=["ID"]
    )
    created_conn = len(created_c0_exact) + len(created_to_brigades)

    # B) "✅ Закрили сьогодні" — по CLOSEDATE, тільки підключення у кат.20, стадія WON
    closed_list = await b24_list(
        "crm.deal.list",
        order={"CLOSEDATE": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": "C20:WON",
            "TYPE_ID": conn_type_ids,
            ">=CLOSEDATE": frm, "<CLOSEDATE": to,
        },
        select=["ID"]
    )
    closed_conn = len(closed_list)

    # C) "📊 Активні на бригадах" — відкриті у бригадних, тип=Підключення
    active_open = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={
            "CLOSED": "N",
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "TYPE_ID": conn_type_ids,
            "STAGE_SEMANTIC_ID": "P",
        },
        select=["ID"]
    )
    active_conn = len(active_open)

    # D) Категорія 0: відкриті у "На конкретний день" та "Думають"
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    # E) Сервісні заявки (подані сьогодні): усе, що потрапило в бригадні стадії у кат.20 СЬОГОДНІ, але тип != Підключення
    service_today = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "!TYPE_ID": conn_type_ids,              # «все, крім підключення»
            ">=DATE_MODIFY": frm, "<DATE_MODIFY": to,
        },
        select=["ID"]
    )
    service_created_cnt = len(service_today)

    # F) Телефонія за день
    calls = await fetch_calls_today(offset_days)
    tel_aggr = aggregate_telephony(calls)

    log.info(
        "[summary] created(today)=%s (c0_exact=%s + to_brigades=%s), "
        "closed=%s, active=%s, exact=%s, think=%s, service_today=%s, "
        "tel(missed=%s,in=%s,out=%s)",
        len(created_c0_exact) + len(created_to_brigades), len(created_c0_exact), len(created_to_brigades),
        closed_conn, active_conn, exact_cnt, think_cnt, service_created_cnt,
        tel_aggr["missed"], tel_aggr["incoming_answered"], tel_aggr["outgoing"]
    )

    return {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "service": {"created_today": service_created_cnt},
        "telephony": tel_aggr,
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; c0 = d["cat0"]; s = d["service"]; t = d["telephony"]

    parts = []
    parts.append(f"🗓 <b>Дата: {dl}</b>")
    parts.append("")
    parts.append("━━━━━━━━━━━━━━━")
    parts.append("📌 <b>Підключення</b>")
    parts.append(f"🆕 Подали: <b>{c['created']}</b>")
    parts.append(f"✅ Закрили: <b>{c['closed']}</b>")
    parts.append(f"📊 Активні на бригадах: <b>{c['active']}</b>")
    parts.append("")
    parts.append(f"📅 На конкретний день: <b>{c0['exact_day']}</b>")
    parts.append(f"💭 Думають: <b>{c0['think']}</b>")
    parts.append("━━━━━━━━━━━━━━━")
    parts.append("⚙️ <b>Сервісні заявки</b>")
    parts.append(f"📨 Подали сьогодні: <b>{s['created_today']}</b>")
    parts.append(format_telephony(dl, t))
    return "\n".join(parts)

# ------------------------ Send helpers -------------------
async def _safe_send(chat_id: int, text: str):
    for attempt in range(7):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            msg = str(e)
            retry_after = None
            if "retry after " in msg.lower():
                try:
                    retry_after = int(msg.lower().split("retry after ")[1].split()[0])
                except Exception:
                    retry_after = None
            wait = retry_after if retry_after else min(30, 2 ** attempt)
            log.warning("telegram send failed: %s, waiting %ss (try #%s)", e, wait, attempt+1)
            await asyncio.sleep(wait)
    log.error("telegram send failed permanently (chat %s)", chat_id)

async def send_company_summary_to_chat(target_chat: int, offset_days: int = 0) -> None:
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(target_chat, format_company_summary(data))
    except Exception as e:
        log.exception("company summary failed for chat %s", target_chat)
        await _safe_send(target_chat, f"❗️Помилка формування сумарного звіту:\n<code>{html.escape(str(e))}</code>")

async def send_company_summary(offset_days: int = 0) -> None:
    if REPORT_SUMMARY_CHAT:
        await send_company_summary_to_chat(REPORT_SUMMARY_CHAT, offset_days)
    else:
        log.warning("REPORT_SUMMARY_CHAT is not configured")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    """
    /report_now           — сумарний за сьогодні в цей же чат
    /report_now 1         — сумарний за вчора в цей же чат
    """
    parts = (m.text or "").split()
    offset = 0
    if len(parts) >= 2 and parts[1].lstrip("-").isdigit():
        offset = int(parts[1])

    await m.answer("🔄 Формую сумарний звіт…")
    # 1) завжди відповідаємо в той чат, де команда
    await send_company_summary_to_chat(m.chat.id, offset)
    # 2) додатково — у службовий чат, якщо налаштований і він інший
    if REPORT_SUMMARY_CHAT and REPORT_SUMMARY_CHAT != m.chat.id:
        await send_company_summary_to_chat(REPORT_SUMMARY_CHAT, offset)
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
