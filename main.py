# main.py — Fiber Reports (summary + service + telephony with stricter dedup & mapping)
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

# Телефонія: оператори (user_id -> display name)
_DEFAULT_OPERATOR_MAP = {
    "238":   "Яна Тищенко",
    "1340":  "Вероніка Дроботя",
    "3356":  "Вікторія Крамаренко",
    "9294":  "Евеліна Безсмертна",
    "10000": "Руслана Писанка",
    "130":   "Олена Михайленко",
}
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "").strip()
try:
    TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else _DEFAULT_OPERATOR_MAP
    TELEPHONY_OPERATORS = {str(k): v for k, v in TELEPHONY_OPERATORS.items()}
except Exception as e:
    TELEPHONY_OPERATORS = _DEFAULT_OPERATOR_MAP
    logging.getLogger("report_bot").warning("TELEPHONY_OPERATORS JSON parse failed: %s — using defaults", e)

TELEPHONY_SHOW_OTHER = os.environ.get("TELEPHONY_SHOW_OTHER", "false").lower() in ("1","true","yes","y")
DEBUG_TEL = os.environ.get("DEBUG_TEL", "0") == "1"

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

# ------------------------ Deal type helpers ---------------
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
    return (name in ("підключення", "подключение") or ("підключ" in name) or ("подключ" in name))

async def _connection_type_ids() -> List[str]:
    m = await get_deal_type_map()
    return [tid for tid, nm in m.items() if _is_connection(tid, nm)]

# ------------------------ Brigade stages ------------------
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

def _day_bounds_local_strings(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    label = start_local.strftime("%d.%m.%Y")
    fmt = "%Y-%m-%d %H:%M:%S"
    return label, start_local.strftime(fmt), end_local.strftime(fmt)

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
    if not exact_id: exact_id = "5"
    if not think_id: think_id = "DETAILS"
    return f"C0:{exact_id}", f"C0:{think_id}"

async def _count_open_in_stage(cat_id: int, stage_full: str, type_ids: Optional[List[str]] = None) -> int:
    flt: Dict[str, Any] = {"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": stage_full}
    if type_ids: flt["TYPE_ID"] = type_ids
    deals = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    if deals: return len(deals)
    short = stage_full.split(":", 1)[-1]
    flt["STAGE_ID"] = short
    deals_fb = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    return len(deals_fb)

# ------------------------ Company summary -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm_utc, to_utc = _day_bounds(offset_days)
    type_map = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()

    # A) Подали сьогодні (Підключення)
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()
    created_c0_exact = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={
            "CATEGORY_ID": 0,
            "STAGE_ID": c0_exact_stage,
            "TYPE_ID": conn_type_ids,
            ">=DATE_CREATE": frm_utc, "<DATE_CREATE": to_utc,
        },
        select=["ID"]
    )
    created_to_brigades = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "TYPE_ID": conn_type_ids,
            ">=DATE_MODIFY": frm_utc, "<DATE_MODIFY": to_utc,
        },
        select=["ID"]
    )
    created_conn = len(created_c0_exact) + len(created_to_brigades)

    # B) Закрили сьогодні (CLOSEDATE у C20:WON)
    closed_list = await b24_list(
        "crm.deal.list",
        order={"CLOSEDATE": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": "C20:WON",
            "TYPE_ID": conn_type_ids,
            ">=CLOSEDATE": frm_utc, "<CLOSEDATE": to_utc,
        },
        select=["ID"]
    )
    closed_conn = len(closed_list)

    # C) Активні на бригадах
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

    # D) Кат.0 «На конкретний день» / «Думають»
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    # E) Сервіс (НЕ підключення) — сьогодні в бригадні
    service_submitted_list = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "!TYPE_ID": conn_type_ids,
            ">=DATE_MODIFY": frm_utc, "<DATE_MODIFY": to_utc,
        },
        select=["ID"]
    )
    service_submitted = len(service_submitted_list)

    log.info("[summary] created_conn=%s, closed_conn=%s, active_conn=%s, exact=%s, think=%s, service_submitted=%s",
             created_conn, closed_conn, active_conn, exact_cnt, think_cnt, service_submitted)

    return {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "service": {"submitted": service_submitted},
    }

# ------------------------ Telephony via Bitrix ------------
# ------------------------ Telephony via Bitrix (improved) ------------
_MISSED_CODES = {"NO_ANSWER", "MISSED", "304", "480", "486", "487", "603"}

def _ctype_val(x: Any) -> str:
    s = str(x or "").upper()
    if s in {"1", "INCOMING"}: return "INCOMING"
    if s in {"2", "OUTGOING"}: return "OUTGOING"
    return s

def _disp_name(uid: str) -> str:
    return TELEPHONY_OPERATORS.get(uid, None) or (f"Інші (тех.)" if not TELEPHONY_SHOW_OTHER else f"ID {uid}")

def _is_known_op(uid: str) -> bool:
    return uid in TELEPHONY_OPERATORS

async def build_telephony_stats(offset_days: int = 0) -> Dict[str, Any]:
    _, frm_local, to_local = _day_bounds_local_strings(offset_days)

    rows = await b24_list(
        "voximplant.statistic.get",
        order={"CALL_START_DATE": "ASC"},
        filter={">=CALL_START_DATE": frm_local, "<CALL_START_DATE": to_local},
        select=[
            "CALL_ID","CALL_SESSION_ID","CALL_TYPE","CALL_DURATION",
            "PORTAL_USER_ID","CALL_START_DATE","CALL_FAILED_CODE"
        ]
    )

    # 1) групуємо по CALL_ID
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cid = str(r.get("CALL_ID") or r.get("CALL_SESSION_ID") or "")
        if not cid:
            # якщо зовсім немає ідентифікатора — пропускаємо
            continue
        groups.setdefault(cid, []).append(r)

    def best_record(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        # a) відповіли: duration>0, є оператор → max duration
        answered = [x for x in items if int(x.get("CALL_DURATION") or 0) > 0]
        answered_with_user = [x for x in answered if str(x.get("PORTAL_USER_ID") or "").strip()]
        if answered_with_user:
            return max(answered_with_user, key=lambda x: int(x.get("CALL_DURATION") or 0))
        if answered:
            return max(answered, key=lambda x: int(x.get("CALL_DURATION") or 0))
        # b) пропущені: код з переліку
        missed = [x for x in items if str(x.get("CALL_FAILED_CODE") or "").upper().replace(" ", "_") in _MISSED_CODES]
        if missed:
            # візьмемо найпізніший по часу як репрезентативний
            return sorted(missed, key=lambda x: str(x.get("CALL_START_DATE") or ""))[-1]
        # c) фолбек — найпізніший
        return sorted(items, key=lambda x: str(x.get("CALL_START_DATE") or ""))[-1]

    uniq = [best_record(v) for v in groups.values()]

    # 2) класифікація
    missed_total = 0
    incoming_total = 0          # прийняті
    incoming_all = 0            # всі вхідні (контроль)
    outgoing_total = 0

    incoming_by_op: Dict[str, int] = {}
    outgoing_by_op: Dict[str, int] = {}

    for r in uniq:
        ctype = _ctype_val(r.get("CALL_TYPE"))
        dur = int(r.get("CALL_DURATION") or 0)
        failed = str(r.get("CALL_FAILED_CODE") or "").upper().replace(" ", "_")
        uid = str(r.get("PORTAL_USER_ID") or "").strip()

        if ctype == "INCOMING":
            incoming_all += 1
            is_missed = (dur == 0) or (failed in _MISSED_CODES)
            if is_missed:
                missed_total += 1
            else:
                incoming_total += 1
                if _is_known_op(uid):
                    name = _disp_name(uid)
                    incoming_by_op[name] = incoming_by_op.get(name, 0) + 1

        elif ctype == "OUTGOING":
            outgoing_total += 1
            if _is_known_op(uid):
                name = _disp_name(uid)
                outgoing_by_op[name] = outgoing_by_op.get(name, 0) + 1

    log.info(
        "[telephony] uniq_calls=%s, missed=%s, incoming_answered=%s, outgoing=%s, incoming_all=%s",
        len(uniq), missed_total, incoming_total, outgoing_total, incoming_all
    )

    return {
        "missed_total": missed_total,
        "incoming_total": incoming_total,
        "incoming_all": incoming_all,
        "outgoing_total": outgoing_total,
        "incoming_by_operator": incoming_by_op,
        "outgoing_by_operator": outgoing_by_op,
    }

# ------------------------ Formatting ----------------------
def format_company_summary(d: Dict[str, Any], tel: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c, c0, s = d["connections"], d["cat0"], d["service"]
    t = tel
    lines = [
        f"🗓 <b>Дата: {dl}</b>",
        "",
        "━━━━━━━━━━━━━━━",
        "📌 <b>Підключення</b>",
        f"🆕 Подали: <b>{c['created']}</b>",
        f"✅ Закрили: <b>{c['closed']}</b>",
        f"📊 Активні на бригадах: <b>{c['active']}</b>",
        "",
        f"📅 На конкретний день: <b>{c0['exact_day']}</b>",
        f"💭 Думають: <b>{c0['think']}</b>",
        "━━━━━━━━━━━━━━━",
        "⚙️ <b>Сервісні заявки</b>",
        f"📨 Подали сьогодні: <b>{s['submitted']}</b>",
        "━━━━━━━━━━━━━━━",
        "📞 <b>Телефонія</b>",
        f"🔕 Пропущених: <b>{t['missed_total']}</b>",
        f"📥 Вхідних (прийнятих): <b>{t['incoming_total']}</b>",
        f"📤 Вихідних: <b>{t['outgoing_total']}</b>",
        f"📥 Усі вхідні (контроль): <b>{t['incoming_all']}</b>",
    ]

    if t.get("incoming_by_operator"):
        lines.append("")
        lines.append("👥 По операторам (вхідні):")
        for name, cnt in sorted(t["incoming_by_operator"].items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"• {name}: {cnt}")

    if t.get("outgoing_by_operator"):
        lines.append("")
        lines.append("👥 По операторам (вихідні):")
        for name, cnt in sorted(t["outgoing_by_operator"].items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"• {name}: {cnt}")

    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)

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
        tel = await build_telephony_stats(offset_days)
        await _safe_send(target_chat, format_company_summary(data, tel))
    except Exception as e:
        log.exception("company summary failed for chat %s", target_chat)
        await _safe_send(target_chat, f"❗️Помилка формування звіту:\n<code>{html.escape(str(e))}</code>")

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
    await send_company_summary_to_chat(m.chat.id, offset)
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
