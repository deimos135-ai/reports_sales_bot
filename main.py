# main.py — Fiber Reports (summary + service + telephony v2)
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
REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))

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

# ------------------------ CRM deal types ------------------
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
    return (name in ("підключення", "подключение")) or ("підключ" in name) or ("подключ" in name)

async def _connection_type_ids() -> List[str]:
    m = await get_deal_type_map()
    return [tid for tid, nm in m.items() if _is_connection(tid, nm)]

# ------------------------ Brigade stages -----------------
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

def _day_bounds_local_str(offset_days: int = 0) -> Tuple[str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    # робимо праву межу відкритою, тому нижче в API будемо використовувати "<"
    end_local = start_local + timedelta(days=1)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start_local.strftime(fmt), end_local.strftime(fmt)

# ------------------------ CAT0 stages --------------------
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

# ------------------------ Telephony ----------------------
_USER_NAME_CACHE: Dict[str, str] = {}
async def _get_user_name(uid: str) -> str:
    if not uid:
        return ""
    if uid in _USER_NAME_CACHE:
        return _USER_NAME_CACHE[uid]
    if uid in TELEPHONY_OPERATORS:
        _USER_NAME_CACHE[uid] = TELEPHONY_OPERATORS[uid]
        return _USER_NAME_CACHE[uid]
    try:
        info = await b24("user.get", ID=int(uid))
        if isinstance(info, list) and info:
            it = info[0]
            name = (it.get("NAME") or "").strip()
            last = (it.get("LAST_NAME") or "").strip()
            full = " ".join(p for p in [name, last] if p).strip() or (it.get("LOGIN") or "").strip()
            if full:
                _USER_NAME_CACHE[uid] = full
                return full
    except Exception:
        pass
    return f"ID {uid}"

async def _telephony_fetch(frm_local: str, to_local: str) -> List[Dict[str, Any]]:
    """
    Тягнемо дзвінки за добу з Bitrix:
      1) telephony.statistic.get (основний)
      2) voximplant.statistic.get (фолбек)
    Комбінуємо різні назви полів дат, права межа — відкрита (<).
    """
    methods = ["telephony.statistic.get", "voximplant.statistic.get"]
    date_filters = [
        {">=CALL_START_DATE": frm_local, "<CALL_START_DATE": to_local},
        {">=START_DATE": frm_local, "<START_DATE": to_local},
    ]
    last_err = None
    for method in methods:
        for flt in date_filters:
            try:
                recs = await b24_list(
                    method,
                    filter=flt,
                    select=[
                        "ID","CALL_TYPE","CALL_DURATION","CALL_STATUS","CALL_FAILED_CODE",
                        "PORTAL_USER_ID","PORTAL_USER_NAME"
                    ]
                )
                log.info("[telephony] %s %s -> %s recs", method, list(flt.keys()), len(recs))
                return recs
            except RuntimeError as e:
                msg = str(e)
                if "ERROR_METHOD_NOT_FOUND" in msg or "Method not found" in msg:
                    last_err = e
                    break
                raise
    if last_err:
        raise last_err
    return []

def _is_missed_call(rec: Dict[str, Any]) -> bool:
    """
    Bitrix UI показує 'Пропущенные' як вхідні без відповіді/помилки.
    Беремо ширше:
      - CALL_TYPE in {1,'INCOMING'}
      - і (CALL_DURATION == 0  або  CALL_STATUS ∈ {NO_ANSWER, ERROR, FAILED, DECLINED, ABANDONED, BUSY}
         або CALL_FAILED_CODE не порожній/не '0'/'OK')
    """
    ctype = str(rec.get("CALL_TYPE") or "").upper()
    dur = int(rec.get("CALL_DURATION") or 0)
    status = str(rec.get("CALL_STATUS") or "").upper()
    fcode = str(rec.get("CALL_FAILED_CODE") or "").upper()
    is_incoming = ctype in {"1", "INCOMING"}
    failed_by_status = status in {"NO_ANSWER", "ERROR", "FAILED", "DECLINED", "ABANDONED", "BUSY"}
    failed_by_code = (fcode and fcode not in {"0", "OK", "SUCCESS"})
    return is_incoming and (dur == 0 or failed_by_status or failed_by_code)

async def build_telephony_stats(offset_days: int = 0) -> Dict[str, Any]:
    frm_local, to_local = _day_bounds_local_str(offset_days)
    records = await _telephony_fetch(frm_local, to_local)

    # дебаг-розклад
    types_counter: Dict[str,int] = {}
    statuses: Dict[str,int] = {}
    for r in records:
        types_counter[str(r.get("CALL_TYPE"))] = types_counter.get(str(r.get("CALL_TYPE")),0)+1
        statuses[str(r.get("CALL_STATUS"))] = statuses.get(str(r.get("CALL_STATUS")),0)+1
    log.info("[telephony] types=%s statuses=%s", types_counter, statuses)

    missed_total = sum(1 for r in records if _is_missed_call(r))

    # вихідні по операторах
    per_op: Dict[str, int] = {}
    for r in records:
        ctype = str(r.get("CALL_TYPE") or "").upper()
        if ctype not in {"2", "OUTGOING"}:
            continue
        uid = str(r.get("PORTAL_USER_ID") or "")
        if not uid:
            # інколи немає ID, але є імʼя — кладемо як "штучний" ключ
            name = (r.get("PORTAL_USER_NAME") or "").strip()
            uid = f"name::{name}" if name else ""
        if not uid:
            continue
        per_op[uid] = per_op.get(uid, 0) + 1

    per_operator_list = []
    for uid, cnt in sorted(per_op.items(), key=lambda x: (-x[1], x[0])):
        if uid.startswith("name::"):
            name = uid.split("name::",1)[1] or "Невідомо"
        else:
            name = (r.get("PORTAL_USER_NAME") or "").strip() if False else await _get_user_name(uid)
        per_operator_list.append({"id": uid, "name": name, "outgoing": cnt})

    log.info("[telephony] totals: missed=%s, outgoing_ops=%s", missed_total, len(per_operator_list))
    return {"missed_total": missed_total, "per_operator": per_operator_list}

# ------------------------ Company summary -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    _ = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()

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

    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    service_submitted = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "!TYPE_ID": conn_type_ids,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            ">=DATE_MODIFY": frm, "<DATE_MODIFY": to,
        },
        select=["ID"]
    )

    log.info("[summary] created=%s (c0=%s + brig=%s), closed=%s, active=%s, exact=%s, think=%s, service_submitted=%s",
             created_conn, len(created_c0_exact), len(created_to_brigades),
             closed_conn, active_conn, exact_cnt, think_cnt, len(service_submitted))

    return {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "service": {"submitted_today": len(service_submitted)},
    }

# ------------------------ Formatting ----------------------
def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; c0 = d["cat0"]; s = d["service"]
    parts = [
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
        f"📨 Подали сьогодні: <b>{s['submitted_today']}</b>",
        "━━━━━━━━━━━━━━━",
    ]
    return "\n".join(parts)

def format_telephony_block(t: Dict[str, Any]) -> str:
    lines = [
        "📞 <b>Телефонія</b>",
        f"🔕 Пропущених: <b>{t['missed_total']}</b>",
    ]
    if t["per_operator"]:
        lines += ["", "👥 По операторам (вихідні):"]
        for item in t["per_operator"]:
            lines.append(f"• {item['name']}: <b>{item['outgoing']}</b>")
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
        text = format_company_summary(data) + "\n" + format_telephony_block(tel)
        await _safe_send(target_chat, text)
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
