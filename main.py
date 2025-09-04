# main.py — Fiber Reports + Telephony
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
REPORT_TIME = os.environ.get("REPORT_TIME", "19:00")

REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))

# оператора задаємо в секретах:
# TELEPHONY_OPERATORS='{"238":"Яна Тищенко","1340":"Вероніка Дроботя",...}'
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "")
TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else {}

# ------------------------ Logging -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("report_bot")

# ------------------------ App/Bot -------------------------
app = FastAPI()
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
HTTP: aiohttp.ClientSession

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
    start = 0; out: List[Dict[str, Any]] = []
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

# ------------------------ Caches --------------------------
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
    return "підключ" in name or "подключ" in name

async def _connection_type_ids() -> List[str]:
    m = await get_deal_type_map()
    return [tid for tid, nm in m.items() if _is_connection(tid, nm)]

# ------------------------ Brigade stages ------------------
_BRIGADE_STAGE = {1: "UC_XF8O6V", 2: "UC_0XLPCN", 3: "UC_204CP3", 4: "UC_TNEW3Z", 5: "UC_RMBZ37"}
_BRIGADE_STAGE_FULL = {f"C20:{v}" for v in _BRIGADE_STAGE.values()}

# ------------------------ Time helpers --------------------
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
    return label, start_local.strftime("%Y-%m-%d %H:%M:%S"), end_local.strftime("%Y-%m-%d %H:%M:%S")

# ------------------------ CAT0 ----------------------------
_CAT0_STAGES: Optional[Dict[str, str]] = None
async def _cat0_stages() -> Dict[str, str]:
    global _CAT0_STAGES
    if _CAT0_STAGES is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_STAGE_0"})
        _CAT0_STAGES = {i["STATUS_ID"]: i["NAME"] for i in items}
    return _CAT0_STAGES

async def _resolve_cat0_stage_ids() -> Tuple[str, str]:
    st = await _cat0_stages()
    exact_id = next((sid for sid, nm in st.items() if (nm or "").strip().lower() == "на конкретний день"), "5")
    think_id = next((sid for sid, nm in st.items() if (nm or "").strip().lower() == "думають"), "DETAILS")
    return f"C0:{exact_id}", f"C0:{think_id}"

async def _count_open_in_stage(cat_id: int, stage_full: str, type_ids: Optional[List[str]] = None) -> int:
    flt: Dict[str, Any] = {"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": stage_full}
    if type_ids: flt["TYPE_ID"] = type_ids
    deals = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    return len(deals)

# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    conn_type_ids = await _connection_type_ids()
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()

    # Подані
    created_c0_exact = await b24_list("crm.deal.list", filter={
        "CATEGORY_ID": 0, "STAGE_ID": c0_exact_stage, "TYPE_ID": conn_type_ids,
        ">=DATE_CREATE": frm, "<DATE_CREATE": to}, select=["ID"])
    created_to_brigades = await b24_list("crm.deal.list", filter={
        "CATEGORY_ID": 20, "STAGE_ID": list(_BRIGADE_STAGE_FULL), "TYPE_ID": conn_type_ids,
        ">=DATE_MODIFY": frm, "<DATE_MODIFY": to}, select=["ID"])
    created_conn = len(created_c0_exact) + len(created_to_brigades)

    # Закриті
    closed_list = await b24_list("crm.deal.list", filter={
        "CATEGORY_ID": 20, "STAGE_ID": "C20:WON", "TYPE_ID": conn_type_ids,
        ">=CLOSEDATE": frm, "<CLOSEDATE": to}, select=["ID"])
    closed_conn = len(closed_list)

    # Активні
    active_open = await b24_list("crm.deal.list", filter={
        "CLOSED": "N", "CATEGORY_ID": 20, "STAGE_ID": list(_BRIGADE_STAGE_FULL),
        "TYPE_ID": conn_type_ids, "STAGE_SEMANTIC_ID": "P"}, select=["ID"])
    active_conn = len(active_open)

    # Кат.0
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    # Сервісні заявки (усі не-підключення, сьогодні створені)
    service_today = await b24_list("crm.deal.list", filter={
        "CATEGORY_ID": 20, "!TYPE_ID": conn_type_ids,
        ">=DATE_CREATE": frm, "<DATE_CREATE": to}, select=["ID"])
    service_cnt = len(service_today)

    return {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "service": {"submitted": service_cnt},
    }

# ------------------------ Telephony -----------------------
async def build_telephony_stats(offset_days: int = 0) -> Dict[str, Any]:
    _, frm_local, to_local = _day_bounds_local_strings(offset_days)
    records = await b24_list("voximplant.statistic.get",
        order={"CALL_START_DATE": "ASC"},
        filter={">=CALL_START_DATE": frm_local, "<CALL_START_DATE": to_local},
        select=["CALL_ID", "CALL_SESSION_ID", "CALL_TYPE", "CALL_DURATION", "PORTAL_USER_ID", "CALL_START_DATE"])

    seen, uniq = set(), []
    for r in records:
        key = r.get("CALL_ID") or r.get("CALL_SESSION_ID") or f"{r.get('PORTAL_USER_ID')}|{r.get('CALL_START_DATE')}"
        if key in seen: continue
        seen.add(key); uniq.append(r)

    def _is_out(r): return str(r.get("CALL_TYPE")).upper() in {"2", "OUTGOING"}
    def _is_missed(r):
        t = str(r.get("CALL_TYPE")).upper(); d = int(r.get("CALL_DURATION") or 0)
        return (t in {"5", "INCOMING", "1"}) and d == 0

    missed, outgoing = 0, 0; by_op: Dict[str,int] = {}
    def _disp(uid: str) -> str:
        if uid in TELEPHONY_OPERATORS: return TELEPHONY_OPERATORS[uid]
        return f"ID {uid}"

    for r in uniq:
        if _is_missed(r): missed += 1
        if _is_out(r):
            outgoing += 1
            name = _disp(str(r.get("PORTAL_USER_ID") or ""))
            by_op[name] = by_op.get(name, 0) + 1

    return {"missed_total": missed, "outgoing_total": outgoing, "outgoing_by_operator": by_op}

# ------------------------ Formatter -----------------------
def format_company_summary(d: Dict[str, Any], tel: Dict[str, Any]) -> str:
    c, c0, s = d["connections"], d["cat0"], d["service"]
    t = tel
    lines = [
        f"🗓 <b>Дата: {d['date_label']}</b>",
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
        f"📤 Набраних: <b>{t['outgoing_total']}</b>",
    ]
    if t["outgoing_by_operator"]:
        lines.append(""); lines.append("👥 По операторам (вихідні):")
        for name, cnt in t["outgoing_by_operator"].items():
            lines.append(f"• {name}: {cnt}")
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ------------------------ Send helpers --------------------
async def _safe_send(chat_id: int, text: str):
    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True); return
        except Exception as e:
            await asyncio.sleep(min(30, 2**attempt))

async def send_company_summary_to_chat(chat_id: int, offset_days: int = 0):
    data, tel = await build_company_summary(offset_days), await build_telephony_stats(offset_days)
    await _safe_send(chat_id, format_company_summary(data, tel))

# ------------------------ Commands ------------------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    parts = (m.text or "").split(); offset = int(parts[1]) if len(parts)>1 and parts[1].isdigit() else 0
    await m.answer("🔄 Формую звіт…")
    await send_company_summary_to_chat(m.chat.id, offset)
    await m.answer("✅ Готово")

# ------------------------ Scheduler -----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
    hh, mm = map(int, REPORT_TIME.split(":"))
    now_local = now_utc.astimezone(REPORT_TZ)
    target_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target_local <= now_local: target_local += timedelta(days=1)
    return target_local.astimezone(timezone.utc)

async def scheduler_loop():
    while True:
        now, nxt = datetime.now(timezone.utc), _next_run_dt(datetime.now(timezone.utc))
        await asyncio.sleep(max(1, (nxt-now).total_seconds()))
        if REPORT_SUMMARY_CHAT: await send_company_summary_to_chat(REPORT_SUMMARY_CHAT, 0)

# ------------------------ Webhook -------------------------
@app.on_event("startup")
async def on_startup():
    global HTTP; HTTP = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    await bot.set_my_commands([BotCommand(command="report_now", description="Сумарний звіт")])
    await bot.set_webhook(f"{WEBHOOK_BASE}/webhook/{WEBHOOK_SECRET}")
    asyncio.create_task(scheduler_loop())

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook(); await HTTP.close(); await bot.session.close()

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET: return {"ok": False}
    update = Update.model_validate(await request.json()); await dp.feed_update(bot, update); return {"ok": True}
