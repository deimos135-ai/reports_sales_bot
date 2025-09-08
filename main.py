# main.py — Fiber Reports + Telephony (Bitrix voximplant.statistic.get) — fixed direction & pagination
import asyncio, html, json, logging, os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

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

REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))  # optional

# Довідник операторів: ENV (JSON) має пріоритет над дефолтом
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "")
try:
    TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else {}
except Exception:
    TELEPHONY_OPERATORS = {}

DEFAULT_OPERATOR_MAP: Dict[str, str] = {
    "238": "Яна Тищенко",
    "1340": "Вероніка Дроботя",
    "3356": "Вікторія Крамаренко",
    "9294": "Евеліна Безсмертна",
    "10000": "Руслана Писанка",
    "130": "Олена Михайленко",
}
OP_NAME: Dict[str, str] = {**DEFAULT_OPERATOR_MAP, **TELEPHONY_OPERATORS}

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
                # voximplant може повертати не лише result, тож повертаємо «як є»
                return data
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")

async def b24_list(method: str, *, page_size: int = 200, throttle: float = 0.12, **params) -> List[Dict[str, Any]]:
    """
    Узагальнене пагінування для більшості list-методів (crm.deal.list тощо),
    які розуміють параметр 'start' і повертають або список, або {items: [...]}.
    """
    start = 0
    out: List[Dict[str, Any]] = []
    while True:
        payload = dict(params); payload["start"] = start
        data = await b24(method, **payload)
        res = data.get("result")
        chunk = res if isinstance(res, list) else (res.get("items", []) if isinstance(res, dict) else [])
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        start += page_size
        if throttle: await asyncio.sleep(throttle)
    return out

async def b24_vox_paginate(
    *,
    flt: Dict[str, Any],
    select: List[str],
    order: Optional[Dict[str, str]] = None,
    page_size: int = 200,
    throttle: float = 0.1,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Спеціальне пагінування для voximplant.statistic.get, яке повертає:
      - result: [ ... ]
      - next: <int>   (offset наступної сторінки)
      - total: <int>  (загальна кількість по запиту)
    Повертає (rows, meta), де meta містить fetched, total, pages.
    """
    start: Optional[int] = 0
    rows: List[Dict[str, Any]] = []
    pages = 0
    total: Optional[int] = None

    while True:
        payload: Dict[str, Any] = {
            "filter": flt,
            "select": select,
            "start": start if isinstance(start, int) else 0,
        }
        if order:
            payload["order"] = order
        data = await b24("voximplant.statistic.get", **payload)
        res = data.get("result", [])
        rows.extend(res)
        if total is None:
            total = data.get("total")  # може бути None у старих порталів
        pages += 1

        nxt = data.get("next")
        if nxt is None:
            break
        start = nxt
        if throttle:
            await asyncio.sleep(throttle)

    meta = {
        "fetched": len(rows),
        "total": int(total) if isinstance(total, int) else len(rows),
        "pages": pages,
    }
    return rows, meta

# ------------------------ Caches / mappings ---------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        data = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        items = data.get("result", [])
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
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str, str, str]:
    """
    Повертає:
      label (ДД.ММ.РРРР локально),
      start_utc_iso, end_utc_iso,
      start_local_iso, end_local_iso
    """
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    def _fmt_local(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    start_utc = start_local.astimezone(timezone.utc).isoformat()
    end_utc = end_local.astimezone(timezone.utc).isoformat()
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc, end_utc, _fmt_local(start_local), _fmt_local(end_local)

# ------------------------ CAT0 stage resolving ------------
_CAT0_STAGES: Optional[Dict[str, str]] = None
async def _cat0_stages() -> Dict[str, str]:
    global _CAT0_STAGES
    if _CAT0_STAGES is None:
        data = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_STAGE_0"})
        items = data.get("result", [])
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

# ------------------------ Telephony (Bitrix) --------------
def _operator_name(pid: Any) -> str:
    if pid is None:
        return "Невідомий оператор"
    sid = str(pid)
    return OP_NAME.get(sid) or f"ID {sid}"

def _is_incoming_by_category(cat: str) -> Optional[bool]:
    """
    НЕ трактуємо 'external' як вихідний.
    Дозволяємо позитивно визначати лише низку типово «вхідних» категорій.
    """
    cat = (cat or "").strip().lower()
    if not cat:
        return None
    if cat in {"callback", "calltracking", "ivr", "queue", "sip"}:
        return True   # inbound
    return None       # external/unknown -> невідомо

def _incoming_outgoing_flags(r: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Повертає (is_incoming, is_outgoing).
    1) CALL_TYPE: 1=in, 2=out — основа.
    2) Якщо CALL_TYPE невизначений — пробуємо категорію (лише inbound).
    """
    ct = r.get("CALL_TYPE")
    if isinstance(ct, int) and ct in (1, 2):
        return (ct == 1, ct == 2)
    by_cat = _is_incoming_by_category(r.get("CALL_CATEGORY"))
    if by_cat is not None:
        return (by_cat, False)
    return (False, False)

def _is_missed_incoming(r: Dict[str, Any]) -> bool:
    is_in, _ = _incoming_outgoing_flags(r)
    if not is_in:
        return False
    dur = int(r.get("CALL_DURATION") or r.get("DURATION") or 0)
    failed = str(r.get("CALL_FAILED_CODE") or "").strip()
    return dur == 0 or failed != ""

async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    label, _, _, start_local_iso, end_local_iso = _day_bounds(offset_days)

    select = [
        "CALL_START_DATE", "CALL_DURATION", "CALL_FAILED_CODE",
        "CALL_TYPE", "CALL_CATEGORY", "PORTAL_USER_ID"
    ]
    flt = {
        ">=CALL_START_DATE": start_local_iso,
        "<CALL_START_DATE": end_local_iso,
    }

    rows, meta = await b24_vox_paginate(
        flt=flt,
        select=select,
        order={"CALL_START_DATE": "ASC"},
        page_size=200,
        throttle=0.1,
    )
    log.info("[telephony] %s: fetched=%s total=%s pages=%s",
             label, meta["fetched"], meta["total"], meta["pages"])

    missed_total = 0
    incoming_answered_total = 0
    outgoing_total = 0

    per_incoming_by_op: DefaultDict[str, int] = defaultdict(int)
    per_outgoing_by_op: DefaultDict[str, int] = defaultdict(int)
    per_processed_by_op: DefaultDict[str, int] = defaultdict(int)  # вхідні прийняті + вихідні

    for r in rows:
        op = _operator_name(r.get("PORTAL_USER_ID"))
        is_in, is_out = _incoming_outgoing_flags(r)
        missed = _is_missed_incoming(r)

        if missed:
            missed_total += 1

        if is_in and not missed:
            incoming_answered_total += 1
            per_incoming_by_op[op] += 1
            per_processed_by_op[op] += 1

        if is_out:
            outgoing_total += 1
            per_outgoing_by_op[op] += 1
            per_processed_by_op[op] += 1

    def _sorted_items(d: Dict[str, int]) -> List[Tuple[str, int]]:
        return sorted(d.items(), key=lambda x: (-x[1], x[0]))

    return {
        "missed_total": missed_total,
        "incoming_answered_total": incoming_answered_total,
        "outgoing_total": outgoing_total,
        "incoming_by_operator": _sorted_items(per_incoming_by_op),
        "outgoing_by_operator": _sorted_items(per_outgoing_by_op),
        "processed_by_operator": _sorted_items(per_processed_by_op),
        "meta": meta,
    }

def format_telephony_summary(t: Dict[str, Any]) -> str:
    lines = []
    lines.append("📞 <b>Телефонія</b>")
    # показуємо лічильники Bitrix-пагінації для прозорості
    meta = t.get("meta") or {}
    lines.append(f"🧾 Записів (за день): <b>{meta.get('fetched', 0)}</b> / {meta.get('total', meta.get('fetched', 0))} · сторінок: {meta.get('pages', 1)}")
    lines.append(f"🔕 Пропущених: <b>{t['missed_total']}</b>")
    lines.append(f"📥 Вхідних (прийнятих): <b>{t['incoming_answered_total']}</b>")
    lines.append(f"📤 Вихідних: <b>{t['outgoing_total']}</b>")
    lines.append("")
    if t["incoming_by_operator"]:
        lines.append("👥 По операторам (вхідні, прийняті):")
        for name, cnt in t["incoming_by_operator"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t["outgoing_by_operator"]:
        lines.append("👥 По операторам (вихідні):")
        for name, cnt in t["outgoing_by_operator"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t["processed_by_operator"]:
        lines.append("👥 По операторам (опрацьовано всього):")
        for name, cnt in t["processed_by_operator"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm_utc, to_utc, _, _ = _day_bounds(offset_days)
    type_map = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()

    # A) Подали сьогодні (кат.0 exact + переміщення у бригади сьогодні)
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

    # B) Закрили сьогодні (по CLOSEDATE)
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

    # C) Активні у бригадних
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

    # D) Категорія 0: "На конкретний день" та "Думають" (відкриті)
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    # E) Телефонія
    telephony = await fetch_telephony_for_day(offset_days)

    log.info(
        "[summary] created=%s (c0_exact=%s + to_brigades=%s), closed=%s, active=%s, exact=%s, think=%s, tel_fetched=%s",
        created_conn, len(created_c0_exact), len(created_to_brigades), closed_conn, active_conn, exact_cnt, think_cnt,
        telephony.get("meta", {}).get("fetched"),
    )

    return {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "telephony": telephony,
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; c0 = d["cat0"]; t = d["telephony"]

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
    parts.append(format_telephony_summary(t))
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
