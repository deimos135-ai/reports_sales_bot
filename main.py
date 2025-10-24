# main.py — Fiber Reports + Telephony (Bitrix voximplant.statistic.get) — FIXED DIRECTION + DEDUP
import asyncio, html, json, logging, os, re
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

# Operators map (ENV has priority)
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

# Optional allowlist of Bitrix user IDs (JSON list of ints)
_ALLOWED_RAW = os.environ.get("TELEPHONY_ALLOWED_USER_IDS", "").strip()
ALLOWED_IDS: Optional[set] = None
if _ALLOWED_RAW:
    try:
        ALLOWED_IDS = {int(x) for x in json.loads(_ALLOWED_RAW)}
    except Exception:
        ALLOWED_IDS = None

def _allowed_pid(pid: Any) -> bool:
    if ALLOWED_IDS is None:
        return True
    try:
        return int(pid) in ALLOWED_IDS
    except Exception:
        return False

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

async def b24_raw(method: str, **params) -> Dict[str, Any]:
    """Повертає сирий JSON Bitrix: може містити result, next, total, time..."""
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
                return data
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")

async def b24(method: str, **params) -> Any:
    """Спрощений виклик: повертає лише result (для методів без пагінації)."""
    data = await b24_raw(method, **params)
    return data.get("result")

async def b24_list(method: str, *, throttle: float = 0.12, **params) -> List[Dict[str, Any]]:
    """
    Універсальне пагінування 'start' з урахуванням Bitrix 'next'.
    Працює для crm.* та voximplant.statistic.get.
    """
    start: Any = params.pop("start", 0)
    out: List[Dict[str, Any]] = []
    pages = 0
    total_known = None

    while True:
        payload = dict(params); payload["start"] = start
        data = await b24_raw(method, **payload)
        result = data.get("result", [])
        if isinstance(result, dict) and "items" in result:
            chunk = result.get("items", [])
        else:
            chunk = result if isinstance(result, list) else []
        out.extend(chunk)
        pages += 1
        if total_known is None and isinstance(data.get("total"), int):
            total_known = data["total"]

        nxt = data.get("next", None)
        if nxt is None:
            break
        start = nxt
        if throttle: await asyncio.sleep(throttle)

    log.info("[b24_list] %s: fetched %s rows in %s pages (total=%s)", method, len(out), pages, total_known)
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
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str, str, str]:
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

# ------------------------ Telephony helpers ---------------
MIN_OUT_DURATION = int(os.environ.get("MIN_OUT_DURATION", "10"))

def _code_str(r: Dict[str, Any]) -> str:
    for k in ("STATUS_CODE", "CALL_FAILED_CODE", "FAILED_CODE"):
        raw = str(r.get(k) or "").strip()
        if raw:
            m = re.search(r"\d{3}", raw)
            return m.group(0) if m else raw
    return ""

def _duration_sec(r: Dict[str, Any]) -> int:
    for k in ("RECORD_DURATION", "CALL_DURATION", "DURATION"):
        v = r.get(k)
        try:
            return int(v)
        except Exception:
            pass
    return 0

def _norm_call_type(r: Dict[str, Any]) -> Optional[int]:
    """
    1 = inbound, 2 = outbound.
    """
    v = r.get("CALL_TYPE")
    try:
        vi = int(v)
        if vi in (1, 2):
            return vi
    except Exception:
        pass
    cat = (r.get("CALL_CATEGORY") or "").strip().lower()
    if cat in {"external", "callback"}:
        return 1
    if cat in {"outgoing"}:
        return 2
    return None

def _is_human_leg(r: Dict[str, Any]) -> bool:
    """
    Лишаємо лише «людські» ніжки: є реальний оператор і він не заборонений allowlist-ом
    """
    pid = r.get("PORTAL_USER_ID")
    try:
        pid_int = int(pid)
    except Exception:
        return False
    if pid_int <= 0:
        return False
    if not _allowed_pid(pid_int):
        return False
    return True

def _pick_better_leg(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Вибираємо найрепрезентативнішу ніжку одного виклику по оператору:
    1) прийнята (code 200 & dur>0) > неприйнята
    2) за рівних — більша тривалість
    3) за рівних — новіший час
    """
    def key(r: Dict[str, Any]) -> Tuple[int, int, str]:
        code_ok = 1 if _code_str(r) == "200" and _duration_sec(r) > 0 else 0
        return (code_ok, _duration_sec(r), str(r.get("CALL_START_DATE") or ""))
    return b if key(b) > key(a) else a

def _operator_name(pid: Any) -> str:
    if pid is None:
        return "Невідомий оператор"
    sid = str(pid)
    return OP_NAME.get(sid) or f"ID {sid}"

# ------------------------ Telephony (Bitrix) --------------
async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    label, _, _, start_local_iso, end_local_iso = _day_bounds(offset_days)

    select = [
        "CALL_ID", "CALL_SESSION_ID",
        "CALL_START_DATE",
        "CALL_DURATION", "RECORD_DURATION", "STATUS_CODE", "CALL_FAILED_CODE", "FAILED_CODE",
        "CALL_TYPE", "CALL_CATEGORY",
        "PORTAL_USER_ID",
        "PORTAL_NUMBER", "PHONE_NUMBER"
    ]
    flt = {">=CALL_START_DATE": start_local_iso, "<CALL_START_DATE": end_local_iso}

    # --- пагінація з next ---
    all_rows: List[Dict[str, Any]] = []
    start: Any = 0
    pages = 0
    total_hint: Optional[int] = None
    while True:
        data = await b24_raw(
            "voximplant.statistic.get",
            filter=flt,
            select=select,
            start=start
        )
        chunk = data.get("result", [])
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk["items"]
        if not isinstance(chunk, list):
            chunk = []
        all_rows.extend(chunk)
        pages += 1
        if total_hint is None and isinstance(data.get("total"), int):
            total_hint = data["total"]
        nxt = data.get("next", None)
        if nxt is None:
            break
        start = nxt
        await asyncio.sleep(0.1)

    # 1) «людські» ніжки
    rows = [r for r in all_rows if _is_human_leg(r)]

    # 2) нормалізація полів
    for r in rows:
        r["_dir"] = _norm_call_type(r)
        r["_code"] = _code_str(r)
        r["_dur"]  = _duration_sec(r)

    # 3) дедуп по (CALL_ID, PORTAL_USER_ID) з фолбеком, якщо CALL_ID відсутній
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        cid = str(r.get("CALL_ID") or "")
        pid = str(r.get("PORTAL_USER_ID") or "")
        if not cid:
            cid = f"{r.get('CALL_START_DATE')}-{r.get('PHONE_NUMBER')}-{r.get('PORTAL_NUMBER')}"
        key = (cid, pid)
        if key in dedup:
            dedup[key] = _pick_better_leg(dedup[key], r)
        else:
            dedup[key] = r

    legs = list(dedup.values())

    # 4) агрегати (напрямок без flip)
    incoming_total = sum(1 for r in legs if r["_dir"] == 1)
    outgoing_total = sum(1 for r in legs if r["_dir"] == 2)

    incoming_answered_total = sum(1 for r in legs if r["_dir"] == 1 and r["_code"] == "200" and r["_dur"] > 0)

    # «пропущені» — по унікальних викликах, де жоден оператор не прийняв
    callid_answered_in: Dict[str, bool] = defaultdict(bool)
    for r in legs:
        if r["_dir"] == 1:
            cid = str(r.get("CALL_ID") or f"{r.get('CALL_START_DATE')}-{r.get('PHONE_NUMBER')}-{r.get('PORTAL_NUMBER')}")
            if r["_code"] == "200" and r["_dur"] > 0:
                callid_answered_in[cid] = True
    missed_total = 0
    seen_callids = set()
    for r in legs:
        if r["_dir"] != 1:
            continue
        cid = str(r.get("CALL_ID") or f"{r.get('CALL_START_DATE')}-{r.get('PHONE_NUMBER')}-{r.get('PORTAL_NUMBER')}")
        if cid in seen_callids:
            continue
        seen_callids.add(cid)
        if not callid_answered_in.get(cid, False):
            missed_total += 1

    outgoing_success_10_total = sum(1 for r in legs if r["_dir"] == 2 and r["_code"] == "200" and r["_dur"] >= MIN_OUT_DURATION)

    # 5) пер-операторні
    def name_of(r: Dict[str, Any]) -> str:
        return _operator_name(r.get("PORTAL_USER_ID"))

    per_processed: DefaultDict[str, int] = defaultdict(int)
    per_in_total: DefaultDict[str, int] = defaultdict(int)
    per_in_answered: DefaultDict[str, int] = defaultdict(int)
    per_out_total: DefaultDict[str, int] = defaultdict(int)
    per_out_success_10: DefaultDict[str, int] = defaultdict(int)

    for r in legs:
        nm = name_of(r)
        if r["_dir"] == 1:
            per_in_total[nm] += 1
            if r["_code"] == "200" and r["_dur"] > 0:
                per_in_answered[nm] += 1
                per_processed[nm] += 1
        elif r["_dir"] == 2:
            per_out_total[nm] += 1
            if r["_code"] == "200" and r["_dur"] >= MIN_OUT_DURATION:
                per_out_success_10[nm] += 1
                per_processed[nm] += 1

    def _sorted_items(d: Dict[str, int]) -> List[Tuple[str, int]]:
        return sorted(d.items(), key=lambda x: (-x[1], x[0]))

    log.info("[telephony] %s: raw=%s, human_legs=%s, dedup_legs=%s, pages=%s (total_hint=%s)",
             label, len(all_rows), len(rows), len(legs), pages, total_hint)

    return {
        "total_records": len(all_rows),      # сирих рядків
        "pages": pages,
        "total_hint": total_hint,

        "incoming_total": incoming_total,
        "outgoing_total": outgoing_total,

        "incoming_answered_total": incoming_answered_total,
        "missed_total": missed_total,
        "outgoing_success_10_total": outgoing_success_10_total,

        "per_processed": _sorted_items(per_processed),
        "per_in_total": _sorted_items(per_in_total),
        "per_in_answered": _sorted_items(per_in_answered),
        "per_out_total": _sorted_items(per_out_total),
        "per_out_success_10": _sorted_items(per_out_success_10),

        "flip_used": False,   # більше не застосовується
    }

def format_telephony_summary(t: Dict[str, Any]) -> str:
    lines = []
    lines.append("📞 <b>Телефонія</b>")
    if t.get("total_hint"):
        lines.append(f"🧾 Записів (за день): <b>{t['total_records']}</b> / {t['total_hint']} · сторінок: {t['pages']}")
    else:
        lines.append(f"🧾 Записів (за день): <b>{t['total_records']}</b> · сторінок: {t['pages']}")
    lines.append(f"📥 Вхідних (всього): <b>{t['incoming_total']}</b>")
    lines.append(f"📤 Вихідних (всього): <b>{t['outgoing_total']}</b>")
    lines.append("")
    lines.append(f"✅ Вхідних (прийнятих): <b>{t['incoming_answered_total']}</b>")
    lines.append(f"🔕 Пропущених (із вхідних): <b>{t['missed_total']}</b>")
    lines.append(f"🎯 Вихідних успішних (≥{MIN_OUT_DURATION}s): <b>{t['outgoing_success_10_total']}</b>")
    lines.append("")
    if t["per_processed"]:
        lines.append("👥 Опрацьовано (вхідні прийняті + вихідні успішні):")
        for name, cnt in t["per_processed"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t["per_out_success_10"]:
        lines.append(f"👥 Вихідні (успішні, ≥{MIN_OUT_DURATION}s) по операторам:")
        for name, cnt in t["per_out_success_10"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t["per_in_total"]:
        lines.append("👥 Вхідні (всього) по операторам:")
        for name, cnt in t["per_in_total"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t["per_in_answered"]:
        lines.append("👥 Вхідні (прийняті) по операторам:")
        for name, cnt in t["per_in_answered"]:
            lines.append(f"• {name}: <b>{cnt}</b>")
        lines.append("")
    if t.get("flip_used"):
        lines.append("🛠 Застосовано адаптивне мапінг-напрямків (flip)")
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)

# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm_utc, to_utc, _, _ = _day_bounds(offset_days)
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

    telephony = await fetch_telephony_for_day(offset_days)

    log.info(
        "[summary] created=%s (c0_exact=%s + to_brigades=%s), closed=%s, active=%s, exact=%s, think=%s, tel_total=%s pages=%s",
        created_conn, len(created_c0_exact), len(created_to_brigades), closed_conn, active_conn, exact_cnt, think_cnt,
        telephony["total_records"], telephony["pages"]
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
