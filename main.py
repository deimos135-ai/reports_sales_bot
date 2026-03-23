# main.py — Fiber Reports + Telephony (Bitrix voximplant.statistic.get)
# CLEAN VERSION: telephony aligned to Bitrix stat rows + cleaner Telegram output
# UPDATED: supports Telegram forum topics via message_thread_id

import asyncio
import html
import json
import logging
import os
import re
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

REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))  # group/supergroup id
REPORT_SUMMARY_THREAD_ID = int(os.environ.get("REPORT_SUMMARY_THREAD_ID", "0"))  # topic/thread id
MIN_OUT_DURATION = int(os.environ.get("MIN_OUT_DURATION", "10"))

# Operators map (ENV has priority)
_TELEPHONY_OPERATORS_RAW = os.environ.get("TELEPHONY_OPERATORS", "")
try:
    TELEPHONY_OPERATORS: Dict[str, str] = json.loads(_TELEPHONY_OPERATORS_RAW) if _TELEPHONY_OPERATORS_RAW else {}
except Exception:
    TELEPHONY_OPERATORS = {}

DEFAULT_OPERATOR_MAP: Dict[str, str] = {
    "238": "Яна Тищенко",
    "130": "Олена Михайленко",
    "1340": "Вероніка Дроботя",
    "14380": "Юля Козуб",
    "13928": "Влад Росада",
    "14172": "Ярослава Андрушечко",
    "1156": "Віртуальний Менеджер",
    "44": "Яна Андрущенко",
    "0": "Без имени",
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
    url = f"{BITRIX_WEBHOOK_BASE}/{method}.json"
    for attempt in range(6):
        try:
            async with HTTP.post(url, json=params) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"]
                    desc = data.get("error_description")
                    if err in ("QUERY_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "INTERNAL_SERVER_ERROR"):
                        log.warning("Bitrix temp error: %s (%s), retry #%s", err, desc, attempt + 1)
                        await _sleep_backoff(attempt)
                        continue
                    raise RuntimeError(f"B24 error: {err}: {desc}")
                return data
        except aiohttp.ClientError as e:
            log.warning("Bitrix network error: %s, retry #%s", e, attempt + 1)
            await _sleep_backoff(attempt)
    raise RuntimeError("Bitrix request failed after retries")


async def b24(method: str, **params) -> Any:
    data = await b24_raw(method, **params)
    return data.get("result")


async def b24_list(method: str, *, throttle: float = 0.12, **params) -> List[Dict[str, Any]]:
    start: Any = params.pop("start", 0)
    out: List[Dict[str, Any]] = []
    pages = 0
    total_known = None

    while True:
        payload = dict(params)
        payload["start"] = start
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
        if throttle:
            await asyncio.sleep(throttle)

    log.info("[b24_list] %s: fetched %s rows in %s pages (total=%s)", method, len(out), pages, total_known)
    return out


# ------------------------ Caches / mappings ---------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
_CAT0_STAGES: Optional[Dict[str, str]] = None


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


async def _cat0_stages() -> Dict[str, str]:
    global _CAT0_STAGES
    if _CAT0_STAGES is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_STAGE_0"})
        _CAT0_STAGES = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] CAT0 stages: %s", len(_CAT0_STAGES))
    return _CAT0_STAGES


# Бригадні стадії в кат.20
_BRIGADE_STAGE = {
    1: "UC_XF8O6V",
    2: "UC_0XLPCN",
    3: "UC_204CP3",
    4: "UC_TNEW3Z",
    5: "UC_RMBZ37",
    6: "UC_CY6YW8",
    7: "7",
}
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


async def _resolve_cat0_stage_ids() -> Tuple[str, str]:
    st = await _cat0_stages()
    exact_id = None
    think_id = None

    for sid, nm in st.items():
        n = (nm or "").strip().lower()
        if n == "на конкретний день":
            exact_id = sid
        if n == "думають":
            think_id = sid

    if not exact_id:
        exact_id = "5"
    if not think_id:
        think_id = "DETAILS"

    return f"C0:{exact_id}", f"C0:{think_id}"


async def _count_open_in_stage(cat_id: int, stage_full: str, type_ids: Optional[List[str]] = None) -> int:
    flt: Dict[str, Any] = {
        "CLOSED": "N",
        "CATEGORY_ID": cat_id,
        "STAGE_ID": stage_full,
    }
    if type_ids:
        flt["TYPE_ID"] = type_ids

    deals = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    if deals:
        return len(deals)

    short = stage_full.split(":", 1)[-1]
    flt["STAGE_ID"] = short
    deals_fb = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    return len(deals_fb)


# ------------------------ Telephony helpers ---------------
def _code_str(r: Dict[str, Any]) -> str:
    for k in ("CALL_FAILED_CODE", "STATUS_CODE", "FAILED_CODE"):
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
    У вашому Bitrix:
      raw CALL_TYPE:
        1 = outgoing
        2 = incoming

    Нормалізуємо до:
      1 = inbound
      2 = outbound
    """
    try:
        v = int(r.get("CALL_TYPE"))
        if v == 2:
            return 1
        if v == 1:
            return 2
    except Exception:
        pass
    return None


def _operator_name(pid: Any) -> str:
    if pid is None:
        return "Без имени"

    sid = str(pid).strip()
    if not sid or sid == "0":
        return "Без имени"

    return OP_NAME.get(sid) or f"ID {sid}"


def _include_row_for_totals(r: Dict[str, Any]) -> bool:
    return r.get("_dir") in (1, 2)


def _include_row_for_operator_stats(r: Dict[str, Any]) -> bool:
    pid = r.get("PORTAL_USER_ID")
    if pid is None:
        return False

    s = str(pid).strip()
    if not s:
        return False

    if s == "0":
        return True

    try:
        pid_int = int(s)
    except Exception:
        return False

    if pid_int < 0:
        return False

    if not _allowed_pid(pid_int):
        return False

    return True


def _sorted_items(d: Dict[str, int]) -> List[Tuple[str, int]]:
    return sorted(d.items(), key=lambda x: (-x[1], x[0]))


def _compact_operator_rows(t: Dict[str, Any]) -> List[Tuple[str, int, int, int, int]]:
    per_in_raw = dict(t.get("per_in_total", []))
    per_out = dict(t.get("per_out_total", []))
    per_missed = dict(t.get("per_missed", []))

    all_names = set(per_in_raw) | set(per_out) | set(per_missed)

    rows = []
    for name in all_names:
        incoming_raw = per_in_raw.get(name, 0)
        missed = per_missed.get(name, 0)
        incoming = max(0, incoming_raw - missed)
        outgoing = per_out.get(name, 0)
        total = incoming + outgoing + missed
        rows.append((name, incoming, outgoing, missed, total))

    rows.sort(key=lambda x: (-x[4], x[0]))
    return rows


# ------------------------ Telephony (Bitrix) --------------
async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    label, _, _, start_local_iso, end_local_iso = _day_bounds(offset_days)

    select = [
        "ID",
        "CALL_ID",
        "CALL_SESSION_ID",
        "CALL_START_DATE",
        "CALL_DURATION",
        "RECORD_DURATION",
        "STATUS_CODE",
        "CALL_FAILED_CODE",
        "FAILED_CODE",
        "CALL_TYPE",
        "CALL_CATEGORY",
        "PORTAL_USER_ID",
        "PORTAL_NUMBER",
        "PHONE_NUMBER",
    ]
    flt = {
        ">=CALL_START_DATE": start_local_iso,
        "<CALL_START_DATE": end_local_iso,
    }

    all_rows: List[Dict[str, Any]] = []
    start: Any = 0
    pages = 0
    total_hint: Optional[int] = None

    while True:
        data = await b24_raw(
            "voximplant.statistic.get",
            filter=flt,
            select=select,
            order={"CALL_START_DATE": "ASC"},
            start=start,
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

    rows: List[Dict[str, Any]] = []
    for r in all_rows:
        rr = dict(r)
        rr["_dir"] = _norm_call_type(rr)
        rr["_code"] = _code_str(rr)
        rr["_dur"] = _duration_sec(rr)
        rr["_op_name"] = _operator_name(rr.get("PORTAL_USER_ID"))
        rows.append(rr)

    total_rows = [r for r in rows if _include_row_for_totals(r)]
    op_rows = [r for r in rows if _include_row_for_operator_stats(r)]

    incoming_total = sum(1 for r in total_rows if r["_dir"] == 1)
    outgoing_total = sum(1 for r in total_rows if r["_dir"] == 2)

    incoming_answered_total = sum(
        1 for r in total_rows
        if r["_dir"] == 1 and r["_code"] == "200" and r["_dur"] > 0
    )

    missed_total = sum(
        1 for r in total_rows
        if r["_dir"] == 1 and r["_code"] == "304"
    )

    outgoing_success_10_total = sum(
        1 for r in total_rows
        if r["_dir"] == 2 and r["_code"] == "200" and r["_dur"] >= MIN_OUT_DURATION
    )

    per_processed: DefaultDict[str, int] = defaultdict(int)
    per_in_total: DefaultDict[str, int] = defaultdict(int)
    per_in_answered: DefaultDict[str, int] = defaultdict(int)
    per_out_total: DefaultDict[str, int] = defaultdict(int)
    per_out_success_10: DefaultDict[str, int] = defaultdict(int)
    per_missed: DefaultDict[str, int] = defaultdict(int)

    for r in op_rows:
        nm = r["_op_name"]

        if r["_dir"] == 1:
            per_in_total[nm] += 1
            if r["_code"] == "200" and r["_dur"] > 0:
                per_in_answered[nm] += 1
                per_processed[nm] += 1
            if r["_code"] == "304":
                per_missed[nm] += 1

        elif r["_dir"] == 2:
            per_out_total[nm] += 1
            if r["_code"] == "200" and r["_dur"] >= MIN_OUT_DURATION:
                per_out_success_10[nm] += 1
                per_processed[nm] += 1

    log.info(
        "[telephony] %s: raw=%s usable=%s operator_rows=%s pages=%s total_hint=%s in=%s out=%s in_ans=%s missed=%s out_ok=%s",
        label,
        len(all_rows),
        len(total_rows),
        len(op_rows),
        pages,
        total_hint,
        incoming_total,
        outgoing_total,
        incoming_answered_total,
        missed_total,
        outgoing_success_10_total,
    )

    return {
        "total_records": len(all_rows),
        "usable_records": len(total_rows),
        "operator_records": len(op_rows),
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
        "per_missed": _sorted_items(per_missed),
    }


# ------------------------ Formatting ----------------------
def format_telephony_summary(t: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("📞 <b>Телефонія</b>")

    if t.get("total_hint"):
        lines.append(f"🧾 Записів (за день): <b>{t['total_records']}</b> / {t['total_hint']} · сторінок: {t['pages']}")
    else:
        lines.append(f"🧾 Записів (за день): <b>{t['total_records']}</b> · сторінок: {t['pages']}")

    lines.append("")
    lines.append(f"📥 Вхідних (всього): <b>{t['incoming_total']}</b>")
    lines.append(f"📤 Вихідних (всього): <b>{t['outgoing_total']}</b>")
    lines.append("")
    lines.append(f"✅ Вхідних (прийнятих): <b>{t['incoming_answered_total']}</b>")
    lines.append(f"🔕 Пропущених (із вхідних): <b>{t['missed_total']}</b>")
    lines.append(f"🎯 Вихідних успішних (≥{MIN_OUT_DURATION}s): <b>{t['outgoing_success_10_total']}</b>")

    op_rows = _compact_operator_rows(t)
    if op_rows:
        lines.append("")
        lines.append("👥 <b>По операторам</b>")

        for name, incoming, outgoing, missed, total in op_rows:
            lines.append("")
            lines.append(f"<b>{name}</b>")
            lines.append(f"📥 Вхідні: <b>{incoming}</b>")
            lines.append(f"📤 Вихідні: <b>{outgoing}</b>")
            lines.append(f"🔕 Пропущені: <b>{missed}</b>")
            lines.append(f"Σ Всього: <b>{total}</b>")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]
    c0 = d["cat0"]
    t = d["telephony"]

    parts: List[str] = []
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


# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm_utc, to_utc, _, _ = _day_bounds(offset_days)

    _ = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()
    c0_exact_stage, c0_think_stage = await _resolve_cat0_stage_ids()

    # -------------------- НОВІ ПІДКЛЮЧЕННЯ --------------------
    # Рахуємо тільки реально створені за день заявки типу "Підключення".
    # Не використовуємо DATE_MODIFY, щоб не підтягувати старі заявки,
    # які просто рухались по стадіях/бригадах сьогодні.
    created_today = await b24_list(
        "crm.deal.list",
        order={"DATE_CREATE": "ASC"},
        filter={
            "TYPE_ID": conn_type_ids,
            ">=DATE_CREATE": frm_utc,
            "<DATE_CREATE": to_utc,
        },
        select=["ID", "TITLE", "CATEGORY_ID", "STAGE_ID", "DATE_CREATE", "TYPE_ID"],
    )

    # На всякий випадок прибираємо дублікати по ID
    created_conn = len({str(d.get("ID")) for d in created_today if d.get("ID") is not None})

    # -------------------- ЗАКРИТІ --------------------
    closed_list = await b24_list(
        "crm.deal.list",
        order={"CLOSEDATE": "ASC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": "C20:WON",
            "TYPE_ID": conn_type_ids,
            ">=CLOSEDATE": frm_utc,
            "<CLOSEDATE": to_utc,
        },
        select=["ID"],
    )
    closed_conn = len(closed_list)

    # -------------------- АКТИВНІ НА БРИГАДАХ --------------------
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
        select=["ID"],
    )
    active_conn = len(active_open)

    # -------------------- CAT0 СТАДІЇ --------------------
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    telephony = await fetch_telephony_for_day(offset_days)

    log.info(
        "[summary] created=%s closed=%s active=%s exact=%s think=%s tel_total=%s pages=%s",
        created_conn,
        closed_conn,
        active_conn,
        exact_cnt,
        think_cnt,
        telephony["total_records"],
        telephony["pages"],
    )

    # Опціонально: тимчасовий дебаг, щоб перевірити які саме заявки потрапили в "Подали"
    for d in created_today:
        log.info(
            "[created_today] id=%s title=%s cat=%s stage=%s created=%s",
            d.get("ID"),
            d.get("TITLE"),
            d.get("CATEGORY_ID"),
            d.get("STAGE_ID"),
            d.get("DATE_CREATE"),
        )

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
        "telephony": telephony,
    }

# ------------------------ Send helpers -------------------
async def _safe_send(chat_id: int, text: str, message_thread_id: Optional[int] = None):
    for attempt in range(7):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                disable_web_page_preview=True,
                message_thread_id=message_thread_id if message_thread_id else None,
            )
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
            log.warning(
                "telegram send failed: %s, waiting %ss (try #%s), chat=%s, thread=%s",
                e, wait, attempt + 1, chat_id, message_thread_id
            )
            await asyncio.sleep(wait)

    log.error("telegram send failed permanently (chat %s, thread %s)", chat_id, message_thread_id)


async def send_company_summary_to_chat(
    target_chat: int,
    offset_days: int = 0,
    message_thread_id: Optional[int] = None,
) -> None:
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(
            target_chat,
            format_company_summary(data),
            message_thread_id=message_thread_id,
        )
    except Exception as e:
        log.exception("company summary failed for chat %s", target_chat)
        await _safe_send(
            target_chat,
            f"❗️Помилка формування сумарного звіту:\n<code>{html.escape(str(e))}</code>",
            message_thread_id=message_thread_id,
        )


async def send_company_summary(offset_days: int = 0) -> None:
    if REPORT_SUMMARY_CHAT:
        await send_company_summary_to_chat(
            REPORT_SUMMARY_CHAT,
            offset_days,
            message_thread_id=REPORT_SUMMARY_THREAD_ID or None,
        )
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
        await send_company_summary_to_chat(
            REPORT_SUMMARY_CHAT,
            offset,
            message_thread_id=REPORT_SUMMARY_THREAD_ID or None,
        )

    await m.answer("✅ Готово")


# ------------------------ Scheduler ----------------------
def _next_run_dt(now_utc: datetime) -> datetime:
    hh, mm = map(int, REPORT_TIME.split(":", 1))
    now_local = now_utc.astimezone(REPORT_TZ)
    target_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target_local <= now_local:
        target_local += timedelta(days=1)
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
    log.info(
        "[startup] webhook set to %s | report_chat=%s | report_thread=%s",
        url, REPORT_SUMMARY_CHAT, REPORT_SUMMARY_THREAD_ID
    )


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
