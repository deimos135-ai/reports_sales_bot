# main.py
import os
import asyncio
import logging
from typing import Any, Dict, List, Tuple, DefaultDict, Optional
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from fastapi import FastAPI

app = FastAPI()
log = logging.getLogger("report")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# -------------------- ENV CONFIG --------------------
# Приклад: B24_BASE_URL = "https://your.bitrix24.ua/rest/123/xxxxxxxxxxxxxxxxxxxx/"
B24_BASE_URL = os.getenv("B24_BASE_URL", "").rstrip("/") + "/"
# Таймзона, де у вас «день». Напр., Europe/Kyiv
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Kyiv")

# Скільки макс. рядків дозволяємо витягнути за один запит-звіт (safety valve)
# Якщо день активний і даних багато — піде в пагінацію, але не більше HARD_LIMIT
HARD_LIMIT = int(os.getenv("HARD_LIMIT", "50000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "200"))
THROTTLE_SEC = float(os.getenv("THROTTLE_SEC", "0.05"))  # пауза між сторінками

# -------------------- UTILS --------------------
def _as_int(x: Any, default: int = 0) -> int:
    try:
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        # код типу "603-S" не є числом
        return int(s) if s.lstrip("+-").isdigit() else default
    except Exception:
        return default

def _as_str(x: Any) -> str:
    return "" if x is None else str(x)

def _operator_name(portal_user_id: Any) -> str:
    # Хук під ваш маппінг (можна підставити кеш з Bitrix users.get)
    pid = _as_str(portal_user_id).strip()
    if not pid:
        return "Невідомий"
    # приклад локального мапу з env JSON або просто повертаємо ID
    return os.getenv(f"OP_{pid}", f"ID {pid}")

def _day_bounds(offset_days: int = 0) -> Tuple[str, datetime, datetime, str, str]:
    tz = ZoneInfo(LOCAL_TZ)
    today = datetime.now(tz).date()
    day = today + timedelta(days=offset_days)
    start = datetime.combine(day, datetime.min.time()).replace(tzinfo=tz)
    end = start + timedelta(days=1)
    # Bitrix любить ISO з зсувом
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    label = day.strftime("%d.%m.%Y")
    return label, start, end, start_iso, end_iso

async def _b24_call(session: aiohttp.ClientSession, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # REST endpoint: {BASE}/voximplant.statistic.get.json
    url = f"{B24_BASE_URL}{method}.json"
    async with session.post(url, data=params, timeout=aiohttp.ClientTimeout(total=90)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)

async def b24_paginated_list(
    method: str,
    *,
    filter: Dict[str, Any],
    select: List[str],
    page_size: int = PAGE_SIZE,
    hard_limit: int = HARD_LIMIT,
    throttle: float = THROTTLE_SEC,
) -> List[Dict[str, Any]]:
    """
    Загальний пагінатор для Bitrix list-методів зі схемою start/next.
    """
    rows: List[Dict[str, Any]] = []
    start = 0
    seen = 0

    async with aiohttp.ClientSession() as session:
        while True:
            params: Dict[str, Any] = {
                "select[]": select,  # масив селектів
                "start": start,
            }
            # Фільтр: ключі типу ">=CALL_START_DATE" мають йти як filter[>=CALL_START_DATE]
            for k, v in filter.items():
                params[f"filter[{k}]"] = v

            # Додатково: деякі методи приймають "limit"
            params["limit"] = page_size

            data = await _b24_call(session, method, params)
            result = data.get("result", [])
            total = data.get("total")  # може бути відсутнім
            nxt = data.get("next")

            if not isinstance(result, list):
                log.warning("Unexpected result type: %s", type(result))
                break

            rows.extend(result)
            seen += len(result)

            # Прогрес-лог кожні ~1000 рядків
            if seen % 1000 < len(result):
                log.info("[b24] fetched %s rows (start=%s, next=%s, total=%s)", seen, start, nxt, total)

            if seen >= hard_limit:
                log.warning("[b24] hard limit reached: %s", hard_limit)
                break

            if nxt is None:
                break

            start = nxt
            if throttle:
                await asyncio.sleep(throttle)

    return rows

# -------------------- TELEPHONY LOGIC --------------------
def _is_incoming(r: Dict[str, Any]) -> bool:
    return _as_int(r.get("CALL_TYPE"), -1) == 1

def _is_outgoing(r: Dict[str, Any]) -> bool:
    return _as_int(r.get("CALL_TYPE"), -1) == 2

def _is_missed_incoming(r: Dict[str, Any]) -> bool:
    # Пропущений — ТІЛЬКИ вхідний і з нульовою тривалістю
    if not _is_incoming(r):
        return False
    dur = _as_int(r.get("CALL_DURATION") or r.get("DURATION"), 0)
    return dur <= 0

def _is_answered_incoming(r: Dict[str, Any]) -> bool:
    if not _is_incoming(r):
        return False
    dur = _as_int(r.get("CALL_DURATION") or r.get("DURATION"), 0)
    code = _as_str(r.get("CALL_FAILED_CODE")).upper()
    # У практиці Bitrix/Asterisk прийнятий зазвичай має duration>0, а код "200"/порожній/OK
    return (dur > 0) and (code in ("", "0", "200", "OK"))

async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    label, _, _, start_iso, end_iso = _day_bounds(offset_days)

    select = [
        "ID",
        "PORTAL_USER_ID",
        "PORTAL_NUMBER",
        "PHONE_NUMBER",
        "CALL_ID",
        "CALL_CATEGORY",
        "CALL_DURATION",
        "CALL_START_DATE",
        "CALL_FAILED_CODE",
        "CALL_TYPE",
    ]
    flt = {
        ">=CALL_START_DATE": start_iso,
        "<CALL_START_DATE": end_iso,
    }

    rows = await b24_paginated_list(
        "voximplant.statistic.get",
        filter=flt,
        select=select,
        page_size=PAGE_SIZE,
        hard_limit=HARD_LIMIT,
        throttle=THROTTLE_SEC,
    )
    log.info("[telephony] %s: fetched %s calls", label, len(rows))

    missed_total = 0
    incoming_answered_total = 0
    outgoing_total = 0

    per_incoming_by_op: DefaultDict[str, int] = defaultdict(int)
    per_outgoing_by_op: DefaultDict[str, int] = defaultdict(int)

    for r in rows:
        name = _operator_name(r.get("PORTAL_USER_ID"))

        if _is_incoming(r):
            if _is_missed_incoming(r):
                missed_total += 1
            elif _is_answered_incoming(r):
                incoming_answered_total += 1
                per_incoming_by_op[name] += 1
            else:
                # Інші вхідні (напр., zero duration + код 200 не трапляється, але на всяк випадок)
                missed_total += 1

        if _is_outgoing(r):
            outgoing_total += 1
            per_outgoing_by_op[name] += 1

    def _sorted_items(d: Dict[str, int]) -> List[Tuple[str, int]]:
        return sorted(d.items(), key=lambda x: (-x[1], x[0]))

    return {
        "missed_total": missed_total,
        "incoming_answered_total": incoming_answered_total,
        "outgoing_total": outgoing_total,
        "incoming_by_operator": _sorted_items(per_incoming_by_op),
        "outgoing_by_operator": _sorted_items(per_outgoing_by_op),
        "total_rows": len(rows),
    }

# -------------------- REPORT TEXT --------------------
def build_report_text(offset_days: int, telephony: Dict[str, Any]) -> str:
    label, *_ = _day_bounds(offset_days)
    missed = telephony["missed_total"]
    in_ok = telephony["incoming_answered_total"]
    out_all = telephony["outgoing_total"]

    inc_ops = telephony["incoming_by_operator"]
    out_ops = telephony["outgoing_by_operator"]

    def lines_for_ops(title: str, items: List[Tuple[str, int]]) -> str:
        if not items:
            return ""
        s = [title]
        for name, cnt in items:
            s.append(f"• {name}: {cnt}")
        return "\n".join(s)

    chunks = [
        f"🗓 Дата: {label}",
        "━━━━━━━━━━━━━━━",
        "📞 Телефонія",
        f"🔕 Пропущених: {missed}",
        f"📥 Вхідних (прийнятих): {in_ok}",
        f"📤 Вихідних: {out_all}",
    ]

    inc_block = lines_for_ops("\n👥 По операторам (вхідні, прийняті):", inc_ops)
    out_block = lines_for_ops("\n👥 По операторам (вихідні):", out_ops)

    if inc_block:
        chunks.append(inc_block)
    if out_block:
        chunks.append(out_block)

    chunks.append("━━━━━━━━━━━━━━━")
    return "\n".join(chunks)

# -------------------- HTTP HANDLERS --------------------
@app.get("/report/today")
async def report_today() -> Dict[str, Any]:
    t = await fetch_telephony_for_day(0)
    return {
        "text": build_report_text(0, t),
        "raw": t,
    }

@app.get("/report/yesterday")
async def report_yesterday() -> Dict[str, Any]:
    t = await fetch_telephony_for_day(-1)
    return {
        "text": build_report_text(-1, t),
        "raw": t,
    }

# кореневий хелсчек
@app.get("/")
async def root():
    return {"ok": True}
