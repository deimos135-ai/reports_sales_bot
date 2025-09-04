# main.py — Fiber Reports (summary-only) + Service Today + Telephony via Bitrix Voximplant
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

# ------------------------ Telephony (Voximplant first) ---
async def _b24_vox_fetch(frm_iso: str, to_iso: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Voximplant статистика з пагінацією через voximplant.statistic.get.
    """
    offset = 0
    out: List[Dict[str, Any]] = []
    while True:
        res = await b24(
            "voximplant.statistic.get",
            FILTER={">=CALL_START_DATE": frm_iso, "<CALL_START_DATE": to_iso},
            ORDER={"CALL_START_DATE": "ASC"},
            LIMIT=limit,
            OFFSET=offset,
        )
        chunk = []
        if isinstance(res, dict):
            # у Voximplant мапа часто лежить у "items"
            chunk = res.get("items") or res.get("result") or []
        elif isinstance(res, list):
            chunk = res
        out.extend(chunk)
        if not chunk or len(chunk) < limit:
            break
        offset += limit
    return out

async def _b24_telephony_fetch(frm_iso: str, to_iso: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Фолбек на telephony.statistic.get (якщо Voximplant не повернув).
    """
    offset = 0
    out: List[Dict[str, Any]] = []
    while True:
        res = await b24(
            "telephony.statistic.get",
            FILTER={">=CALL_START_DATE": frm_iso, "<CALL_START_DATE": to_iso},
            ORDER={"CALL_START_DATE": "ASC"},
            LIMIT=limit,
            OFFSET=offset,
        )
        chunk = []
        if isinstance(res, dict):
            chunk = res.get("items") or res.get("result") or []
        elif isinstance(res, list):
            chunk = res
        out.extend(chunk)
        if not chunk or len(chunk) < limit:
            break
        offset += limit
    return out

def _is_missed_call(call: Dict[str, Any]) -> bool:
    """
    Пропущений: вхідний (CALL_TYPE=2 або CALL_DIRECTION=2) і тривалість 0
    або відомі "no-answer/busy/timeout" коди.
    """
    call_type = int(call.get("CALL_TYPE") or call.get("CALL_DIRECTION") or 0)
    duration = int(call.get("CALL_DURATION") or call.get("DURATION") or 0)
    failed_code = str(call.get("CALL_FAILED_CODE") or call.get("FAILED_CODE") or "").upper()
    if call_type == 2 and duration == 0:
        return True
    return failed_code in {"NO_ANSWER", "BUSY", "603", "486", "480", "408"}

def _extract_operator_name(call: Dict[str, Any]) -> str:
    """
    Під Voximplant/Telephony ключі відрізняються. Пробуємо по черзі.
    """
    for key in ("PORTAL_USER_NAME", "USER_NAME", "ACCOUNT_USER_NAME", "OPERATOR_NAME"):
        val = (call.get(key) or "").strip()
        if val:
            return val
    uid = call.get("PORTAL_USER_ID") or call.get("USER_ID") or call.get("ACCOUNT_USER_ID")
    return f"ID {uid}" if uid is not None else "невідомий"

async def build_telephony_summary(frm_iso: str, to_iso: str) -> Dict[str, Any]:
    calls = await _b24_vox_fetch(frm_iso, to_iso)
    source = "voximplant"
    if not calls:
        calls = await _b24_telephony_fetch(frm_iso, to_iso)
        source = "telephony"
    log.info("[telephony] source=%s, got=%s records", source, len(calls))

    missed_total = 0
    out_total = 0
    per_user: Dict[str, Dict[str, int]] = {}

    for c in calls:
        call_type = int(c.get("CALL_TYPE") or c.get("CALL_DIRECTION") or 0)  # 1=out, 2=in
        if call_type == 1:
            out_total += 1
            name = _extract_operator_name(c)
            per_user.setdefault(name, {"missed": 0, "out": 0})
            per_user[name]["out"] += 1
        elif call_type == 2 and _is_missed_call(c):
            missed_total += 1
            name = _extract_operator_name(c)
            per_user.setdefault(name, {"missed": 0, "out": 0})
            per_user[name]["missed"] += 1

    return {"missed_total": missed_total, "out_total": out_total, "operators": per_user, "source": source}

# ------------------------ Summary builder -----------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    type_map = await get_deal_type_map()
    conn_type_ids = await _connection_type_ids()

    # A) Подали сьогодні (підключення)
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

    # B) Закрили сьогодні (підключення) по CLOSEDATE
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

    # C) Активні на бригадах (підключення)
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

    # D) Кат.0: точний день / думають (підключення)
    exact_cnt = await _count_open_in_stage(0, c0_exact_stage, conn_type_ids)
    think_cnt = await _count_open_in_stage(0, c0_think_stage, conn_type_ids)

    # E) Сервісні «подані сьогодні» (не підключення)
    non_conn_type_ids = [tid for tid in type_map.keys() if tid not in conn_type_ids]
    service_opened_today = await b24_list(
        "crm.deal.list",
        order={"ID": "DESC"},
        filter={
            "CATEGORY_ID": 20,
            "STAGE_ID": list(_BRIGADE_STAGE_FULL),
            "TYPE_ID": non_conn_type_ids,
            ">=DATE_CREATE": frm, "<DATE_CREATE": to,
        },
        select=["ID"]
    )
    service_today = len(service_opened_today)

    # F) Телефонія через Voximplant (фолбек на telephony)
    telephony_stats: Optional[Dict[str, Any]] = None
    try:
        telephony_stats = await build_telephony_summary(frm, to)
    except Exception as e:
        log.warning("telephony stats failed: %s", e)

    log.info("[summary] created=%s (c0_exact=%s + to_brigades=%s), closed=%s, active=%s, exact=%s, think=%s, service_today=%s",
             created_conn, len(created_c0_exact), len(created_to_brigades),
             closed_conn, active_conn, exact_cnt, think_cnt, service_today)

    out: Dict[str, Any] = {
        "date_label": label,
        "connections": {"created": created_conn, "closed": closed_conn, "active": active_conn},
        "cat0": {"exact_day": exact_cnt, "think": think_cnt},
        "service": {"today": service_today},
    }
    if telephony_stats:
        out["telephony"] = telephony_stats
    return out

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]; c0 = d["cat0"]
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
        "",
        "━━━━━━━━━━━━━━━",
        "⚙️ <b>Сервісні заявки</b>",
        f"📨 Подали сьогодні: <b>{d['service']['today']}</b>",
    ]

    if "telephony" in d:
        t = d["telephony"]
        lines += [
            "",
            "━━━━━━━━━━━━━━━",
            "📞 <b>Телефонія</b>",
            f"🔕 Пропущених: <b>{t.get('missed_total', 0)}</b>",
            f"📤 Набраних: <b>{t.get('out_total', 0)}</b>",
        ]
        ops = t.get("operators", {})
        if ops:
            lines += ["", "👥 По операторам:"]
            for name, vals in ops.items():
                lines.append(f"• {name}: пропущені — <b>{vals.get('missed',0)}</b>, набрані — <b>{vals.get('out',0)}</b>")

    lines += ["", "━━━━━━━━━━━━━━━"]
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
