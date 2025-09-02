# main.py — sales-reports-bot
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

REPORT_CHAT = int(os.environ.get("REPORT_CHAT", "0"))  # чат для сумарного звіту

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

# ------------------------ Caches --------------------------
_DEAL_TYPE_MAP: Optional[Dict[str, str]] = None
async def get_deal_type_map() -> Dict[str, str]:
    global _DEAL_TYPE_MAP
    if _DEAL_TYPE_MAP is None:
        items = await b24("crm.status.list", filter={"ENTITY_ID": "DEAL_TYPE"})
        _DEAL_TYPE_MAP = {i["STATUS_ID"]: i["NAME"] for i in items}
        log.info("[cache] DEAL_TYPE: %s", len(_DEAL_TYPE_MAP))
    return _DEAL_TYPE_MAP

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ Summary report ------------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    label, frm, to = _day_bounds(offset_days)
    deal_type_map = await get_deal_type_map()

    # Подані в кат.20
    created_conn = await b24_list("crm.deal.list",
        filter={">=DATE_CREATE": frm, "<DATE_CREATE": to, "CATEGORY_ID": 20},
        select=["ID", "TYPE_ID", "STAGE_ID", "CLOSED"])
    connections_created = sum(1 for d in created_conn if deal_type_map.get(d.get("TYPE_ID"), "").lower().startswith("підключ"))

    # Закриті в кат.20
    closed_conn = await b24_list("crm.deal.list",
        filter={">=DATE_MODIFY": frm, "<DATE_MODIFY": to, "CATEGORY_ID": 20, "STAGE_ID": "C20:WON"},
        select=["ID", "TYPE_ID"])
    connections_closed = sum(1 for d in closed_conn if deal_type_map.get(d.get("TYPE_ID"), "").lower().startswith("підключ"))

    # Активні на бригадах (кат.20)
    active_conn = await b24_list("crm.deal.list",
        filter={"CATEGORY_ID": 20, "CLOSED": "N"},
        select=["ID", "TYPE_ID", "STAGE_ID"])
    connections_active = sum(1 for d in active_conn if deal_type_map.get(d.get("TYPE_ID"), "").lower().startswith("підключ"))

    # Категорія 0
    deals_cat0 = await b24_list("crm.deal.list",
        filter={"CATEGORY_ID": 0, "CLOSED": "N"},
        select=["ID", "STAGE_ID"])
    conn_day = sum(1 for d in deals_cat0 if d.get("STAGE_ID") == "5")       # На конкретний день
    conn_think = sum(1 for d in deals_cat0 if d.get("STAGE_ID") == "DETAILS")  # Думають

    return {
        "date_label": label,
        "connections": {
            "created": connections_created,
            "closed": connections_closed,
            "active": connections_active,
            "on_day": conn_day,
            "thinking": conn_think,
        },
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]

    lines = [
        f"📅 Дата: {dl}",
        "",
        "━━━━━━━━━━━━━━━",
        "📌 Підключення",
        f"🆕 Подали: <b>{c['created']}</b>",
        f"✅ Закрили: <b>{c['closed']}</b>",
        f"📊 Активні на бригадах: <b>{c['active']}</b>",
        "",
        f"📅 На конкретний день: <b>{c['on_day']}</b>",
        f"💭 Думають: <b>{c['thinking']}</b>",
        "━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)

# ------------------------ Send helpers -------------------
async def _safe_send(chat_id: int, text: str):
    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text, disable_web_page_preview=True)
            return
        except Exception as e:
            log.warning("telegram send failed: %s, retry #%s", e, attempt+1)
            await _sleep_backoff(attempt)
    log.error("telegram send failed permanently")

async def send_company_summary(offset_days: int = 0) -> None:
    if not REPORT_CHAT:
        log.warning("No REPORT_CHAT configured")
        return
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(REPORT_CHAT, format_company_summary(data))
    except Exception:
        log.exception("company summary failed")
        await _safe_send(REPORT_CHAT, "❗️Помилка формування комбінованого звіту")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    offset = 0
    parts = (m.text or "").split()
    if len(parts) >= 2 and parts[1].isdigit():
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
            log.info("[scheduler] tick -> sending company summary")
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
        BotCommand(command="report_now", description="Ручний запуск звіту (/report_now [offset])"),
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
