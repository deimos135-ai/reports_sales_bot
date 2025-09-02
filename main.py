# main.py — reports-bot (тільки сумарний звіт по Підключеннях)
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

_raw_report_chats = os.environ.get("REPORT_CHATS", "")
REPORT_CHATS: Dict[str, int] = json.loads(_raw_report_chats) if _raw_report_chats.strip() else {}
REPORT_SUMMARY_CHAT = int(os.environ.get("REPORT_SUMMARY_CHAT", "0"))  # якщо 0 — беремо REPORT_CHATS["all"]

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
                        log.warning("Bitrix temporary error: %s (%s), retry #%s", err, desc, attempt+1)
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

def normalize_type(type_name: str) -> str:
    t = (type_name or "").strip().lower()
    if t in {"підключення","подключение"}: return "connection"
    if "підключ" in t or "подключ" in t: return "connection"
    return "other"

async def get_type_ids(bucket: str) -> List[str]:
    """список TYPE_ID для кошика ('connection')."""
    m = await get_deal_type_map()
    return [code for code, name in m.items() if normalize_type(name) == bucket]

# --- cache stages per category + helpers ---
_CAT_STAGES: Dict[int, List[Dict[str, str]]] = {}

async def get_category_stages(cat_id: int) -> List[Dict[str, str]]:
    global _CAT_STAGES
    if cat_id not in _CAT_STAGES:
        items = await b24("crm.dealcategory.stage.list", filter={"CATEGORY_ID": cat_id})
        _CAT_STAGES[cat_id] = items or []
        log.info("[cache] CAT%s stages: %s", cat_id, len(_CAT_STAGES[cat_id]))
    return _CAT_STAGES[cat_id]

async def find_stage_code_by_name_contains(cat_id: int, needle: str) -> Optional[str]:
    stages = await get_category_stages(cat_id)
    n = (needle or "").strip().lower()
    for s in stages:
        name = (s.get("NAME") or "").strip().lower()
        if n in name:
            sid = s.get("STATUS_ID")
            if sid:
                return f"C{cat_id}:{sid}"
    return None

async def count_open_in_stage(cat_id: int, stage_code: Optional[str], *, type_ids: Optional[List[str]]) -> int:
    """
    Рахує відкриті угоди у конкретній стадії.
    Якщо type_ids=None — НЕ фільтруємо по TYPE_ID (використовуємо для категорії 0).
    """
    if not stage_code:
        return 0
    flt: Dict[str, Any] = {"CLOSED": "N", "CATEGORY_ID": cat_id, "STAGE_ID": stage_code}
    if type_ids is not None and len(type_ids) > 0:
        flt["TYPE_ID"] = type_ids
    deals = await b24_list("crm.deal.list", order={"ID": "DESC"}, filter=flt, select=["ID"])
    return len(deals)

# ------------------------ Time helpers -------------------
def _day_bounds(offset_days: int = 0) -> Tuple[str, str, str]:
    now_local = datetime.now(REPORT_TZ)
    start_local = (now_local - timedelta(days=offset_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    label = start_local.strftime("%d.%m.%Y")
    return label, start_utc.isoformat(), end_utc.isoformat()

# ------------------------ Company summary (ПІДКЛЮЧЕННЯ) ---------------
async def build_company_summary(offset_days: int = 0) -> Dict[str, Any]:
    """
    Підключення:
      • подані сьогодні (cat=20, DATE_CREATE)
      • закриті сьогодні (cat=20, STAGE=WON, DATE_MODIFY)
      • активні на бригадах (cat=20, відкриті, STAGE ∈ бригадних)
      • на конкретний день (cat=0, відкриті, стадія — без фільтру TYPE_ID)
      • думають (cat=0, відкриті, стадія — без фільтру TYPE_ID)
    """
    label, frm, to = _day_bounds(offset_days)
    type_conn = await get_type_ids("connection")

    # --- cat=20: подані сьогодні
    created_cat20 = await b24_list(
        "crm.deal.list",
        order={"ID":"DESC"},
        filter={">=DATE_CREATE": frm, "<DATE_CREATE": to, "CATEGORY_ID": 20, "TYPE_ID": type_conn},
        select=["ID"],
    )

    # --- cat=20: закриті сьогодні
    closed_cat20 = await b24_list(
        "crm.deal.list",
        order={"DATE_MODIFY":"ASC"},
        filter={"STAGE_ID":"C20:WON", ">=DATE_MODIFY": frm, "<DATE_MODIFY": to, "TYPE_ID": type_conn},
        select=["ID"],
    )

    # --- cat=20: активні на бригадах
    open_conn_cat20 = await b24_list(
        "crm.deal.list",
        order={"ID":"DESC"},
        filter={"CLOSED":"N","CATEGORY_ID":20, "TYPE_ID": type_conn},
        select=["ID","STAGE_ID"],
    )
    # якщо колонки бригад названі через кастомні коди — впишіть тут
    BRIGADE_STAGE_CODES = {"UC_XF8O6V","UC_0XLPCN","UC_204CP3","UC_TNEW3Z","UC_RMBZ37"}
    brigade_stage_ids = {f"C20:{v}" for v in BRIGADE_STAGE_CODES}
    active_brigades = sum(1 for d in open_conn_cat20 if (str(d.get("STAGE_ID") or "") in brigade_stage_ids))

    # --- cat=0: "на конкретний день" та "думають" (без TYPE_ID!)
    stage_specific_day = await find_stage_code_by_name_contains(0, "конкретн")
    stage_thinking     = await find_stage_code_by_name_contains(0, "дума")
    cnt_specific_day = await count_open_in_stage(0, stage_specific_day, type_ids=None)
    cnt_thinking      = await count_open_in_stage(0, stage_thinking,     type_ids=None)

    return {
        "date_label": label,
        "connections": {
            "created_cat20": len(created_cat20),
            "closed_cat20": len(closed_cat20),
            "active_brigades_cat20": active_brigades,
            "specific_day_cat0": cnt_specific_day,
            "thinking_cat0": cnt_thinking,
        },
    }

def format_company_summary(d: Dict[str, Any]) -> str:
    dl = d["date_label"]
    c = d["connections"]
    lines = [
        f"📅 <b>Дата: {dl}</b>",
        "",
        "📌 <b>Підключення</b>",
        f"Всього подали (кат.20) — <b>{c['created_cat20']}</b>",
        f"Закрили сьогодні (кат.20) — <b>{c['closed_cat20']}</b>",
        f"Активних на бригадах (кат.20) — <b>{c['active_brigades_cat20']}</b>",
        "",
        f"На конкретний день (кат.0) — <b>{c['specific_day_cat0']}</b>",
        f"Думають (кат.0) — <b>{c['thinking_cat0']}</b>",
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

def _resolve_summary_chat() -> Optional[int]:
    if REPORT_SUMMARY_CHAT: return REPORT_SUMMARY_CHAT
    if "all" in REPORT_CHATS: return int(REPORT_CHATS["all"])
    return None

async def send_company_summary(offset_days: int = 0) -> None:
    chat_id = _resolve_summary_chat()
    if not chat_id:
        log.warning("No REPORT_SUMMARY_CHAT nor REPORT_CHATS['all'] configured")
        return
    try:
        data = await build_company_summary(offset_days)
        await _safe_send(chat_id, format_company_summary(data))
    except Exception:
        log.exception("company summary failed")
        await _safe_send(chat_id, "❗️Помилка формування комбінованого звіту")
    
# ---- ADD: pretty table for category stages ---------------------------------
def _pad(s: str, n: int) -> str:
    s = str(s or "")
    return s + " " * max(0, n - len(s))

async def render_category_stages_table(cat_id: int) -> str:
    """
    Повертає текст з таблицею стадій для категорії cat_id:
    колонки: STATUS_ID, NAME
    """
    stages = await get_category_stages(cat_id)
    rows = [(s.get("STATUS_ID", ""), s.get("NAME", "")) for s in stages]

    # розміри колонок
    w_id = max([len("STATUS_ID")] + [len(x[0]) for x in rows]) + 2
    w_nm = max([len("NAME")] + [len(x[1]) for x in rows]) + 2

    # збираємо таблицю у code-block
    out = []
    out.append(f"Категорія: {cat_id}\n")
    out.append("<code>")
    out.append(_pad("STATUS_ID", w_id) + _pad("NAME", w_nm))
    out.append(_pad("-" * len("STATUS_ID"), w_id) + _pad("-" * len("NAME"), w_nm))
    for sid, name in rows:
        out.append(_pad(sid, w_id) + _pad(name, w_nm))
    out.append("</code>")
    out.append(
        "\nПорада: знайди тут точні назви колонок для "
        "«На конкретний день» та «Думають» і впиши їх у match-логіку."
    )
    return "\n".join(out)

# ---- ADD: command handler to print stages -----------------------------------
@dp.message(Command("cat_stages"))
async def cmd_cat_stages(m: Message):
    """
    /cat_stages          -> покаже стадії категорії 0
    /cat_stages 20       -> покаже стадії категорії 20
    """
    parts = (m.text or "").split()
    try:
        cat_id = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        cat_id = 0

    await m.answer("🔎 Збираю стадії категорії…")
    try:
        text = await render_category_stages_table(cat_id)
        await m.answer(text)
    except Exception as e:
        log.exception("cat_stages failed")
        await m.answer(f"❗️Помилка завантаження стадій: {html.escape(str(e))}")

# ------------------------ Manual command -----------------
@dp.message(Command("report_now"))
async def report_now(m: Message):
    # /report_now [offset]
    parts = (m.text or "").split()
    offset = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
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
