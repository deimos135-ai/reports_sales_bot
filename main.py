async def fetch_telephony_for_day(offset_days: int = 0) -> Dict[str, Any]:
    """Телефонія за добу через voximplant.statistic.get (узгоджено з BI звітами Бітрікс)."""
    label, frm, to = _day_bounds(offset_days)

    rows = await b24_list(
        "voximplant.statistic.get",
        filter={">=CALL_START_DATE": frm, "<CALL_START_DATE": to},
        select=[
            "ID", "CALL_TYPE", "CALL_DURATION",
            "PORTAL_USER_ID", "CALL_FAILED_CODE", "CALL_FAILED_REASON"
        ],
        page_size=200
    )

    # Bitrix інколи віддає вхідні як 1 і як 5 (черги/IVR).
    INBOUND_TYPES = {"1", "5"}
    OUTBOUND_TYPES = {"2"}  # вихідні

    missed_total = 0            # пропущені (вхідні з нульовою тривалістю)
    inbound_answered = 0        # вхідні прийняті
    outbound_total = 0          # вихідні з реальною розмовою

    per_inbound: Dict[str, int] = {}
    per_outbound: Dict[str, int] = {}

    # Для діагностики будемо збирати розклад типів
    _types_dist: Dict[str, int] = {}

    for r in rows:
        ctype = _ctype_val(r.get("CALL_TYPE"))
        _types_dist[ctype] = _types_dist.get(ctype, 0) + 1

        uid = str(r.get("PORTAL_USER_ID") or "")
        duration = int(r.get("CALL_DURATION") or 0)

        if ctype in INBOUND_TYPES:
            if duration <= 0:
                missed_total += 1
            else:
                inbound_answered += 1
                if uid:
                    per_inbound[uid] = per_inbound.get(uid, 0) + 1

        elif ctype in OUTBOUND_TYPES:
            # Узгоджуємо з BI: враховуємо лише успішні (було з’єднання)
            if duration > 0:
                outbound_total += 1
                if uid:
                    per_outbound[uid] = per_outbound.get(uid, 0) + 1

    log.info("[telephony] rows=%s, types=%s", len(rows), _types_dist)

    return {
        "missed_total": missed_total,
        "inbound_answered": inbound_answered,
        "outbound_total": outbound_total,
        "per_inbound": per_inbound,
        "per_outbound": per_outbound,
        "date_label": label,
        "sample_size": len(rows),
        "types_dist": _types_dist,  # залишив у payload на випадок подальшого тюнінгу
    }
