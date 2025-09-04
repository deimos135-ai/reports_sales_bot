# ------------------------ Telephony via Bitrix (improved) ------------
_MISSED_CODES = {"NO_ANSWER", "MISSED", "304", "480", "486", "487", "603"}

def _ctype_val(x: Any) -> str:
    s = str(x or "").upper()
    if s in {"1", "INCOMING"}: return "INCOMING"
    if s in {"2", "OUTGOING"}: return "OUTGOING"
    return s

def _disp_name(uid: str) -> str:
    return TELEPHONY_OPERATORS.get(uid, None) or (f"Інші (тех.)" if not TELEPHONY_SHOW_OTHER else f"ID {uid}")

def _is_known_op(uid: str) -> bool:
    return uid in TELEPHONY_OPERATORS

async def build_telephony_stats(offset_days: int = 0) -> Dict[str, Any]:
    _, frm_local, to_local = _day_bounds_local_strings(offset_days)

    rows = await b24_list(
        "voximplant.statistic.get",
        order={"CALL_START_DATE": "ASC"},
        filter={">=CALL_START_DATE": frm_local, "<CALL_START_DATE": to_local},
        select=[
            "CALL_ID","CALL_SESSION_ID","CALL_TYPE","CALL_DURATION",
            "PORTAL_USER_ID","CALL_START_DATE","CALL_FAILED_CODE"
        ]
    )

    # 1) групуємо по CALL_ID
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cid = str(r.get("CALL_ID") or r.get("CALL_SESSION_ID") or "")
        if not cid:
            # якщо зовсім немає ідентифікатора — пропускаємо
            continue
        groups.setdefault(cid, []).append(r)

    def best_record(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        # a) відповіли: duration>0, є оператор → max duration
        answered = [x for x in items if int(x.get("CALL_DURATION") or 0) > 0]
        answered_with_user = [x for x in answered if str(x.get("PORTAL_USER_ID") or "").strip()]
        if answered_with_user:
            return max(answered_with_user, key=lambda x: int(x.get("CALL_DURATION") or 0))
        if answered:
            return max(answered, key=lambda x: int(x.get("CALL_DURATION") or 0))
        # b) пропущені: код з переліку
        missed = [x for x in items if str(x.get("CALL_FAILED_CODE") or "").upper().replace(" ", "_") in _MISSED_CODES]
        if missed:
            # візьмемо найпізніший по часу як репрезентативний
            return sorted(missed, key=lambda x: str(x.get("CALL_START_DATE") or ""))[-1]
        # c) фолбек — найпізніший
        return sorted(items, key=lambda x: str(x.get("CALL_START_DATE") or ""))[-1]

    uniq = [best_record(v) for v in groups.values()]

    # 2) класифікація
    missed_total = 0
    incoming_total = 0          # прийняті
    incoming_all = 0            # всі вхідні (контроль)
    outgoing_total = 0

    incoming_by_op: Dict[str, int] = {}
    outgoing_by_op: Dict[str, int] = {}

    for r in uniq:
        ctype = _ctype_val(r.get("CALL_TYPE"))
        dur = int(r.get("CALL_DURATION") or 0)
        failed = str(r.get("CALL_FAILED_CODE") or "").upper().replace(" ", "_")
        uid = str(r.get("PORTAL_USER_ID") or "").strip()

        if ctype == "INCOMING":
            incoming_all += 1
            is_missed = (dur == 0) or (failed in _MISSED_CODES)
            if is_missed:
                missed_total += 1
            else:
                incoming_total += 1
                if _is_known_op(uid):
                    name = _disp_name(uid)
                    incoming_by_op[name] = incoming_by_op.get(name, 0) + 1

        elif ctype == "OUTGOING":
            outgoing_total += 1
            if _is_known_op(uid):
                name = _disp_name(uid)
                outgoing_by_op[name] = outgoing_by_op.get(name, 0) + 1

    log.info(
        "[telephony] uniq_calls=%s, missed=%s, incoming_answered=%s, outgoing=%s, incoming_all=%s",
        len(uniq), missed_total, incoming_total, outgoing_total, incoming_all
    )

    return {
        "missed_total": missed_total,
        "incoming_total": incoming_total,
        "incoming_all": incoming_all,
        "outgoing_total": outgoing_total,
        "incoming_by_operator": incoming_by_op,
        "outgoing_by_operator": outgoing_by_op,
    }
