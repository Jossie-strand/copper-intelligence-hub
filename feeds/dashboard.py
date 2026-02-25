"""
feeds/dashboard.py

Shared Dashboard writer used by all three exchange feed scripts.
Implements "write when any exchange reports" pattern:
  - If today's row exists → update only this exchange's columns + recalculate derived
  - If today's row doesn't exist → create new row with blanks for other exchanges

Dashboard column layout (0-based index, 1-based col ref for gspread):
  A=0   Date
  B=1   COMEX Total (mt)
  C=2   COMEX Change (mt)
  D=3   COMEX Registered (mt)       ← auto from COMEX feed
  E=4   COMEX Eligible (mt)         ← auto from COMEX feed
  F=5   COMEX Reg/Total %           ← calculated: D/B
  G=6   LME Total (mt)
  H=7   LME Change (mt)
  I=8   LME Cancelled Warrants (mt) ← manual entry (future feed)
  J=9   LME Cancelled %             ← calculated: I/G
  K=10  SHFE Total (mt)
  L=11  SHFE Change (mt)
  M=12  Combined Total (mt)         ← calculated: B+G+K
  N=13  Combined Change (mt)        ← calculated: C+H+L
  O=14  WoW Change (mt)             ← same weekday last week vs M
"""

import datetime

DASHBOARD_HEADERS = [
    "Date",
    "COMEX Total (mt)", "COMEX Change (mt)",
    "COMEX Registered (mt)", "COMEX Eligible (mt)", "COMEX Reg/Total %",
    "LME Total (mt)", "LME Change (mt)",
    "LME Cancelled Warrants (mt)", "LME Cancelled %",
    "SHFE Total (mt)", "SHFE Change (mt)",
    "Combined Total (mt)", "Combined Change (mt)",
    "WoW Change (mt)",
]

# Column indices (0-based)
COL_DATE       = 0   # A
COL_COMEX_T    = 1   # B
COL_COMEX_C    = 2   # C
COL_COMEX_REG  = 3   # D
COL_COMEX_ELIG = 4   # E
COL_COMEX_PCT  = 5   # F  — calculated
COL_LME_T      = 6   # G
COL_LME_C      = 7   # H
COL_LME_CW     = 8   # I  — manual / future feed
COL_LME_CW_PCT = 9   # J  — calculated
COL_SHFE_T     = 10  # K
COL_SHFE_C     = 11  # L
COL_COMB_T     = 12  # M  — calculated
COL_COMB_C     = 13  # N  — calculated
COL_WOW        = 14  # O  — calculated

NUM_COLS = 15

# Maps exchange name → (total_col, change_col, extra_cols_dict)
EXCHANGE_MAP = {
    "COMEX": {
        "total":  COL_COMEX_T,
        "change": COL_COMEX_C,
        "extras": {
            "registered": COL_COMEX_REG,
            "eligible":   COL_COMEX_ELIG,
        }
    },
    "LME": {
        "total":  COL_LME_T,
        "change": COL_LME_C,
        "extras": {}
    },
    "SHFE": {
        "total":  COL_SHFE_T,
        "change": COL_SHFE_C,
        "extras": {}
    },
}


def ensure_headers(tab):
    """Write header row if sheet is empty or header is missing."""
    first = tab.row_values(1)
    if not first or first[0] != "Date":
        tab.insert_row(DASHBOARD_HEADERS, 1)
        print("  ✅ Dashboard headers written")
    else:
        # Patch any missing headers without destroying existing data
        if len(first) < NUM_COLS:
            tab.update("A1:O1", [DASHBOARD_HEADERS], value_input_option="USER_ENTERED")
            print("  ✅ Dashboard headers updated to full schema")


def _safe_float(val):
    try:
        if val == "" or val is None:
            return None
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _calc_derived(row):
    """
    Recalculate all derived columns from current row values.
    Returns updated row list.
    """
    row = list(row)
    while len(row) < NUM_COLS:
        row.append("")

    comex_t    = _safe_float(row[COL_COMEX_T])
    comex_c    = _safe_float(row[COL_COMEX_C])
    comex_reg  = _safe_float(row[COL_COMEX_REG])
    lme_t      = _safe_float(row[COL_LME_T])
    lme_c      = _safe_float(row[COL_LME_C])
    lme_cw     = _safe_float(row[COL_LME_CW])
    shfe_t     = _safe_float(row[COL_SHFE_T])
    shfe_c     = _safe_float(row[COL_SHFE_C])

    # COMEX Reg/Total %
    if comex_t and comex_reg:
        row[COL_COMEX_PCT] = round(comex_reg / comex_t * 100, 1)
    else:
        row[COL_COMEX_PCT] = ""

    # LME Cancelled %
    if lme_t and lme_cw:
        row[COL_LME_CW_PCT] = round(lme_cw / lme_t * 100, 1)
    else:
        row[COL_LME_CW_PCT] = ""

    # Combined Total — sum whatever exchanges have reported
    totals = [v for v in [comex_t, lme_t, shfe_t] if v is not None]
    row[COL_COMB_T] = round(sum(totals), 0) if totals else ""

    # Combined Change — only sum if all three present (partial day = blank)
    changes = [v for v in [comex_c, lme_c, shfe_c] if v is not None]
    row[COL_COMB_C] = round(sum(changes), 0) if len(changes) == 3 else ""

    return row


def _calc_wow(tab, today_date, combined_total):
    """
    Find same weekday last week, return WoW delta on combined total.
    Only populated once all three exchanges have reported (combined_total is set).
    """
    if combined_total == "" or combined_total is None:
        return ""

    target_date = (today_date - datetime.timedelta(days=7)).isoformat()
    all_dates = tab.col_values(1)

    for i, d in enumerate(all_dates):
        if d == target_date:
            prior_row = tab.row_values(i + 1)
            prior_val = _safe_float(prior_row[COL_COMB_T]) if len(prior_row) > COL_COMB_T else None
            if prior_val is not None:
                return round(combined_total - prior_val, 0)
            break
    return ""


def write_exchange(tab, today_date_str, exchange, total_mt, change_mt, extras=None):
    """
    Write one exchange's data into the Dashboard.

    Parameters
    ----------
    tab           : gspread worksheet (Dashboard tab)
    today_date_str: ISO date string e.g. "2026-02-25"
    exchange      : "COMEX", "LME", or "SHFE"
    total_mt      : float or None
    change_mt     : float or None
    extras        : dict of extra columns e.g. {"registered": 381935, "eligible": 219102}
    """
    if exchange not in EXCHANGE_MAP:
        raise ValueError(f"Unknown exchange: {exchange}")

    today = today_date_str
    today_date = datetime.date.fromisoformat(today)
    col_t  = EXCHANGE_MAP[exchange]["total"]
    col_c  = EXCHANGE_MAP[exchange]["change"]
    ex_map = EXCHANGE_MAP[exchange]["extras"]

    # ── Find existing row ────────────────────────────────────
    all_dates = tab.col_values(1)
    existing_row_idx = None
    for i, d in enumerate(all_dates):
        if d == today:
            existing_row_idx = i + 1  # gspread is 1-indexed
            break

    if existing_row_idx:
        # ── UPDATE ───────────────────────────────────────────
        print(f"  Dashboard: updating row {existing_row_idx} ({today}) with {exchange} data")
        row_data = tab.row_values(existing_row_idx)
        while len(row_data) < NUM_COLS:
            row_data.append("")

        row_data[col_t] = total_mt  if total_mt  is not None else ""
        row_data[col_c] = change_mt if change_mt is not None else ""

        if extras:
            for field, val in extras.items():
                if field in ex_map and val is not None:
                    row_data[ex_map[field]] = val

        row_data = _calc_derived(row_data)

        # WoW — recalculate (combined may now be complete)
        combined_t = _safe_float(row_data[COL_COMB_T])
        row_data[COL_WOW] = _calc_wow(tab, today_date, combined_t)

        tab.update(
            f"A{existing_row_idx}:O{existing_row_idx}",
            [row_data[:NUM_COLS]],
            value_input_option="USER_ENTERED"
        )
        print(f"  ✅ Dashboard row {existing_row_idx} updated")

    else:
        # ── CREATE ───────────────────────────────────────────
        print(f"  Dashboard: creating new row for {today} ({exchange})")
        row_data = [""] * NUM_COLS
        row_data[COL_DATE] = today
        row_data[col_t]    = total_mt  if total_mt  is not None else ""
        row_data[col_c]    = change_mt if change_mt is not None else ""

        if extras:
            for field, val in extras.items():
                if field in ex_map and val is not None:
                    row_data[ex_map[field]] = val

        row_data = _calc_derived(row_data)

        combined_t = _safe_float(row_data[COL_COMB_T])
        row_data[COL_WOW] = _calc_wow(tab, today_date, combined_t)

        tab.append_row(row_data, value_input_option="USER_ENTERED")
        print(f"  ✅ Dashboard new row created for {today}")
