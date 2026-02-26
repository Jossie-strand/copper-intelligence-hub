"""
feeds/dashboard.py

Shared Dashboard writer used by all three exchange feed scripts.

Row key: DATA DATE (the date the exchange data is for), not today's run date.
This ensures all three feeds always find/update the same row regardless of
run order or timing — even if jobs run in parallel.

Dashboard column layout (0-based index):
  A=0   Data Date              ← exchange report/activity date
  B=1   COMEX Total (mt)
  C=2   COMEX Change (mt)
  D=3   COMEX Registered (mt)
  E=4   COMEX Eligible (mt)
  F=5   COMEX Reg/Total %      ← calculated: D/B
  G=6   LME Total (mt)
  H=7   LME Change (mt)
  I=8   LME Cancelled Warrants (mt)  ← manual entry
  J=9   LME Cancelled %        ← calculated: I/G
  K=10  SHFE Total (mt)
  L=11  SHFE Change (mt)
  M=12  Combined Total (mt)    ← calculated: sum of available exchange totals
  N=13  Combined Change (mt)   ← calculated: only when all 3 present
  O=14  WoW Change (mt)        ← same weekday last week vs M
"""

import datetime

DASHBOARD_HEADERS = [
    "Data Date",
    "COMEX Total (mt)", "COMEX Change (mt)",
    "COMEX Registered (mt)", "COMEX Eligible (mt)", "COMEX Reg/Total %",
    "LME Total (mt)", "LME Change (mt)",
    "LME Cancelled Warrants (mt)", "LME Cancelled %",
    "SHFE Total (mt)", "SHFE Change (mt)",
    "Combined Total (mt)", "Combined Change (mt)",
    "WoW Change (mt)",
]

COL_DATE       = 0
COL_COMEX_T    = 1
COL_COMEX_C    = 2
COL_COMEX_REG  = 3
COL_COMEX_ELIG = 4
COL_COMEX_PCT  = 5
COL_LME_T      = 6
COL_LME_C      = 7
COL_LME_CW     = 8   # manual
COL_LME_CW_PCT = 9
COL_SHFE_T     = 10
COL_SHFE_C     = 11
COL_COMB_T     = 12
COL_COMB_C     = 13
COL_WOW        = 14

NUM_COLS = 15

EXCHANGE_MAP = {
    "COMEX": {"total": COL_COMEX_T, "change": COL_COMEX_C,
               "extras": {"registered": COL_COMEX_REG, "eligible": COL_COMEX_ELIG}},
    "LME":   {"total": COL_LME_T,   "change": COL_LME_C,   "extras": {}},
    "SHFE":  {"total": COL_SHFE_T,  "change": COL_SHFE_C,  "extras": {}},
}


def ensure_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Data Date":
        tab.update("A1:O1", [DASHBOARD_HEADERS], value_input_option="USER_ENTERED")
        print("  ✅ Dashboard headers written")


def _safe_float(val):
    try:
        if val == "" or val is None:
            return None
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _calc_derived(row):
    row = list(row)
    while len(row) < NUM_COLS:
        row.append("")

    comex_t   = _safe_float(row[COL_COMEX_T])
    comex_c   = _safe_float(row[COL_COMEX_C])
    comex_reg = _safe_float(row[COL_COMEX_REG])
    lme_t     = _safe_float(row[COL_LME_T])
    lme_c     = _safe_float(row[COL_LME_C])
    lme_cw    = _safe_float(row[COL_LME_CW])
    shfe_t    = _safe_float(row[COL_SHFE_T])
    shfe_c    = _safe_float(row[COL_SHFE_C])

    # COMEX Reg/Total %
    row[COL_COMEX_PCT] = round(comex_reg / comex_t * 100, 1) if comex_t and comex_reg else ""

    # LME Cancelled %
    row[COL_LME_CW_PCT] = round(lme_cw / lme_t * 100, 1) if lme_t and lme_cw else ""

    # Combined Total — whatever exchanges have reported
    totals = [v for v in [comex_t, lme_t, shfe_t] if v is not None]
    row[COL_COMB_T] = round(sum(totals), 0) if totals else ""

    # Combined Change — only when all three present
    changes = [v for v in [comex_c, lme_c, shfe_c] if v is not None]
    row[COL_COMB_C] = round(sum(changes), 0) if len(changes) == 3 else ""

    return row


def _calc_wow(tab, data_date, combined_total):
    if combined_total == "" or combined_total is None:
        return ""
    target_date = (data_date - datetime.timedelta(days=7)).isoformat()
    all_dates = tab.col_values(1)
    for i, d in enumerate(all_dates):
        if d == target_date:
            prior_row = tab.row_values(i + 1)
            prior_val = _safe_float(prior_row[COL_COMB_T]) if len(prior_row) > COL_COMB_T else None
            if prior_val is not None:
                return round(combined_total - prior_val, 0)
            break
    return ""


def write_exchange(tab, data_date_str, exchange, total_mt, change_mt, extras=None):
    """
    Write one exchange's data into the Dashboard, keyed on data_date_str.

    data_date_str : ISO date string for the DATA being written (not today's run date)
                    e.g. COMEX activity date, LME report date, SHFE report date
    """
    if exchange not in EXCHANGE_MAP:
        raise ValueError(f"Unknown exchange: {exchange}")

    col_t  = EXCHANGE_MAP[exchange]["total"]
    col_c  = EXCHANGE_MAP[exchange]["change"]
    ex_map = EXCHANGE_MAP[exchange]["extras"]

    try:
        data_date = datetime.date.fromisoformat(data_date_str)
    except ValueError:
        # Fallback to today if date can't be parsed
        data_date = datetime.date.today()
        data_date_str = data_date.isoformat()

    # Find existing row for this data date
    all_dates = tab.col_values(1)
    existing_row_idx = None
    for i, d in enumerate(all_dates):
        if d == data_date_str:
            existing_row_idx = i + 1  # gspread is 1-indexed
            break

    if existing_row_idx:
        print(f"  Dashboard: updating row {existing_row_idx} ({data_date_str}) with {exchange} data")
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
        combined_t = _safe_float(row_data[COL_COMB_T])
        row_data[COL_WOW] = _calc_wow(tab, data_date, combined_t)

        tab.update(
            f"A{existing_row_idx}:O{existing_row_idx}",
            [row_data[:NUM_COLS]],
            value_input_option="USER_ENTERED"
        )
        print(f"  ✅ Dashboard row {existing_row_idx} updated ({exchange}: {total_mt} mt)")

    else:
        print(f"  Dashboard: creating new row for {data_date_str} ({exchange})")
        row_data = [""] * NUM_COLS
        row_data[COL_DATE] = data_date_str
        row_data[col_t]    = total_mt  if total_mt  is not None else ""
        row_data[col_c]    = change_mt if change_mt is not None else ""

        if extras:
            for field, val in extras.items():
                if field in ex_map and val is not None:
                    row_data[ex_map[field]] = val

        row_data = _calc_derived(row_data)
        combined_t = _safe_float(row_data[COL_COMB_T])
        row_data[COL_WOW] = _calc_wow(tab, data_date, combined_t)

        tab.append_row(row_data, value_input_option="USER_ENTERED")
        print(f"  ✅ Dashboard new row created for {data_date_str} ({exchange}: {total_mt} mt)")
