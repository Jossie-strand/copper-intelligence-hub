"""
feeds/mine_dashboard.py

Shared helpers for mine-production-database feeds.
Mirrors the pattern of dashboard.py used by the inventory tracker.

Feed Log column layout (0-based):
  A=0  Run Date
  B=1  Data Date
  C=2  Source
  D=3  Mine / Entity
  E=4  Field Updated
  F=5  Old Value
  G=6  New Value
  H=7  Notes
"""

import datetime

FEED_LOG_HEADERS = [
    "Run Date", "Data Date", "Source",
    "Mine / Entity", "Field Updated",
    "Old Value", "New Value", "Notes",
]


def ensure_feed_log_headers(tab):
    first = tab.row_values(1)
    if not first or first[0] != "Run Date":
        tab.update("A1:H1", [FEED_LOG_HEADERS], value_input_option="USER_ENTERED")
        print("  ✅ Feed Log headers written")


def log_update(tab, data_date_str, source, mine, field, old_val, new_val, notes=""):
    """Append one row to the Feed Log tab."""
    today = datetime.date.today().isoformat()
    row = [today, data_date_str, source, mine, field,
           str(old_val) if old_val is not None else "",
           str(new_val) if new_val is not None else "",
           notes]
    tab.append_row(row, value_input_option="USER_ENTERED")
