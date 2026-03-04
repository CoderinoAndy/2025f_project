from datetime import datetime
from functools import lru_cache

VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}
MERGE_SORT_MAX_ITEMS = 120


@lru_cache(maxsize=4096)
def _parse_dt_cached(text_value):
    """Parse persisted date strings safely."""
    value = str(text_value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def _parse_dt(value):
    if value is None:
        return None
    return _parse_dt_cached(str(value).strip())


def _dt_sort_value(dt):
    """Convert datetime into a sortable integer without timestamp()."""
    if dt is None:
        return -1
    return dt.toordinal() * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second


def _merge_sorted_rows(left_rows, right_rows):
    """Merge two sorted row lists while preserving stability."""
    merged = []
    left_index = 0
    right_index = 0

    while left_index < len(left_rows) and right_index < len(right_rows):
        if left_rows[left_index][1] <= right_rows[right_index][1]:
            merged.append(left_rows[left_index])
            left_index += 1
        else:
            merged.append(right_rows[right_index])
            right_index += 1

    if left_index < len(left_rows):
        merged.extend(left_rows[left_index:])
    if right_index < len(right_rows):
        merged.extend(right_rows[right_index:])
    return merged


def _merge_sort_rows(rows):
    """Sort rows with merge sort using a precomputed tuple key."""
    if len(rows) <= 1:
        return rows

    midpoint = len(rows) // 2
    left_rows = _merge_sort_rows(rows[:midpoint])
    right_rows = _merge_sort_rows(rows[midpoint:])
    return _merge_sorted_rows(left_rows, right_rows)


def sort_emails(emails, sort_code):
    """Sort mailbox rows using supported sort codes."""
    sort_code = sort_code if sort_code in VALID_SORTS else "date_desc"

    def row_key(email):
        dt_value = _dt_sort_value(_parse_dt(email.get("date")))
        pr_value = int(email.get("priority") or 0)

        if sort_code == "date_desc":
            return (-dt_value,)
        if sort_code == "date_asc":
            return (dt_value,)
        if sort_code == "priority_desc":
            return (-pr_value, -dt_value)
        if sort_code == "priority_asc":
            return (pr_value, -dt_value)
        if sort_code == "unread_first":
            return (bool(email.get("is_read")), -dt_value)
        if sort_code == "read_first":
            return (not bool(email.get("is_read")), -dt_value)
        return (-dt_value,)

    rows = [(email, row_key(email)) for email in emails]
    if len(rows) <= 1:
        return [row[0] for row in rows]

    if len(rows) <= MERGE_SORT_MAX_ITEMS:
        return [row[0] for row in _merge_sort_rows(list(rows))]

    return [row[0] for row in sorted(rows, key=lambda row: row[1])]
