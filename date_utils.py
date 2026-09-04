from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import calendar
 
TORONTO_TZ = ZoneInfo('America/Toronto')
 
 
def _today() -> date:
    return datetime.now(tz=TORONTO_TZ).date()
 
 
def _is_weekday(d: date = None) -> bool:
    d = d or _today()
    return d.weekday() >= 5
 
 
def _is_business_day(d: date = None) -> bool:
    d = d or _today()
    return not _is_weekday(d)
 
 
def last_month_end(as_of: date = None) -> date:
    as_of = as_of or _today()
    first_of_month = as_of.replace(day=1)
    return first_of_month - timedelta(days=1)
 
 
def last_month_end_day(as_of: date = None) -> int:
    return last_month_end(as_of).day
 
 
def last_quarter_end(as_of: date = None) -> date:
    as_of = as_of or date.today()
    q = (as_of.month - 1) // 3 + 1
    # previous quarter
    if q == 1:
        year, quarter = as_of.year - 1, 4
    else:
        year, quarter = as_of.year, q - 1
    end_month = quarter * 3
    last_day = calendar.monthrange(year, end_month)[1]
    return date(year, end_month, last_day)
 
 
def last_quarter_end_day(as_of: date = None) -> int:
    return last_quarter_end(as_of).day
 
 
def next_quarter_end(as_of: date = None) -> date:
    as_of = as_of or _today()
    quarter = (as_of.month - 1) // 3 + 1
    return date(as_of.year, quarter * 3, calendar.monthrange(as_of.year, quarter * 3)[1])
 
 
def next_quarter_end_day(as_of: date = None) -> int:
    return next_quarter_end(as_of).day
 
 
def nth_business_date(n: int, as_of: date = None) -> date:
    if n <= 0:
        raise ValueError('n must be positive')
    as_of = as_of or _today()
    as_of_temp = date(as_of.year, as_of.month, 1)
    while n > 0:
        as_of_temp += timedelta(days=1)
        if _is_business_day(as_of_temp):
            n -= 1
    return as_of_temp
 
 
def date_to_str(d: date = None, fmt: str = "%Y%m%d") -> str:
    d = d or _today()
    return d.strftime(fmt)
 
 
def str_to_date(s: str, fmt: str = "%Y%m%d") -> date:
    return datetime.strptime(s, fmt).date()
 
 
def get_previous_quarter_end_date(ref_date=None):
    """
    Calculates the last day of the previous calendar quarter.
 
    Args:
        ref_date: The reference date (datetime.date object).
                  Defaults to today if None.
    Returns:
        A datetime.date object representing the last day
        of the previous quarter.
    """
    if ref_date is None:
        ref_date = datetime.date.today()
 
    # Determine the last month of the previous quarter
    if ref_date.month <= 3:
        # If in Q1 (Jan-Mar), previous quarter ended in Dec of the previous year
        prev_q_month = 12
        prev_q_year = ref_date.year - 1
    else:
        # Otherwise, the previous quarter ended 3 months ago
        prev_q_month = ((ref_date.month - 1) // 3) * 3
        prev_q_year = ref_date.year
 
    # Get the number of days in that specific month and year
    # calendar.monthrange returns (weekday of first day, number of days in month)
    days_in_month = calendar.monthrange(prev_q_year, prev_q_month)[1]
 
    # Return the date object for the last day of the previous quarter
    return datetime.strptime(str(prev_q_year) + '-' + str(prev_q_month) + '-' + str(days_in_month), '%Y-%m-%d').date()
 
 
def get_quarter_end_date(q_str):
    # Parse 25Q3 into 2025 and 3
    year = int("20" + q_str[:2])
    quarter = int(q_str[3])
 
    # Map quarter to month (3, 6, 9, 12)
    month = quarter * 3
 
    # Create date object for the first day of the quarter month
    start_date = datetime(year, month, 1)
 
    # Get last day of that quarter using relativedelta
    # end_date = start_date + relativedelta(months=2, day=31)
 
    return datetime.strptime(str(year) + '-' + str(month) + '-' + str(calendar.monthrange(year, month)[1]), '%Y-%m-%d')
 
 
def parse_iq_forecast_date(code: str) -> datetime.date:
    """
    Parse string in format YYQQFCMMDD (e.g., 26Q2FC0511)
    Returns a datetime.date object
    """
    # Extract parts
    year = int(code[:2]) + 2000   # assuming 2000s
    month = int(code[-4:-2])
    day = int(code[-2:])
   
    return datetime(year, month, day).date()
 
 
def to_iq_date_format(dt: date) -> str:
    """
    Convert a date to IQ format: YYQQFCMMDD
    Example: 2026-05-11 -> 26Q2FC0511
    """
    year = dt.year % 100  # last two digits
    month = dt.month
    day = dt.day
 
    # Determine quarter
    quarter = (month - 1) // 3 + 1
 
    return f"{year:02d}Q{quarter}FC{month:02d}{day:02d}"
 
 
def get_next_month_first_day(report_date: str | date | datetime) -> str:
    """
    Convert a quarter-end date to the first day of the next month.
 
    Examples:
        2026-03-31 -> 2026-04-01
        2026-06-30 -> 2026-07-01
        2026-09-30 -> 2026-10-01
        2026-12-31 -> 2027-01-01
 
    Returns:
        str in YYYY-MM-DD format
    """
    if isinstance(report_date, str):
        report_date = datetime.strptime(report_date, "%Y-%m-%d").date()
    elif isinstance(report_date, datetime):
        report_date = report_date.date()
 
    next_month_first_day = (
        report_date.replace(day=1) + relativedelta(months=1)
    )
 
    return next_month_first_day.strftime("%Y-%m-%d")
 
 
def get_qe_key(value: str | date | datetime) -> str:
    """
    Examples:
        2026-03-31 -> 26Q1
        2026-06-30 -> 26Q2
    """
    if isinstance(value, str):
        value = datetime.strptime(value, "%Y-%m-%d").date()
    elif isinstance(value, datetime):
        value = value.date()
 
    quarter = (value.month - 1) // 3 + 1
    return f"{value:%y}Q{quarter}"
 
 
def get_qe_date(qe_key: str) -> str:
    """
    Examples:
        26Q1 -> 2026-03-31
        26Q2 -> 2026-06-30
        26Q3 -> 2026-09-30
        26Q4 -> 2026-12-31
    """
    year = int(f"20{qe_key[:2]}")
    quarter = int(qe_key[-1])
 
    quarter_end_dates = {
        1: "03-31",
        2: "06-30",
        3: "09-30",
        4: "12-31",
    }
 
    if quarter not in quarter_end_dates:
        raise ValueError(f"Invalid QE key: {qe_key}")
 
    return f"{year}-{quarter_end_dates[quarter]}"