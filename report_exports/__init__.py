"""Excel export builders that replicate the admin team's manual report workbooks.

Every builder is a pure function:  payload (dict)  ->  .xlsx bytes.
No database access happens here, so the layout can be tested without Supabase.
The database layer (fetchers.py) only has to produce the payload shapes documented
in each builder's docstring.
"""
from .registry import REPORTS, build_report  # noqa: F401
