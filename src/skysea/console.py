"""Make reports print on the Windows console.

The console uses code page cp1252: it cannot print Polars' box-drawing table borders, nor names
like 'Istanbul' with a Turkish dotted I. ASCII borders + UTF-8 output (unknown characters become
'?' instead of crashing the script)."""
import sys

import polars as pl


def setup(rows: int = 25, width: int = 200) -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    pl.Config.set_tbl_formatting("ASCII_FULL_CONDENSED").set_tbl_hide_column_data_types(True)
    pl.Config.set_tbl_rows(rows).set_tbl_width_chars(width).set_fmt_str_lengths(40)
