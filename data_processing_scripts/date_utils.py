#!/usr/bin/env python3
"""Shared date normalization helpers for financial data frames."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def _strip_timezone(value):
    if value is None or pd.isna(value):
        return pd.NaT

    if isinstance(value, pd.Timestamp):
        return value.tz_localize(None) if value.tzinfo is not None else value

    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value

    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return pd.NaT

    if pd.isna(parsed):
        return pd.NaT

    if isinstance(parsed, pd.Timestamp) and parsed.tzinfo is not None:
        return parsed.tz_localize(None)

    return parsed


def normalize_date_series(series: pd.Series) -> pd.Series:
    """Return a timezone-naive datetime64 series, preserving source calendar dates."""
    if series is None:
        return pd.Series(dtype="datetime64[ns]")

    normalized = series.map(_strip_timezone)
    return pd.to_datetime(normalized, errors="coerce")


def normalize_date_column(df: pd.DataFrame, column: str = "Date", *, drop_invalid: bool = True, sort: bool = True, dedupe: bool = False) -> pd.DataFrame:
    """Normalize a date column in a DataFrame without shifting timezone-aware timestamps."""
    if df is None or df.empty or column not in df.columns:
        return df

    df_copy = df.copy()
    df_copy[column] = normalize_date_series(df_copy[column])

    if drop_invalid:
        df_copy = df_copy.dropna(subset=[column])

    if sort:
        df_copy = df_copy.sort_values(column)

    if dedupe:
        df_copy = df_copy.drop_duplicates(subset=[column], keep="last")

    return df_copy