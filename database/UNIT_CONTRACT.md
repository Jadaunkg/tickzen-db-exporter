# Storage-unit contract (producer side)

Every number in the database has a unit that belongs to its `table.column` — see `column_units.json`
(an identical copy ships with the Next.js ticker-page app, which converts for display; a test in each
repo fails if the two copies differ). Full picture: the Next.js repo's `docs/DATA_CONTRACT.md`.

**Producer duties**

1. Convert vendor units **once**, at the mapper, with the field's *known* unit (never by magnitude):
   yfinance `debtToEquity` is percentage points; margins/ROE/growth/`payoutRatio` are fractions;
   `heldPercent*`/`shortPercentOfFloat` are fractions that the ownership table stores as percentage points.
2. Never publish a manufactured value: `reconcile_margin` (no 0.0 margin the statements disagree with),
   `next_earnings_date` (only dates still in the future, else NULL), no per-zone constant standing in for
   a bankruptcy probability.
3. Every write goes through `AzureTableQuery.insert/upsert` or `replace_rows_atomic`, which call
   `unit_contract.guard_records` (bounds are wide: they catch a percentage written into a fraction
   column or a NaN, nothing subtler). `TICKZEN_UNIT_GUARD=enforce|warn|off` (default `enforce`: the bad
   *value* becomes NULL and is logged; the rest of the row is still written).
4. After each export run `python database/validate_unit_contract.py` (read-only; exit 1 on violations).
   It checks the schema, the bounds on the latest rows, **cross-table invariants** (the same quantity in
   two units must agree — this is how a unit swap is detected without guessing), staleness/zeros, and
   that the sitemap's stock list matches which pages can render.

**Adding a column**: add it to `column_units.json` (both repos), map it with its real vendor unit,
and add a case to `tests/database/test_unit_contract.py`.
