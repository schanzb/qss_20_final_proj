# 02_clean.py — Documentation

## Purpose

Reads the raw imported tables from `data/citizens_united.db` and builds
analysis-ready derived tables. Applies all standard filters (Z9/Z4 exclusions,
valid transaction types), inflation-adjusts all dollar amounts to constant 2024
dollars, and constructs the partisan reclassification logic that is central to
research questions 2 and 3.

**Input:** `data/citizens_united.db` (must be populated by `01_import.py`)
**Output:** 6 derived tables + 21 additional indexes in the same database
**Runtime:** ~5–15 minutes (dominated by large JOIN queries on indexed tables)

---

## Derived Tables Created (in dependency order)

```
1. pres_candidates          — presidential candidates only, deduplicated, with era tag
2. indivs_to_pres           — individual contributions to presidential candidates
3. pacs_to_pres             — PAC contributions / IEs to presidential candidates
4. exp527_aligned           — 527 expenditures with partisan alignment
5. partisan_spending_monthly — monthly time series, all spending pro-R / pro-D
6. partisan_spending_weekly  — weekly time series for counter-spending (Q3)
```

---

## Standard Filters Applied Everywhere

```sql
WHERE RealCode NOT LIKE 'Z9%'    -- exclude non-contributions (e.g. refunds)
  AND RealCode NOT LIKE 'Z4%'    -- exclude joint fundraising transfers
```

For `individual_contributions` only:
```sql
AND Type IN ('10','11','15','15E','15J','22Y')  -- valid FEC individual contribution codes
```

---

## Inflation Adjustment

Every dollar amount column is joined to `cpi_factors` and multiplied by the
cycle's CPI-U factor to produce an `Amount_2024` column in constant 2024
dollars. Factors used:

| Cycle | Factor |
|---|---|
| 2004 | ×1.6653 |
| 2008 | ×1.4611 |
| 2012 | ×1.3607 |
| 2016 | ×1.3018 |

Raw `Amount` is preserved alongside `Amount_2024` in all derived tables.

---

## Date Handling

OpenSecrets dates are in `MM/DD/YYYY` format. SQLite's date functions require
`YYYY-MM-DD`. The script converts inline using `substr()`:

```sql
-- ISO conversion used throughout
substr(Date, 7, 4) || '-' || substr(Date, 1, 2) || '-' || substr(Date, 4, 2)

-- Month extraction
substr(Date, 1, 2)

-- ISO year-week (for weekly table)
strftime('%Y-%W', <iso_date>)
```

---

## Pseudocode / Logic Flow

```
main()
│
├── connect_db()               # WAL mode, 2GB cache (same as import)
│
├── VERIFY raw tables are non-empty, else exit
│
├── create_pres_candidates()
│   ├── SELECT from candidates WHERE DistIDRunFor = 'PRES'
│   ├── GROUP BY Cycle, CID     ← deduplicates duplicate rows in raw data
│   ├── MAX() on other fields   ← picks one value from duplicates (all identical)
│   └── CASE Cycle → era ('pre_CU' for 2004/2008, 'post_CU' for 2012/2016)
│
├── create_indivs_to_pres()
│   ├── SELECT from individual_contributions
│   ├── INNER JOIN pres_candidates ON RecipID = CID AND Cycle = Cycle
│   │   └── This join is also the anti-double-counting filter:
│   │       individual contributions TO PACs have RecipID starting with 'C'
│   │       (committee IDs), which never match a candidate CID starting with 'N'
│   ├── LEFT JOIN cpi_factors → compute Amount_2024
│   ├── Apply Z9/Z4/Type filters
│   └── CASE Party → partisan_direction ('pro_R', 'pro_D', 'unaligned')
│
├── create_pacs_to_pres()
│   ├── UNION ALL of two sources:
│   │   ├── pacs_to_candidates INNER JOIN pres_candidates ON CandID = CID
│   │   │   ├── Preserves DI field ('D'=direct, 'I'=independent — THE CU metric)
│   │   │   ├── Partisan reclassification uses Type field:
│   │   │   │   ├── Type IN (24E,24C,24F,24K,24Z) → supports recipient's party
│   │   │   │   └── Type IN (24A,24N) → AGAINST candidate → flip to opposing party
│   │   │   └── Apply Z9/Z4 filters on PrimCode
│   │   └── pac_to_pac INNER JOIN pres_candidates ON RecipCommID = CID
│   │       └── Catches PAC→candidate records the FEC filed in the wrong table
│   └── Amount_2024 via LEFT JOIN cpi_factors
│
├── create_exp527_aligned()
│   ├── Build temp table _tmp_cmte527_latest: one row per EIN using MAX(Year)
│   │   └── Handles EINs that appear in multiple quarters with different ViewPt
│   ├── SELECT from expenditures_527
│   ├── LEFT JOIN _tmp_cmte527_latest ON EIN
│   │   └── ViewPt: 'C' → pro_R, 'L' → pro_D, other → unaligned
│   ├── Filter: Ctype = 'F' (federal-focus committees only)
│   ├── Derive Cycle from QuarterYr field (e.g. 'Q408' → '2008')
│   └── Filter: derived Cycle must be one of our 4 election cycles
│
├── create_partisan_monthly()
│   └── UNION ALL of 4 streams, each grouped by (Cycle, era, Year, Month, spending_type, partisan_direction):
│       ├── individual contributions   → spending_type = 'individual'
│       ├── pacs_to_pres WHERE DI='D' → spending_type = 'pac_direct'
│       ├── pacs_to_pres WHERE DI='I' → spending_type = 'pac_independent'
│       └── exp527_aligned            → spending_type = '527'
│
├── create_partisan_weekly()
│   └── Same structure as monthly, but grouped by ISO year-week (YYYY-WW)
│       instead of Year + Month. Requires length(Date) = 10 to skip malformed dates.
│
├── create_derived_indexes()   # 21 indexes on derived tables
│
└── validate()                 # Sanity checks — see Validation section below
```

---

## Partisan Reclassification Logic

This is the core transformation for Q2 and Q3. The goal is to classify every
dollar as either `pro_R` or `pro_D` based on its *effective* partisan direction,
not just the recipient.

### For PAC contributions (`pacs_to_pres`)

```sql
CASE
  -- Expenditures FOR a candidate (or direct contributions) → that candidate's party
  WHEN c.Party = 'R' AND p.Type IN ('24E','24C','24F','24K','24Z') THEN 'pro_R'
  WHEN c.Party = 'D' AND p.Type IN ('24E','24C','24F','24K','24Z') THEN 'pro_D'
  -- Independent expenditures AGAINST a candidate → reclassify as supporting opponent
  WHEN c.Party = 'D' AND p.Type IN ('24A','24N') THEN 'pro_R'
  WHEN c.Party = 'R' AND p.Type IN ('24A','24N') THEN 'pro_D'
  ELSE 'unaligned'
END
```

- `24A` / `24N` = IE against a candidate. Spending $1M against the Democrat
  is functionally the same as spending $1M for the Republican, so it is
  reclassified as `pro_R`.

### For individual contributions (`indivs_to_pres`)

Individuals give directly to candidates, so direction is simply the
recipient's party:

```sql
CASE c.Party WHEN 'R' THEN 'pro_R' WHEN 'D' THEN 'pro_D' ELSE 'unaligned' END
```

### For 527 expenditures (`exp527_aligned`)

527 orgs don't contribute to specific candidates; they spend on issue
advocacy. Partisan direction comes from the committee's `ViewPt` field:

```
ViewPt = 'C'  →  pro_R
ViewPt = 'L'  →  pro_D
ViewPt = 'N' or 'U'  →  unaligned
```

---

## Anti-Double-Counting

Individual contributions that go *to a PAC* (rather than directly to a
candidate) will reappear later as a PAC contribution. Counting both would
double-count that money.

The fix is structural: `indivs_to_pres` JOINs `individual_contributions` to
`pres_candidates` on `RecipID = CID`. Candidate CIDs start with `N`
(e.g. `N00000245`). PAC/committee IDs start with `C`. An individual giving
to a PAC has `RecipID` like `C00123456`, which will never match a candidate
CID, so those rows are automatically excluded by the INNER JOIN.

---

## 527 Cycle Derivation

527 files don't have a `Cycle` column. Instead they have a `QuarterYr` field
in the format `Q[1-4][YY]` (e.g. `Q408` = Q4 of 2008). The script maps these
to election cycles by extracting the 2-digit year and converting:

```sql
CASE CAST('20' || substr(QuarterYr, 3, 2) AS INTEGER)
    WHEN 2003 THEN '2004'   -- pre-election year counts toward that cycle
    WHEN 2004 THEN '2004'
    WHEN 2007 THEN '2008'
    WHEN 2008 THEN '2008'
    WHEN 2011 THEN '2012'
    WHEN 2012 THEN '2012'
    WHEN 2015 THEN '2016'
    WHEN 2016 THEN '2016'
    ELSE NULL               -- filtered out
END
```

---

## Core Analytical Tables

### `partisan_spending_monthly`

The primary table for Q2 and Q3 analysis. Each row is one combination of:

| Column | Values |
|---|---|
| `Cycle` | 2004, 2008, 2012, 2016 |
| `era` | pre_CU, post_CU |
| `Year` | calendar year (string) |
| `Month` | MM (string) |
| `spending_type` | individual, pac_direct, pac_independent, 527 |
| `partisan_direction` | pro_R, pro_D, unaligned |
| `total_amount` | nominal dollars |
| `total_amount_2024` | inflation-adjusted 2024 dollars |
| `n_transactions` | count of underlying records |

### `partisan_spending_weekly`

Same structure but `YearWeek` (`YYYY-WW`) replaces `Year` + `Month`. Used for
the Granger causality / arms race analysis in Q3, where weekly resolution is
needed to detect lagged spending responses between parties.

---

## Validation Checks

The script runs a suite of checks at the end and logs PASS/FAIL for each:

| Check | What it verifies |
|---|---|
| `pres_candidates has rows` | At least 5 presidential candidate records |
| `All expected eras present` | Both `pre_CU` and `post_CU` are populated |
| Key candidate presence | G.W. Bush (2004), Obama (2008), Obama (2012) found |
| `No PAC-recipient rows in indivs_to_pres` | RecipID never starts with 'C' |
| `indivs_to_pres has only known eras` | No NULL or unexpected era values |
| `pacs_to_pres has only known eras` | Same |
| `indivs partisan coverage >= 90%` | At least 90% of rows are pro_R or pro_D |
| Partisan spending totals | Logged in $B for manual sanity check |
| `exp527_aligned has all 4 cycles` | 527 data covers all four election cycles |
| Monthly/weekly tables have rows | Basic non-empty check |

Validation failures are warnings, not hard stops — the pipeline completes and
the data can still be used, but failures should be investigated before drawing
conclusions from the analysis scripts.

---

## Key Bug Fixed (vs. original)

The original `create_pres_candidates()` used a plain `SELECT` without
deduplication. The raw `candidates` table contains duplicate `(Cycle, CID)`
rows (e.g. Kerry appears twice in 2004 with `DistIDRunFor = 'PRES'`). Without
deduplication, every JOIN in `indivs_to_pres` and `pacs_to_pres` would
fan out and double-count contributions for affected candidates.

**Fix:** Added `GROUP BY Cycle, CID` with `MAX()` aggregates on all other
columns. Since duplicate rows are identical, `MAX()` simply picks one value.
