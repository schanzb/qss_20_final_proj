# Database Schema — citizens_united.db

All columns are stored as `TEXT` unless noted. Numeric conversions happen at
query time. Dates are in `MM/DD/YYYY` format throughout.

---

## Table of Contents

**Raw tables (imported by 01_import.py)**
- [candidates](#candidates) — 21,619 rows
- [committees](#committees) — 51,596 rows
- [individual_contributions](#individual_contributions) — 27,801,931 rows
- [pacs_to_candidates](#pacs_to_candidates) — 1,463,938 rows
- [pac_to_pac](#pac_to_pac) — 494,296 rows
- [expenditures](#expenditures) — 38,714,246 rows
- [cmtes_527](#cmtes_527) — 15,149 rows
- [receipts_527](#receipts_527) — 4,715,026 rows
- [expenditures_527](#expenditures_527) — 3,999,783 rows
- [category_codes](#category_codes) — 492 rows
- [cpi_factors](#cpi_factors) — 4 rows

**Derived tables (built by 02_clean.py)**
- [pres_candidates](#pres_candidates) — 3,463 rows
- [indivs_to_pres](#indivs_to_pres) — 6,812,993 rows
- [pacs_to_pres](#pacs_to_pres) — 214,067 rows
- [exp527_aligned](#exp527_aligned) — 0 rows ⚠
- [partisan_spending_monthly](#partisan_spending_monthly) — 729 rows
- [partisan_spending_weekly](#partisan_spending_weekly) — 2,754 rows

---

## Coded Field Reference

These fields appear across multiple tables — values are the same everywhere.

### `Cycle`
Election cycle year: `'2004'`, `'2008'`, `'2012'`, `'2016'`

### `era`
Citizens United era tag (derived):
- `'pre_CU'` — cycles 2004, 2008 (before the 2010 ruling)
- `'post_CU'` — cycles 2012, 2016 (after the 2010 ruling)

### `Party`
- `D` — Democrat
- `R` — Republican
- `3` — Third party
- `I` — Independent
- `L` — Libertarian
- `U` — Unknown

### `DI` (**the Citizens United metric** — in `pacs_to_candidates`, `pacs_to_pres`)
- `D` — Direct contribution (subject to contribution limits; unchanged by CU)
- `I` — Independent expenditure (unlimited post-CU; this is what exploded after 2010)

### `Type` (in `pacs_to_candidates`, `pacs_to_pres`)
- `24K` — Direct contribution to candidate
- `24E` — Independent expenditure FOR candidate
- `24A` — Independent expenditure AGAINST candidate → reclassified as supporting opponent
- `24C` — Coordinated party expenditure FOR candidate
- `24F` — Communication cost FOR candidate
- `24N` — Communication cost AGAINST candidate → reclassified as supporting opponent
- `24Z` — In-kind contribution

### `Type` (in `individual_contributions`) — valid transaction codes kept
- `10`, `11`, `15`, `15E`, `15J`, `22Y`

### `RecipCode` (first character)
- `D` — Democrat candidate
- `R` — Republican candidate
- `3` — Third party candidate
- `P` — Traditional PAC
- `O` — Outside spending group / Super PAC (the post-CU vehicle)
- `U` — Unknown

### `RecipCode` (second character, candidates)
- `W` — Winner, `L` — Loser, `I` — Incumbent, `C` — Challenger, `O` — Open seat

### `ViewPt` (527 committee partisan alignment — in `cmtes_527`, `exp527_aligned`)
- `C` — Conservative → mapped to `pro_R`
- `L` — Liberal → mapped to `pro_D`
- `N` — None / nonpartisan
- `U` — Unknown

### `Ctype` (527 committee focus level)
- `F` — Federal (only these are kept in `exp527_aligned`)
- `S` — State
- `U` — Unknown

### `partisan_direction` (derived field)
- `'pro_R'` — spending that benefits Republicans
- `'pro_D'` — spending that benefits Democrats
- `'unaligned'` — nonpartisan or unknown

### `spending_type` (in monthly/weekly tables)
- `'individual'` — direct donations from individuals to presidential candidates
- `'pac_direct'` — PAC direct contributions (`DI = 'D'`)
- `'pac_independent'` — PAC independent expenditures (`DI = 'I'`)
- `'527'` — 527 organization expenditures

### `RealCode` / `PrimCode` — industry/ideology codes
5-character codes identifying donor industry or ideology. Join to
`category_codes` on `Catcode` for human-readable labels.
- Exclude `Z9%` — non-contributions (refunds, etc.)
- Exclude `Z4%` — joint fundraising transfers (double-counting risk)

---

## Raw Tables

### `candidates`
One row per candidate per election cycle. All federal candidates, not just presidential.

| Column | Description |
|---|---|
| `Cycle` | Election cycle: 2004, 2008, 2012, 2016 |
| `FECCanID` | FEC-assigned candidate ID (e.g. `H2TX01234`) |
| `CID` | OpenSecrets candidate ID (e.g. `N00000245`) — primary join key |
| `CRPName` | Candidate name as formatted by OpenSecrets |
| `Party` | Party affiliation — see Party codes above |
| `DistIDRunFor` | District running for: `'PRES'` for president, `'MAS2'` for MA Senate, etc. |
| `DistIDRunIn` | District currently holding |
| `CurrCand` | `Y` if currently a candidate |
| `CycleCand` | `Y` if candidate in this specific cycle |
| `CRPICO` | Incumbent/challenger/open seat code |
| `RecipCode` | 2-char entity classification — see RecipCode above |
| `NoPacs` | `Y` if candidate pledged to refuse PAC money |

**Filter for presidential candidates:** `WHERE DistIDRunFor = 'PRES'`

---

### `committees`
FEC-registered fundraising committees. One row per committee per cycle.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `CMteID` | FEC committee ID (e.g. `C00123456`) — starts with `C` |
| `CMteName` | Committee name |
| `Affiliate` | Affiliated organization name |
| `UltOrg` | Ultimate parent organization |
| `RecipID` | Candidate or entity this committee supports |
| `CMtePrimCode` | Committee's own industry/ideology code |
| `OtherID` | Additional cross-reference ID |
| `Party` | Party affiliation |
| `PrimCode` | Primary industry/ideology code |
| `Source` | Data source flag |
| `Sensitive` | Sensitive industry flag |
| `IsActBlue` | `Y` if this is an ActBlue bundling committee |
| `Extra` | Overflow field |

---

### `individual_contributions`
Itemized contributions from individuals to candidates/committees. The largest
table — 27.8M rows across all four cycles.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `FECTransID` | FEC transaction ID (7 chars in 2004/2008; 19 chars in 2012/2016) |
| `ContribID` | OpenSecrets contributor ID |
| `Contributor` | Donor name |
| `RecipID` | Recipient ID — candidate CID (`N…`) or committee ID (`C…`) |
| `Orgname` | Donor's organization name |
| `UltOrg` | Ultimate parent organization of donor |
| `RealCode` | Industry/ideology code — filter out `Z9%` and `Z4%` |
| `Date` | Transaction date `MM/DD/YYYY` |
| `Amount` | Dollar amount (nominal) |
| `Street` | Donor street address |
| `City` | Donor city |
| `State` | Donor state (2-letter) |
| `Zip` | Donor zip code |
| `RecipCode` | Recipient entity type — `P%` = PAC, `D`/`R` = candidate party |
| `Type` | FEC transaction type — keep `10,11,15,15E,15J,22Y` only |
| `CmteID` | Receiving committee FEC ID |
| `OtherID` | Secondary cross-reference |
| `Gender` | Donor gender (`M`/`F`/`U`) |
| `Microfilm` | FEC microfilm reference |
| `Occupation` | Donor occupation (split from `FECOccEmp` in 2012+) |
| `Employer` | Donor employer (2012+ only; blank for 2004/2008) |
| `Source` | Data source flag |

**Anti-double-counting note:** Individuals who give to PACs have `RecipID`
starting with `C`. When joining to `pres_candidates` on `RecipID = CID`, those
rows are automatically excluded because candidate CIDs start with `N`.

---

### `pacs_to_candidates`
PAC contributions and independent expenditures to candidates.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `FECTransID` | FEC transaction ID |
| `CommID` | Spending PAC's committee ID |
| `CandID` | Recipient candidate's OpenSecrets CID — join key to `candidates` |
| `Amount` | Dollar amount (nominal) |
| `Date` | Transaction date `MM/DD/YYYY` |
| `PrimCode` | Donor PAC industry/ideology code — filter out `Z9%`, `Z4%` |
| `Type` | Transaction type — `24K`, `24E`, `24A`, `24C`, `24F`, `24N`, `24Z` |
| `DI` | **Direct (`D`) or Independent (`I`)** — the Citizens United metric |
| `RecipCode` | Recipient entity classification |

---

### `pac_to_pac`
PAC-to-PAC transfers and inter-committee transactions. Also check here for
PAC→candidate records the FEC sometimes files in this table instead of
`pacs_to_candidates`.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `FECTransID` | FEC transaction ID |
| `CommID` | Sending committee ID |
| `CommName` | Sending committee name |
| `Payee` | Payee name |
| `PayeeCity` / `PayeeState` / `PayeeZip` | Payee location |
| `Extra1` | Overflow field |
| `PrimCode` | Industry/ideology code |
| `Date` | Transaction date |
| `Amount` | Dollar amount |
| `RecipCommID` | Receiving committee/candidate ID |
| `Party` | Party affiliation |
| `OtherCmteID` | Secondary committee reference |
| `RecipCmteType` | Type of receiving entity |
| `RealCode` | Industry code — filter `Z9%`, `Z4%` |
| `Extra2` | Overflow field |
| `Type` | Transaction type |
| `CmteClass` | Committee class |
| `Microfilm` | FEC microfilm reference |
| `FECType` | FEC-assigned transaction type |
| `PrimCode2` | Secondary industry code |
| `Source` | Data source flag |

---

### `expenditures`
FEC-reported disbursements by committees. Very large — 38.7M rows. Covers
all committee spending, not just to candidates.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `SeqNo` | Sequential record number |
| `TransID` | Transaction ID |
| `RecipID` | Recipient ID |
| `RecipCode` | Recipient entity classification |
| `CommName` | Spending committee name |
| `Payee` | Payee name |
| `PayeeCode` | Payee classification code |
| `Amount` | Dollar amount |
| `Date` | Transaction date `MM/DD/YYYY` |
| `City` / `State` / `Zip` | Payee location |
| `Addr1` | Payee address |
| `Extra1–5` | Overflow fields |
| `PrimCode` | Industry/ideology code |
| `ExpType` | Expenditure type |
| `Source` | Data source flag |

---

### `cmtes_527`
527 organization committee metadata from IRS Form 8872 filings.

| Column | Description |
|---|---|
| `Year` | Calendar year of filing |
| `QuarterYr` | Filing quarter: `Q[1-4][YY]` format (e.g. `Q408` = Q4 2008) |
| `EIN` | IRS Employer ID — primary key for 527 orgs, used to join to expenditures |
| `OrgName` | Organization full name |
| `ShortName` | Organization short name |
| `CMteName` | Committee name |
| `CMteType` | Committee type code |
| `Affiliate1/2/3` | Affiliated organizations |
| `Party` | Party affiliation |
| `PrimCode` | Industry/ideology code |
| `Source` | Data source |
| `FilingType` | IRS filing type |
| `Ctype` | Focus: `F`=federal, `S`=state, `U`=unknown — **filter to `F`** |
| `FilingInfo` | Filing metadata |
| `ViewPt` | **Partisan viewpoint: `C`=conservative, `L`=liberal, `N`=none, `U`=unknown** |
| `Extra` | Overflow |
| `State` | State of organization |

---

### `receipts_527`
Itemized receipts (donations received) by 527 organizations.

| Column | Description |
|---|---|
| `QuarterYr` | Filing quarter |
| `EIN` | Receiving 527 org's IRS ID |
| `FilingNo` | IRS filing number |
| `RecipEIN` | Recipient EIN (if transfer to another org) |
| `OrgNameShort` / `OrgNameLong` | Organization names |
| `Addr1` / `City` / `State` / `Zip` | Organization address |
| `Amount` | Dollar amount received |
| `Date` | Receipt date |
| `RecipID` | Recipient ID |
| `RecipName` | Recipient name |
| `RecipType` | Recipient type code |
| `SourceCode` | Source classification code |

---

### `expenditures_527`
Itemized expenditures by 527 organizations from IRS Form 8872B filings.

| Column | Description |
|---|---|
| `QuarterYr` | Filing quarter |
| `EIN` | Spending 527 org's IRS ID — join to `cmtes_527` for `ViewPt` |
| `TransSeqNo` | Transaction sequence number |
| `CMteName` | Committee name |
| `PaidByEIN` | EIN of entity actually paying (may differ from filer) |
| `PayeeShort` / `PayeeLong` | Payee names |
| `Amount` | Dollar amount |
| `Date` | Expenditure date `MM/DD/YYYY` |
| `ExpCategoryCode` | IRS expenditure category code |
| `Status` | Filing status |
| `Description` | Free-text expenditure description |
| `Addr1` / `Addr2` / `City` / `State` / `Zip` | Payee address |
| `RecipName` | Recipient name |
| `RecipTitle` | Recipient title/role |

---

### `category_codes`
OpenSecrets industry/ideology code lookup. Join on `Catcode` to decode
`RealCode`, `PrimCode` fields in other tables.

| Column | Description |
|---|---|
| `Catcode` | 5-character industry/ideology code (primary key) |
| `Catname` | Human-readable category name |
| `Catorder` | Sort order |
| `Industry` | Industry group name |
| `Sector` | Sector name |
| `SectorLong` | Full sector description |

---

### `cpi_factors`
CPI-U inflation adjustment multipliers to constant 2024 dollars.

| Column | Description |
|---|---|
| `Cycle` | Election cycle year (primary key) |
| `factor` | Multiply nominal amount by this to get 2024 dollars |

| Cycle | Factor |
|---|---|
| 2004 | 1.6653 |
| 2008 | 1.4611 |
| 2012 | 1.3607 |
| 2016 | 1.3018 |

---

## Derived Tables

Built by `02_clean.py`. These are the tables used directly by the analysis
scripts. All dollar amounts include both nominal (`Amount`) and
inflation-adjusted (`Amount_2024`) columns.

---

### `pres_candidates`
Presidential candidates only, deduplicated, with era tag. Built from
`candidates WHERE DistIDRunFor = 'PRES'`, grouped by `(Cycle, CID)` to
eliminate duplicate rows in the raw data.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `CID` | OpenSecrets candidate ID — join key |
| `FECCanID` | FEC candidate ID |
| `CRPName` | Candidate name |
| `Party` | `D`, `R`, etc. |
| `DistIDRunFor` | Always `'PRES'` in this table |
| `CycleCand` | `Y` if active candidate this cycle |
| `RecipCode` | Entity classification |
| `era` | `'pre_CU'` or `'post_CU'` |

---

### `indivs_to_pres`
Individual contributions to presidential candidates only. Anti-double-counting
is handled structurally: the JOIN on `RecipID = CID` excludes contributions to
PACs (their `RecipID` starts with `C`; candidate CIDs start with `N`).

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `era` | `pre_CU` or `post_CU` |
| `FECTransID` | FEC transaction ID |
| `ContribID` | Donor ID |
| `Contributor` | Donor name |
| `RecipID` | Candidate CID |
| `RecipParty` | Recipient's party (`D` or `R`) |
| `RecipName` | Candidate name |
| `RealCode` | Industry code (Z9/Z4 already excluded) |
| `Date` | Transaction date `MM/DD/YYYY` |
| `Amount` | Nominal dollar amount (REAL) |
| `Amount_2024` | Inflation-adjusted 2024 dollars |
| `City` / `State` | Donor location |
| `RecipCode` | Recipient entity classification |
| `Type` | FEC transaction type |
| `CmteID` | Receiving committee |
| `Gender` | Donor gender |
| `Occupation` / `Employer` | Donor employer info |
| `partisan_direction` | `pro_R`, `pro_D`, or `unaligned` |

---

### `pacs_to_pres`
PAC contributions and independent expenditures to presidential candidates.
Drawn from both `pacs_to_candidates` (primary) and `pac_to_pac` (secondary,
for misclassified PAC→candidate records). Includes partisan reclassification:
`24A`/`24N` (against a candidate) are flipped to support the opposing party.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `era` | `pre_CU` or `post_CU` |
| `FECTransID` | FEC transaction ID |
| `CommID` | Spending PAC committee ID |
| `CandID` | Recipient candidate CID |
| `RecipParty` | Recipient's party |
| `RecipName` | Candidate name |
| `PrimCode` | PAC industry code |
| `Type` | Transaction type (`24K`, `24E`, `24A`, etc.) |
| `DI` | **Direct (`D`) or Independent (`I`)** — the CU metric |
| `Date` | Transaction date |
| `Amount` | Nominal amount (REAL) |
| `Amount_2024` | Inflation-adjusted 2024 dollars |
| `partisan_direction` | `pro_R`, `pro_D`, or `unaligned` (with 24A/24N flipped) |
| `source_table` | `'pacs_to_candidates'` or `'pac_to_pac'` |

---

### `exp527_aligned`
⚠ **Currently 0 rows — needs investigation.** Should contain federal-focus
527 expenditures joined to `cmtes_527` for `ViewPt` partisan alignment.

Expected columns when populated:

| Column | Description |
|---|---|
| `QuarterYr` | Filing quarter |
| `EIN` | 527 org IRS ID |
| `TransSeqNo` | Transaction sequence number |
| `CMteName` | Committee name |
| `PaidByEIN` | Paying entity EIN |
| `Payee` | Payee name |
| `Amount` | Nominal amount (TEXT) |
| `Amount_real` | Nominal amount (REAL) |
| `Date` | Expenditure date |
| `ExpCategoryCode` | IRS category code |
| `Description` | Expenditure description |
| `City` / `State` | Payee location |
| `ViewPt` | `C`, `L`, `N`, or `U` |
| `Ctype` | Always `F` (federal) |
| `partisan_direction` | `pro_R`, `pro_D`, or `unaligned` |
| `Cycle` | Derived from `QuarterYr` |
| `era` | `pre_CU` or `post_CU` |
| `Amount_2024` | Inflation-adjusted 2024 dollars |

---

### `partisan_spending_monthly`
**Primary analytical table for Q2 and Q3.** One row per unique combination of
cycle + era + year + month + spending type + partisan direction. All spending
unified into a single time series across all four source types.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `era` | `pre_CU` or `post_CU` |
| `Year` | Calendar year (string, e.g. `'2016'`) |
| `Month` | Month `MM` (string, e.g. `'10'`) |
| `spending_type` | `individual`, `pac_direct`, `pac_independent`, `527` |
| `partisan_direction` | `pro_R`, `pro_D`, or `unaligned` |
| `total_amount` | Sum of nominal dollars |
| `total_amount_2024` | Sum of inflation-adjusted 2024 dollars |
| `n_transactions` | Count of underlying records |

**Example query — partisan gap by cycle:**
```sql
SELECT Cycle, partisan_direction, SUM(total_amount_2024)/1e9 AS billions
FROM partisan_spending_monthly
WHERE partisan_direction IN ('pro_R', 'pro_D')
GROUP BY Cycle, partisan_direction
ORDER BY Cycle, partisan_direction;
```

---

### `partisan_spending_weekly`
Same structure as `partisan_spending_monthly` but at weekly granularity for
Q3 counter-spending / Granger causality analysis.

| Column | Description |
|---|---|
| `Cycle` | Election cycle |
| `era` | `pre_CU` or `post_CU` |
| `YearWeek` | ISO year-week string `YYYY-WW` (e.g. `'2016-40'`) |
| `spending_type` | `individual`, `pac_direct`, `pac_independent`, `527` |
| `partisan_direction` | `pro_R`, `pro_D`, or `unaligned` |
| `total_amount` | Sum of nominal dollars |
| `total_amount_2024` | Sum of inflation-adjusted 2024 dollars |
| `n_transactions` | Count of underlying records |

---

## Key Joins

```
candidates ←——————————————————— pres_candidates (filtered, deduplicated)
    ↑ CID                              ↑ CID
    |                                  |
individual_contributions ——————→ indivs_to_pres
    RecipID = CID, Cycle = Cycle

pacs_to_candidates ————————————→ pacs_to_pres
    CandID = CID, Cycle = Cycle

pac_to_pac —————————————————————→ pacs_to_pres (secondary source)
    RecipCommID = CID, Cycle = Cycle

expenditures_527 ———————————————→ exp527_aligned
    EIN = EIN (joined to cmtes_527 for ViewPt)

committees ←——————— CMteID ———————— individual_contributions.CmteID

category_codes ←—— Catcode ————— RealCode / PrimCode (in most tables)

cpi_factors ←———— Cycle ————————— all tables with Amount (for Amount_2024)
```
