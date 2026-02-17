# 01_import.py — Documentation

## Purpose

Reads all raw OpenSecrets bulk data files and loads them into a single SQLite
database (`data/citizens_united.db`). This is the first step in the pipeline;
nothing else can run until this completes.

**Input:** Raw `.txt` files in `data/raw/`
**Output:** `data/citizens_united.db` with 11 populated tables and 28 indexes
**Runtime:** ~13 minutes on this system (dominated by `16expends.txt` at 5.5 GB)

---

## File Layout Expected

```
data/raw/
  campaign_finance/   04cands.txt, 04cmtes.txt, 04indivs.txt, 04pac_other.txt, 04pacs.txt
                      (same for 08, 12, 16)
  expend/             04expends.txt, 08expends.txt, 12expends.txt, 16expends.txt
  527/                cmtes527.txt, rcpts527.txt, expends527.txt
  reference/          CRP_Categories.txt, inflation.csv
```

---

## OpenSecrets Format

OpenSecrets bulk files are **not standard CSV**. Text fields are wrapped in
pipe characters (`|value|`) and the whole row is comma-separated:

```
|LASTNAME, FIRSTNAME|,|EMPLOYER|,20241101,5000,...
```

The script uses `csv.reader` with `quotechar='|'` so that pipes act as the
quote character. This correctly handles commas inside names/employer strings
without splitting them into extra fields. Each field is then `.strip()`-ed to
remove residual whitespace.

---

## Database Schema

All columns are stored as `TEXT` regardless of their logical type. SQLite
handles dynamic widths, which avoids schema mismatch issues between the
2004/2008 and 2012/2016 cycles (field lengths and layouts changed at the 2012
boundary). Numeric conversions happen at query time in `02_clean.py`.

### Tables created

| Table | Source | Notes |
|---|---|---|
| `candidates` | `{yy}cands.txt` | One row per candidate per cycle |
| `committees` | `{yy}cmtes.txt` | Committee metadata |
| `pacs_to_candidates` | `{yy}pacs.txt` | PAC → candidate contributions; `DI` field is key CU metric |
| `pac_to_pac` | `{yy}pac_other.txt` | PAC → PAC transfers; may contain PAC→candidate records too |
| `individual_contributions` | `{yy}indivs.txt` | Largest table (27.8M rows total) |
| `expenditures` | `{yy}expends.txt` | FEC disbursement records (38.7M rows total) |
| `cmtes_527` | `cmtes527.txt` | 527 committee metadata with `ViewPt` partisan alignment |
| `receipts_527` | `rcpts527.txt` | 527 receipts |
| `expenditures_527` | `expends527.txt` | 527 disbursements |
| `category_codes` | `CRP_Categories.txt` | Industry/ideology code lookup |
| `cpi_factors` | hardcoded + `inflation.csv` | CPI-U multipliers to 2024 dollars |

---

## Pseudocode / Logic Flow

```
main()
│
├── VERIFY all raw data directories exist, else exit
│
├── connect_db()
│   └── Open SQLite with WAL mode, 2GB page cache, 4GB mmap
│
├── load_checkpoint()           # Read import_checkpoint.json if it exists
│                               # Allows resuming after a crash mid-import
│
├── IF schema not done:
│   └── create_schema()         # DROP IF EXISTS + CREATE all 11 tables
│
├── IF cpi not done:
│   └── load_cpi_factors()      # Insert hardcoded factors; refresh from inflation.csv
│
├── IF categories not done:
│   └── load_categories()       # Parse CRP_Categories.txt (tab or comma delimited)
│
├── FOR each cycle in [04, 08, 12, 16]:
│   ├── import_candidates()     # ~4K–8K rows, fast
│   ├── import_committees()     # ~9K–18K rows, fast
│   ├── import_pacs()           # ~245K–520K rows
│   ├── import_pac_other()      # ~89K–157K rows
│   ├── import_indivs()         # 2.5M–17.8M rows — STREAMING
│   └── import_expends()        # 1.7M–27M rows — STREAMING (largest files)
│   (each step checkpointed on completion)
│
├── import_cmtes527()           # 15K rows
├── import_rcpts527()           # 4.7M rows — streaming
├── import_expends527()         # 4M rows — streaming
│
├── create_indexes()            # 28 indexes built AFTER bulk insert for speed
│
└── validate_import()           # Row counts per table/cycle; key candidate check
```

### Streaming / chunking

Files larger than ~500MB are streamed row-by-row through a generator
(`parse_rows`) rather than loaded into memory all at once. Rows are
accumulated into batches of 50,000 and inserted with `executemany`. A
`conn.commit()` is issued every 500,000 rows to prevent the WAL from growing
unboundedly.

### Encoding fallback

`open_file()` tries `utf-8` first, then `latin-1`, then `cp1252`. The first
encoding that doesn't raise a `UnicodeDecodeError` on a 1KB test read is used
for the whole file. `errors='replace'` ensures unprintable characters don't
crash the import.

### Checkpointing

After each file completes, `mark_done(state, key)` writes a JSON file
(`scripts/import_checkpoint.json`). On re-run, `is_done(state, key)` skips
already-completed steps. This allows recovery from mid-run crashes without
re-importing gigabytes of data.

---

## Performance Settings

| PRAGMA | Value | Effect |
|---|---|---|
| `journal_mode` | `WAL` | Allows concurrent reads during write; safer than DELETE mode |
| `synchronous` | `NORMAL` | Reduces fsync calls; safe with WAL |
| `cache_size` | `-2000000` (2 GB) | Keeps hot pages in memory |
| `mmap_size` | `4294967296` (4 GB) | Memory-maps the database file for faster reads |
| `temp_store` | `MEMORY` | Keeps temp tables in RAM |

Indexes are created **after** bulk insertion. Building an index incrementally
(one row at a time) is 10–100x slower than a single post-insert `CREATE INDEX`.

---

## Validation Output

At the end the script logs:
- Row counts per table, broken down by cycle
- A flag (`⚠`) on any table with unexpectedly low counts
- A check for 7 known presidential candidate CIDs

**Note:** The hardcoded CID for John Kerry (`N00004357`) in the validation
check is incorrect — his actual CID is `N00000245`. The "NOT FOUND" warning
for Kerry in the log is a false alarm; `02_clean.py` finds him correctly via
`DistIDRunFor = 'PRES'`.

---

## Row Counts (actual, this run)

| Table | 2004 | 2008 | 2012 | 2016 |
|---|---|---|---|---|
| candidates | 3,843 | 4,108 | 5,952 | 7,716 |
| committees | 9,322 | 10,017 | 14,445 | 17,812 |
| individual_contributions | 2,537,689 | 3,636,349 | 3,838,983 | 17,788,910 |
| pacs_to_candidates | 245,041 | 304,315 | 394,627 | 519,955 |
| pac_to_pac | 89,121 | 110,648 | 137,302 | 157,225 |
| expenditures | 1,730,695 | 2,907,092 | 7,065,109 | 27,011,350 |

| Table | Total rows |
|---|---|
| cmtes_527 | 15,149 |
| receipts_527 | 4,715,026 |
| expenditures_527 | 3,999,783 |
| category_codes | 492 |
| cpi_factors | 4 |
