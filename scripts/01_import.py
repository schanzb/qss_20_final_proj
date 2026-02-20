#!/usr/bin/env python3
"""
01_import.py — Import raw OpenSecrets bulk data into SQLite

Imports all data for election cycles 2004, 2008, 2012, 2020 from:
  data/raw/campaign_finance/ — candidates, committees, individuals, PACs, expenditures
  data/raw/expend/           — FEC expenditures
  data/raw/527/              — 527 organization data
  data/raw/reference/        — CPI factors, category codes

Output: data/citizens_united.db

OpenSecrets format: text fields wrapped in pipes (|value|), comma-separated.
Uses csv.reader with quotechar='|' to correctly handle commas inside quoted fields.

Developed with assistance by Claude Code.

Runtime: ~3–5 hours (dominated by 20indivs.txt and 20expends.txt streaming).
"""

import csv
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CF_DIR = RAW_DIR / "campaign_finance"
EXP_DIR = RAW_DIR / "expend"
DIR_527 = RAW_DIR / "527"
REF_DIR = RAW_DIR / "reference"
DB_PATH = DATA_DIR / "citizens_united.db"
CHECKPOINT_PATH = SCRIPT_DIR / "import_checkpoint.json"

# ── Constants ─────────────────────────────────────────────────────────────────
CYCLES = ["04", "08", "12", "20"]          # 2-digit year suffixes
CYCLE_MAP = {"04": "2004", "08": "2008", "12": "2012", "20": "2020"}

CHUNK_SIZE = 50_000       # rows per chunk for streaming
COMMIT_EVERY = 500_000    # rows between commits for large files

# Hardcoded CPI-U multipliers to 2024 dollars (verified against BLS CPI-U)
# Source: data/raw/reference/inflation.csv
CPI_FACTORS = {
    "2004": 1.6653,
    "2008": 1.4611,
    "2012": 1.3607,
    "2020": 1.2124,   # confirmed from inflation.csv: $100 in 2020 = $121.24 in 2024
}

# ── Column schemas ────────────────────────────────────────────────────────────
# All columns stored as TEXT; SQLite handles dynamic widths.
# Column positions match the OpenSecrets file layouts.

COLS_CANDIDATES = [
    "Cycle", "FECCanID", "CID", "CRPName", "Party",
    "DistIDRunFor", "DistIDRunIn", "CurrCand", "CycleCand",
    "CRPICO", "RecipCode", "NoPacs",
]  # 12 columns

COLS_COMMITTEES = [
    "Cycle", "CMteID", "CMteName", "Affiliate", "UltOrg",
    "RecipID", "CMtePrimCode", "OtherID", "Party", "PrimCode",
    "Source", "Sensitive", "IsActBlue", "Extra",
]  # 14 columns

COLS_INDIVS = [
    "Cycle", "FECTransID", "ContribID", "Contributor", "RecipID",
    "Orgname", "UltOrg", "RealCode", "Date", "Amount",
    "Street", "City", "State", "Zip", "RecipCode",
    "Type", "CmteID", "OtherID", "Gender", "Microfilm",
    "Occupation", "Employer", "Source",
]  # 23 columns

COLS_PACS = [
    "Cycle", "FECTransID", "CommID", "CandID", "Amount",
    "Date", "PrimCode", "Type", "DI", "RecipCode",
]  # 10 columns

COLS_PAC_OTHER = [
    "Cycle", "FECTransID", "CommID", "CommName",
    "Payee", "PayeeCity", "PayeeState", "PayeeZip", "Extra1",
    "PrimCode", "Date", "Amount", "RecipCommID", "Party",
    "OtherCmteID", "RecipCmteType", "RealCode", "Extra2",
    "Type", "CmteClass", "Microfilm", "FECType", "PrimCode2",
    "Source",
]  # 24 columns

COLS_EXPENDS = [
    "Cycle", "SeqNo", "TransID", "RecipID", "RecipCode",
    "CommName", "Payee", "PayeeCode", "Amount", "Date",
    "City", "State", "Zip", "Addr1", "Extra1",
    "Extra2", "Extra3", "Extra4", "PrimCode", "Extra5",
    "ExpType", "Source",
]  # 22 columns

COLS_CMTES_527 = [
    "Year", "QuarterYr", "EIN", "OrgName", "ShortName",
    "CMteName", "CMteType", "Affiliate1", "Affiliate2", "Affiliate3",
    "Party", "PrimCode", "Source", "FilingType", "Ctype",
    "FilingInfo", "ViewPt", "Extra", "State",
]  # 19 columns

COLS_RCPTS_527 = [
    "QuarterYr", "EIN", "FilingNo", "RecipEIN",
    "OrgNameShort", "OrgNameLong", "Addr1", "City", "State", "Zip",
    "Amount", "Date", "RecipID", "RecipName", "RecipType", "SourceCode",
]  # 16 columns (flexible — pad/truncate if file differs)

COLS_EXPENDS_527 = [
    "QuarterYr", "EIN", "TransSeqNo", "CMteName", "PaidByEIN",
    "PayeeShort", "PayeeLong", "Amount", "Date", "ExpCategoryCode",
    "Status", "Description", "Addr1", "Addr2", "City",
    "State", "Zip", "RecipName", "RecipTitle",
]  # 19 columns

COLS_CATEGORIES = [
    "Catcode", "Catname", "Catorder", "Industry", "Sector", "SectorLong",
]  # 6 columns

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "import.log", mode="w"),
    ],
)
log = logging.getLogger(__name__)


# ── Database setup ────────────────────────────────────────────────────────────

def connect_db() -> sqlite3.Connection:
    """Open the database with performance pragmas."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -2000000")   # 2 GB page cache
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA mmap_size = 4294967296")  # 4 GB mmap
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all raw import tables (drops and recreates if they exist)."""
    log.info("Creating database schema …")
    cur = conn.cursor()

    # Reference tables
    cur.executescript("""
    DROP TABLE IF EXISTS cpi_factors;
    CREATE TABLE cpi_factors (
        Cycle  TEXT PRIMARY KEY,
        factor REAL NOT NULL
    );

    DROP TABLE IF EXISTS category_codes;
    CREATE TABLE category_codes (
        Catcode    TEXT PRIMARY KEY,
        Catname    TEXT,
        Catorder   TEXT,
        Industry   TEXT,
        Sector     TEXT,
        SectorLong TEXT
    );
    """)

    # Campaign finance tables (4 cycles)
    cur.executescript("""
    DROP TABLE IF EXISTS candidates;
    CREATE TABLE candidates (
        Cycle TEXT, FECCanID TEXT, CID TEXT, CRPName TEXT, Party TEXT,
        DistIDRunFor TEXT, DistIDRunIn TEXT, CurrCand TEXT, CycleCand TEXT,
        CRPICO TEXT, RecipCode TEXT, NoPacs TEXT
    );

    DROP TABLE IF EXISTS committees;
    CREATE TABLE committees (
        Cycle TEXT, CMteID TEXT, CMteName TEXT, Affiliate TEXT, UltOrg TEXT,
        RecipID TEXT, CMtePrimCode TEXT, OtherID TEXT, Party TEXT, PrimCode TEXT,
        Source TEXT, Sensitive TEXT, IsActBlue TEXT, Extra TEXT
    );

    DROP TABLE IF EXISTS individual_contributions;
    CREATE TABLE individual_contributions (
        Cycle TEXT, FECTransID TEXT, ContribID TEXT, Contributor TEXT, RecipID TEXT,
        Orgname TEXT, UltOrg TEXT, RealCode TEXT, Date TEXT, Amount TEXT,
        Street TEXT, City TEXT, State TEXT, Zip TEXT, RecipCode TEXT,
        Type TEXT, CmteID TEXT, OtherID TEXT, Gender TEXT, Microfilm TEXT,
        Occupation TEXT, Employer TEXT, Source TEXT
    );

    DROP TABLE IF EXISTS pacs_to_candidates;
    CREATE TABLE pacs_to_candidates (
        Cycle TEXT, FECTransID TEXT, CommID TEXT, CandID TEXT, Amount TEXT,
        Date TEXT, PrimCode TEXT, Type TEXT, DI TEXT, RecipCode TEXT
    );

    DROP TABLE IF EXISTS pac_to_pac;
    CREATE TABLE pac_to_pac (
        Cycle TEXT, FECTransID TEXT, CommID TEXT, CommName TEXT,
        Payee TEXT, PayeeCity TEXT, PayeeState TEXT, PayeeZip TEXT, Extra1 TEXT,
        PrimCode TEXT, Date TEXT, Amount TEXT, RecipCommID TEXT, Party TEXT,
        OtherCmteID TEXT, RecipCmteType TEXT, RealCode TEXT, Extra2 TEXT,
        Type TEXT, CmteClass TEXT, Microfilm TEXT, FECType TEXT, PrimCode2 TEXT,
        Source TEXT
    );

    DROP TABLE IF EXISTS expenditures;
    CREATE TABLE expenditures (
        Cycle TEXT, SeqNo TEXT, TransID TEXT, RecipID TEXT, RecipCode TEXT,
        CommName TEXT, Payee TEXT, PayeeCode TEXT, Amount TEXT, Date TEXT,
        City TEXT, State TEXT, Zip TEXT, Addr1 TEXT, Extra1 TEXT,
        Extra2 TEXT, Extra3 TEXT, Extra4 TEXT, PrimCode TEXT, Extra5 TEXT,
        ExpType TEXT, Source TEXT
    );
    """)

    # 527 tables
    cur.executescript("""
    DROP TABLE IF EXISTS cmtes_527;
    CREATE TABLE cmtes_527 (
        Year TEXT, QuarterYr TEXT, EIN TEXT, OrgName TEXT, ShortName TEXT,
        CMteName TEXT, CMteType TEXT, Affiliate1 TEXT, Affiliate2 TEXT, Affiliate3 TEXT,
        Party TEXT, PrimCode TEXT, Source TEXT, FilingType TEXT, Ctype TEXT,
        FilingInfo TEXT, ViewPt TEXT, Extra TEXT, State TEXT
    );

    DROP TABLE IF EXISTS receipts_527;
    CREATE TABLE receipts_527 (
        QuarterYr TEXT, EIN TEXT, FilingNo TEXT, RecipEIN TEXT,
        OrgNameShort TEXT, OrgNameLong TEXT, Addr1 TEXT, City TEXT, State TEXT, Zip TEXT,
        Amount TEXT, Date TEXT, RecipID TEXT, RecipName TEXT, RecipType TEXT, SourceCode TEXT
    );

    DROP TABLE IF EXISTS expenditures_527;
    CREATE TABLE expenditures_527 (
        QuarterYr TEXT, EIN TEXT, TransSeqNo TEXT, CMteName TEXT, PaidByEIN TEXT,
        PayeeShort TEXT, PayeeLong TEXT, Amount TEXT, Date TEXT, ExpCategoryCode TEXT,
        Status TEXT, Description TEXT, Addr1 TEXT, Addr2 TEXT, City TEXT,
        State TEXT, Zip TEXT, RecipName TEXT, RecipTitle TEXT
    );
    """)

    conn.commit()
    log.info("Schema created.")


# ── File parsing utilities ────────────────────────────────────────────────────

def open_file(filepath: Path, encoding: str = "utf-8"):
    """Open a file with encoding fallback: utf-8 → latin-1 → cp1252."""
    for enc in [encoding, "latin-1", "cp1252"]:
        try:
            f = open(filepath, "r", encoding=enc, errors="replace", newline="")
            # Quick read test
            f.read(1024)
            f.seek(0)
            return f, enc
        except (UnicodeDecodeError, LookupError):
            continue
    raise IOError(f"Cannot open {filepath} with any supported encoding")


def parse_rows(filepath: Path, expected_cols: int, encoding: str = "utf-8"):
    """
    Stream an OpenSecrets pipe-delimited file row by row.

    Uses csv.reader with quotechar='|' so that pipe-wrapped fields
    (including those containing commas, e.g. "LASTNAME, FIRSTNAME")
    are parsed as single fields rather than split on the internal comma.

    Rows are padded or truncated to expected_cols.
    """
    f, enc = open_file(filepath, encoding)
    if enc != encoding:
        log.warning(f"  Opened {filepath.name} with fallback encoding {enc}")

    reader = csv.reader(f, delimiter=",", quotechar="|", skipinitialspace=True)
    row_count = 0
    error_count = 0

    try:
        for raw_row in reader:
            # Strip residual whitespace from each field
            row = [field.strip() for field in raw_row]

            # Normalize column count
            if len(row) < expected_cols:
                row.extend([""] * (expected_cols - len(row)))
            elif len(row) > expected_cols:
                row = row[:expected_cols]

            yield row
            row_count += 1

    except csv.Error as e:
        error_count += 1
        log.warning(f"  CSV error at row {row_count}: {e}")
    finally:
        f.close()

    log.info(f"  → {row_count:>10,} rows  ({error_count} errors)  [{filepath.name}]")


def bulk_insert(conn: sqlite3.Connection, table: str, cols: list[str],
                rows_iter, commit_every: int = COMMIT_EVERY) -> int:
    """
    Insert rows from an iterator into `table` in large batches.

    Commits every `commit_every` rows to balance memory and durability.
    Returns total rows inserted.
    """
    placeholders = ", ".join(["?"] * len(cols))
    sql = f"INSERT INTO {table} VALUES ({placeholders})"
    cur = conn.cursor()
    total = 0
    batch = []

    for row in rows_iter:
        batch.append(row)
        total += 1
        if len(batch) >= CHUNK_SIZE:
            cur.executemany(sql, batch)
            batch.clear()
            if total % commit_every == 0:
                conn.commit()
                log.info(f"    … {total:,} rows committed to {table}")

    if batch:
        cur.executemany(sql, batch)
    conn.commit()
    return total


# ── Reference data ────────────────────────────────────────────────────────────

def load_cpi_factors(conn: sqlite3.Connection) -> None:
    """Insert hardcoded CPI-U adjustment factors (to 2024 dollars)."""
    log.info("Loading CPI factors …")
    rows = [(cycle, factor) for cycle, factor in CPI_FACTORS.items()]
    conn.executemany("INSERT OR REPLACE INTO cpi_factors VALUES (?, ?)", rows)
    conn.commit()

    # Optionally try to refresh from the CSV (handles the known typo on 2016 row)
    csv_path = REF_DIR / "inflation.csv"
    if csv_path.exists():
        try:
            with open(csv_path) as f:
                for line in f:
                    parts = [p.strip() for p in line.replace(" ", ",").split(",") if p.strip()]
                    if len(parts) >= 3 and parts[0].isdigit():
                        year = parts[0]
                        # Third numeric value is "$ 2024 equivalent of $100 in year"
                        numerics = [p for p in parts if _is_number(p)]
                        if len(numerics) >= 2:
                            factor = float(numerics[-1]) / 100.0
                            if year in CPI_FACTORS:
                                conn.execute(
                                    "INSERT OR REPLACE INTO cpi_factors VALUES (?, ?)",
                                    (year, factor),
                                )
            conn.commit()
            log.info("  CPI factors refreshed from inflation.csv")
        except Exception as e:
            log.warning(f"  Could not parse inflation.csv ({e}); using hardcoded values")

    log.info("  CPI factors loaded:")
    for row in conn.execute("SELECT Cycle, factor FROM cpi_factors ORDER BY Cycle"):
        log.info(f"    {row[0]}: ×{row[1]:.4f}")


def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def load_categories(conn: sqlite3.Connection) -> None:
    """Import CRP industry/ideology category codes."""
    cat_path = REF_DIR / "CRP_Categories.txt"
    if not cat_path.exists():
        log.warning(f"CRP_Categories.txt not found at {cat_path}; skipping")
        return

    log.info("Loading category codes …")
    rows = []

    # File may use tab or pipe delimiter; try both
    with open(cat_path, "r", encoding="latin-1", errors="replace") as f:
        first_line = f.readline()
    delimiter = "\t" if "\t" in first_line else ","

    with open(cat_path, "r", encoding="latin-1", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header_skipped = False
        for row in reader:
            row = [field.strip().strip("|").strip() for field in row]
            if not header_skipped:
                header_skipped = True
                if row and not row[0][0].isalpha() or row[0].lower() in ("catcode", "cat"):
                    continue  # skip header line
            if len(row) >= 6:
                rows.append(row[:6])
            elif len(row) >= 1 and row[0]:
                padded = row + [""] * (6 - len(row))
                rows.append(padded[:6])

    conn.executemany(
        "INSERT OR IGNORE INTO category_codes VALUES (?, ?, ?, ?, ?, ?)", rows
    )
    conn.commit()
    log.info(f"  {len(rows):,} category codes loaded")


# ── Campaign finance importers ────────────────────────────────────────────────

def import_candidates(conn: sqlite3.Connection, yy: str) -> int:
    """Import candidates file for a 2-digit cycle (e.g. '04' → 2004)."""
    path = CF_DIR / f"{yy}cands.txt"
    log.info(f"Importing candidates [{CYCLE_MAP[yy]}] from {path.name} …")
    rows = parse_rows(path, len(COLS_CANDIDATES))
    n = bulk_insert(conn, "candidates", COLS_CANDIDATES, rows)
    log.info(f"  → {n:,} candidate records")
    return n


def import_committees(conn: sqlite3.Connection, yy: str) -> int:
    """Import committee metadata for a cycle."""
    path = CF_DIR / f"{yy}cmtes.txt"
    log.info(f"Importing committees [{CYCLE_MAP[yy]}] from {path.name} …")
    rows = parse_rows(path, len(COLS_COMMITTEES))
    n = bulk_insert(conn, "committees", COLS_COMMITTEES, rows)
    log.info(f"  → {n:,} committee records")
    return n


def import_pacs(conn: sqlite3.Connection, yy: str) -> int:
    """Import PAC-to-candidate contributions for a cycle."""
    path = CF_DIR / f"{yy}pacs.txt"
    log.info(f"Importing pacs_to_candidates [{CYCLE_MAP[yy]}] from {path.name} …")
    rows = parse_rows(path, len(COLS_PACS))
    n = bulk_insert(conn, "pacs_to_candidates", COLS_PACS, rows)
    log.info(f"  → {n:,} PAC contribution records")
    return n


def import_pac_other(conn: sqlite3.Connection, yy: str) -> int:
    """Import PAC-to-PAC transfer records for a cycle."""
    path = CF_DIR / f"{yy}pac_other.txt"
    log.info(f"Importing pac_to_pac [{CYCLE_MAP[yy]}] from {path.name} …")
    rows = parse_rows(path, len(COLS_PAC_OTHER))
    n = bulk_insert(conn, "pac_to_pac", COLS_PAC_OTHER, rows)
    log.info(f"  → {n:,} PAC-to-PAC records")
    return n


def import_indivs(conn: sqlite3.Connection, yy: str) -> int:
    """
    Import individual contribution records (streaming for large files).

    16indivs.txt is 3.9 GB / ~17.8M rows — the largest file in the dataset.
    """
    path = CF_DIR / f"{yy}indivs.txt"
    log.info(f"Importing individual_contributions [{CYCLE_MAP[yy]}] from {path.name} …")
    log.info(f"  File size: {path.stat().st_size / 1e9:.1f} GB — streaming in {CHUNK_SIZE:,}-row chunks")

    rows = parse_rows(path, len(COLS_INDIVS))
    n = bulk_insert(conn, "individual_contributions", COLS_INDIVS, rows,
                    commit_every=COMMIT_EVERY)
    log.info(f"  → {n:,} individual contribution records")
    return n


def import_expends(conn: sqlite3.Connection, yy: str) -> int:
    """
    Import FEC expenditure records from data/raw/expend/ (streaming).

    16expends.txt is 5.1 GB — the very largest file.
    """
    path = EXP_DIR / f"{yy}expends.txt"
    log.info(f"Importing expenditures [{CYCLE_MAP[yy]}] from {path.name} …")
    log.info(f"  File size: {path.stat().st_size / 1e9:.1f} GB — streaming")

    rows = parse_rows(path, len(COLS_EXPENDS))
    n = bulk_insert(conn, "expenditures", COLS_EXPENDS, rows,
                    commit_every=COMMIT_EVERY)
    log.info(f"  → {n:,} expenditure records")
    return n


# ── 527 importers ─────────────────────────────────────────────────────────────

def import_cmtes527(conn: sqlite3.Connection) -> int:
    """Import 527 committee metadata (all years in one file)."""
    path = DIR_527 / "cmtes527.txt"
    log.info(f"Importing cmtes_527 from {path.name} …")
    rows = parse_rows(path, len(COLS_CMTES_527))
    n = bulk_insert(conn, "cmtes_527", COLS_CMTES_527, rows)
    log.info(f"  → {n:,} 527 committee records")
    return n


def import_rcpts527(conn: sqlite3.Connection) -> int:
    """Import 527 receipts (streaming, ~1 GB)."""
    path = DIR_527 / "rcpts527.txt"
    log.info(f"Importing receipts_527 from {path.name} …")
    log.info(f"  File size: {path.stat().st_size / 1e9:.1f} GB — streaming")
    rows = parse_rows(path, len(COLS_RCPTS_527))
    n = bulk_insert(conn, "receipts_527", COLS_RCPTS_527, rows,
                    commit_every=COMMIT_EVERY)
    log.info(f"  → {n:,} 527 receipt records")
    return n


def import_expends527(conn: sqlite3.Connection) -> int:
    """Import 527 expenditure records (streaming, ~870 MB)."""
    path = DIR_527 / "expends527.txt"
    log.info(f"Importing expenditures_527 from {path.name} …")
    log.info(f"  File size: {path.stat().st_size / 1e9:.1f} GB — streaming")
    rows = parse_rows(path, len(COLS_EXPENDS_527))
    n = bulk_insert(conn, "expenditures_527", COLS_EXPENDS_527, rows,
                    commit_every=COMMIT_EVERY)
    log.info(f"  → {n:,} 527 expenditure records")
    return n


# ── Indexes ───────────────────────────────────────────────────────────────────

def create_indexes(conn: sqlite3.Connection) -> None:
    """
    Build performance indexes on columns used in JOINs and WHERE clauses.
    Created AFTER bulk insert for maximum speed (avoids per-row index updates).
    """
    log.info("Creating indexes (this may take a few minutes) …")

    indexes = [
        # candidates
        ("idx_cands_cycle",        "candidates(Cycle)"),
        ("idx_cands_cid",          "candidates(CID)"),
        ("idx_cands_distid",       "candidates(DistIDRunFor)"),
        ("idx_cands_cycle_cid",    "candidates(Cycle, CID)"),

        # committees
        ("idx_cmtes_cycle",        "committees(Cycle)"),
        ("idx_cmtes_cmteid",       "committees(CMteID)"),
        ("idx_cmtes_primcode",     "committees(PrimCode)"),

        # individual_contributions  (largest table — most important indexes)
        ("idx_indivs_cycle",       "individual_contributions(Cycle)"),
        ("idx_indivs_recipid",     "individual_contributions(RecipID)"),
        ("idx_indivs_realcode",    "individual_contributions(RealCode)"),
        ("idx_indivs_type",        "individual_contributions(Type)"),
        ("idx_indivs_date",        "individual_contributions(Date)"),
        ("idx_indivs_cycle_recip", "individual_contributions(Cycle, RecipID)"),

        # pacs_to_candidates
        ("idx_pacs_cycle",         "pacs_to_candidates(Cycle)"),
        ("idx_pacs_candid",        "pacs_to_candidates(CandID)"),
        ("idx_pacs_di",            "pacs_to_candidates(DI)"),
        ("idx_pacs_type",          "pacs_to_candidates(Type)"),
        ("idx_pacs_cycle_cand",    "pacs_to_candidates(Cycle, CandID)"),

        # pac_to_pac
        ("idx_pacother_cycle",     "pac_to_pac(Cycle)"),
        ("idx_pacother_recipcomm", "pac_to_pac(RecipCommID)"),

        # expenditures
        ("idx_expends_cycle",      "expenditures(Cycle)"),
        ("idx_expends_recipid",    "expenditures(RecipID)"),

        # cmtes_527
        ("idx_cmte527_ein",        "cmtes_527(EIN)"),
        ("idx_cmte527_viewpt",     "cmtes_527(ViewPt)"),
        ("idx_cmte527_ctype",      "cmtes_527(Ctype)"),

        # expenditures_527
        ("idx_exp527_ein",         "expenditures_527(EIN)"),
        ("idx_exp527_quarter",     "expenditures_527(QuarterYr)"),
        ("idx_exp527_date",        "expenditures_527(Date)"),
    ]

    for name, definition in indexes:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {definition}")
        conn.commit()

    log.info(f"  {len(indexes)} indexes created.")


# ── Validation ────────────────────────────────────────────────────────────────

def validate_import(conn: sqlite3.Connection) -> None:
    """Log row counts for all imported tables; warn on unexpected zeros."""
    log.info("=" * 60)
    log.info("VALIDATION — row counts by table and cycle")
    log.info("=" * 60)

    tables_with_cycle = [
        "candidates",
        "committees",
        "individual_contributions",
        "pacs_to_candidates",
        "pac_to_pac",
        "expenditures",
    ]

    for table in tables_with_cycle:
        try:
            rows = conn.execute(
                f"SELECT Cycle, COUNT(*) as n FROM {table} GROUP BY Cycle ORDER BY Cycle"
            ).fetchall()
            log.info(f"\n  {table}:")
            for cycle, n in rows:
                flag = " ⚠" if n < 100 else ""
                log.info(f"    {cycle}: {n:>12,}{flag}")
        except sqlite3.OperationalError as e:
            log.warning(f"  Could not query {table}: {e}")

    single_tables = {
        "cpi_factors":       4,
        "category_codes":    200,
        "cmtes_527":         1_000,
        "receipts_527":      100_000,
        "expenditures_527":  100_000,
    }

    log.info("")
    for table, min_expected in single_tables.items():
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            flag = " ⚠ UNEXPECTEDLY LOW" if n < min_expected else ""
            log.info(f"  {table:<30}: {n:>10,}{flag}")
        except sqlite3.OperationalError as e:
            log.warning(f"  Could not query {table}: {e}")

    # Quick sanity check: presidential candidates should include key names
    log.info("\n  Presidential candidate check (CID presence):")
    known_pres = {
        "N00008072": "George W. Bush",
        "N00004357": "John Kerry",
        "N00009638": "Barack Obama",
        "N00006424": "John McCain",
        "N00000286": "Mitt Romney",
        "N00001669": "Joe Biden",          # 2020
        "N00023864": "Donald Trump",       # 2020 (ran again)
    }
    for cid, name in known_pres.items():
        row = conn.execute(
            "SELECT Cycle FROM candidates WHERE CID = ? AND DistIDRunFor = 'PRES'",
            (cid,),
        ).fetchall()
        cycles = [r[0] for r in row]
        status = f"found in cycles {cycles}" if cycles else "NOT FOUND ⚠"
        log.info(f"    {name:<25} ({cid}): {status}")

    log.info("=" * 60)


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    return {}


def save_checkpoint(state: dict) -> None:
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(state, f, indent=2)


def is_done(state: dict, key: str) -> bool:
    return state.get(key) == "done"


def mark_done(state: dict, key: str) -> None:
    state[key] = "done"
    save_checkpoint(state)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main() -> None:
    t_start = time.time()
    log.info("=" * 60)
    log.info("Citizens United Import Pipeline — 01_import.py")
    log.info(f"Database: {DB_PATH}")
    log.info("=" * 60)

    # Verify raw data directories exist
    for d in [CF_DIR, EXP_DIR, DIR_527, REF_DIR]:
        if not d.exists():
            log.error(f"Required directory not found: {d}")
            sys.exit(1)

    conn = connect_db()
    state = load_checkpoint()

    # ── Schema ──────────────────────────────────────────────────────────────
    if not is_done(state, "schema"):
        create_schema(conn)
        mark_done(state, "schema")

    # ── Reference data ───────────────────────────────────────────────────────
    if not is_done(state, "cpi"):
        load_cpi_factors(conn)
        mark_done(state, "cpi")

    if not is_done(state, "categories"):
        load_categories(conn)
        mark_done(state, "categories")

    # ── Campaign finance — per cycle ─────────────────────────────────────────
    for yy in CYCLES:
        cycle = CYCLE_MAP[yy]

        if not is_done(state, f"candidates_{yy}"):
            import_candidates(conn, yy)
            mark_done(state, f"candidates_{yy}")

        if not is_done(state, f"committees_{yy}"):
            import_committees(conn, yy)
            mark_done(state, f"committees_{yy}")

        if not is_done(state, f"pacs_{yy}"):
            import_pacs(conn, yy)
            mark_done(state, f"pacs_{yy}")

        if not is_done(state, f"pac_other_{yy}"):
            import_pac_other(conn, yy)
            mark_done(state, f"pac_other_{yy}")

        # ── Large streaming files ────────────────────────────────────────────
        if not is_done(state, f"indivs_{yy}"):
            t0 = time.time()
            import_indivs(conn, yy)
            elapsed = time.time() - t0
            log.info(f"  indivs [{cycle}] completed in {elapsed/60:.1f} min")
            mark_done(state, f"indivs_{yy}")

        if not is_done(state, f"expends_{yy}"):
            t0 = time.time()
            import_expends(conn, yy)
            elapsed = time.time() - t0
            log.info(f"  expends [{cycle}] completed in {elapsed/60:.1f} min")
            mark_done(state, f"expends_{yy}")

    # ── 527 data ─────────────────────────────────────────────────────────────
    if not is_done(state, "cmtes527"):
        import_cmtes527(conn)
        mark_done(state, "cmtes527")

    if not is_done(state, "rcpts527"):
        t0 = time.time()
        import_rcpts527(conn)
        log.info(f"  rcpts527 completed in {(time.time()-t0)/60:.1f} min")
        mark_done(state, "rcpts527")

    if not is_done(state, "expends527"):
        t0 = time.time()
        import_expends527(conn)
        log.info(f"  expends527 completed in {(time.time()-t0)/60:.1f} min")
        mark_done(state, "expends527")

    # ── Indexes ───────────────────────────────────────────────────────────────
    if not is_done(state, "indexes"):
        create_indexes(conn)
        mark_done(state, "indexes")

    # ── Validation ────────────────────────────────────────────────────────────
    validate_import(conn)

    conn.close()
    total_min = (time.time() - t_start) / 60
    log.info(f"\nImport pipeline complete in {total_min:.1f} minutes.")
    log.info(f"Checkpoint saved to {CHECKPOINT_PATH}")
    log.info("Next step: run  python scripts/02_clean.py")


if __name__ == "__main__":
    main()
