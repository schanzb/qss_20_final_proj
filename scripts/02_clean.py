#!/usr/bin/env python3
"""
02_clean.py — Apply filters, inflation adjustment, and build analytical tables

Reads from: data/citizens_united.db  (must have been populated by 01_import.py)

Derived tables created:
  pres_candidates          — presidential candidates only, with era tag
  indivs_to_pres           — individual contributions to presidential candidates
  pacs_to_pres             — PAC contributions / IEs to presidential candidates
  exp527_aligned           — 527 expenditures with partisan alignment
  partisan_spending_monthly — monthly time series, all spending pro-R / pro-D
  partisan_spending_weekly  — weekly time series for counter-spending analysis

Filters applied throughout:
  • RealCode NOT LIKE 'Z9%'  (exclude non-contributions)
  • RealCode NOT LIKE 'Z4%'  (exclude joint fundraising transfers)
  • Type IN (...) for individual_contributions (valid FEC transaction types)
  • Anti-double-counting: individuals TO PACs excluded when joining with PAC data

Amounts are inflation-adjusted to constant 2024 dollars via cpi_factors table.
"""

import logging
import sqlite3
import sys
import time
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "citizens_united.db"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(SCRIPT_DIR / "clean.log", mode="w"),
    ],
)
log = logging.getLogger(__name__)


# ── Database connection ───────────────────────────────────────────────────────

def connect_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}")
        log.error("Run 01_import.py first.")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -2000000")
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


def run_sql(conn: sqlite3.Connection, label: str, sql: str) -> None:
    """Execute a SQL statement with timing and logging."""
    log.info(f"  {label} …")
    t0 = time.time()
    conn.executescript(sql) if ";" in sql.strip()[:-1] else conn.execute(sql)
    conn.commit()
    elapsed = time.time() - t0
    log.info(f"    done in {elapsed:.1f}s")


def drop_and_run(conn: sqlite3.Connection, table: str, create_sql: str) -> None:
    """Drop a derived table if it exists, then create it fresh."""
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    run_sql(conn, f"CREATE TABLE {table}", create_sql)


# ── Helper: partisan direction CASE expression ─────────────────────────────────
# Used for PAC contributions in pacs_to_pres.
# Direct contributions & IEs FOR a candidate → support that party.
# IEs AGAINST a candidate → support the OPPOSING party (reclassify).

PARTISAN_CASE_PACS = """
    CASE
        WHEN c.Party = 'R' AND p.Type IN ('24E','24C','24F','24K','24Z') THEN 'pro_R'
        WHEN c.Party = 'D' AND p.Type IN ('24E','24C','24F','24K','24Z') THEN 'pro_D'
        WHEN c.Party = 'D' AND p.Type IN ('24A','24N') THEN 'pro_R'
        WHEN c.Party = 'R' AND p.Type IN ('24A','24N') THEN 'pro_D'
        ELSE 'unaligned'
    END
"""

PARTISAN_CASE_INDIVS = """
    CASE c.Party
        WHEN 'R' THEN 'pro_R'
        WHEN 'D' THEN 'pro_D'
        ELSE 'unaligned'
    END
"""

# ── Date helpers ──────────────────────────────────────────────────────────────
# OpenSecrets dates are MM/DD/YYYY. SQLite needs YYYY-MM-DD for date functions.
# Convert inline with substr():
#   substr(Date, 7, 4) || '-' || substr(Date, 1, 2) || '-' || substr(Date, 4, 2)

ISO_DATE = "substr(Date, 7, 4) || '-' || substr(Date, 1, 2) || '-' || substr(Date, 4, 2)"
YEAR_EXPR = "substr(Date, 7, 4)"
MONTH_EXPR = "substr(Date, 1, 2)"
WEEK_EXPR  = f"strftime('%Y-%W', {ISO_DATE})"


# ── 527 cycle expression ──────────────────────────────────────────────────────
# QuarterYr format: Q[1-4][YY] (e.g. Q408 = Q4 2008, Q216 = Q2 2016).
# Extract 2-digit year: substr(QuarterYr, 3, 2)
# Map to election cycle: only keep 2003/2004 → '2004', etc.

CYCLE_FROM_QUARTER = """
    CASE CAST('20' || substr(QuarterYr, 3, 2) AS INTEGER)
        WHEN 2003 THEN '2004'
        WHEN 2004 THEN '2004'
        WHEN 2007 THEN '2008'
        WHEN 2008 THEN '2008'
        WHEN 2011 THEN '2012'
        WHEN 2012 THEN '2012'
        WHEN 2015 THEN '2016'
        WHEN 2016 THEN '2016'
        ELSE NULL
    END
"""

ERA_FROM_CYCLE = """
    CASE Cycle
        WHEN '2004' THEN 'pre_CU'
        WHEN '2008' THEN 'pre_CU'
        WHEN '2012' THEN 'post_CU'
        WHEN '2016' THEN 'post_CU'
    END
"""


# ── 1. pres_candidates ────────────────────────────────────────────────────────

def create_pres_candidates(conn: sqlite3.Connection) -> None:
    """
    Filter candidates to presidential races only, add era classification.

    DistIDRunFor = 'PRES' identifies presidential candidates.
    CycleCand = 'Y' confirms they were an active candidate in that cycle.
    """
    log.info("Creating pres_candidates …")
    drop_and_run(conn, "pres_candidates", """
        CREATE TABLE pres_candidates AS
        SELECT
            Cycle,
            CID,
            FECCanID,
            CRPName,
            Party,
            DistIDRunFor,
            CycleCand,
            RecipCode,
            CASE Cycle
                WHEN '2004' THEN 'pre_CU'
                WHEN '2008' THEN 'pre_CU'
                WHEN '2012' THEN 'post_CU'
                WHEN '2016' THEN 'post_CU'
            END AS era
        FROM candidates
        WHERE DistIDRunFor = 'PRES'
    """)

    n = conn.execute("SELECT COUNT(*) FROM pres_candidates").fetchone()[0]
    log.info(f"  {n} presidential candidate records across all cycles")

    # Log per cycle
    for row in conn.execute(
        "SELECT Cycle, Party, COUNT(*) FROM pres_candidates GROUP BY Cycle, Party ORDER BY Cycle, Party"
    ).fetchall():
        log.info(f"    {row[0]} Party={row[1]}: {row[2]} candidates")


# ── 2. indivs_to_pres ─────────────────────────────────────────────────────────

def create_indivs_to_pres(conn: sqlite3.Connection) -> None:
    """
    Individual contributions to presidential candidates.

    Filters:
      - RealCode NOT LIKE 'Z9%' — exclude non-contributions
      - RealCode NOT LIKE 'Z4%' — exclude joint fundraising transfers
      - Type IN valid FEC transaction codes for individual contributions
      - JOIN to pres_candidates ensures only presidential recipients
        (anti-double-counting: individuals who gave to PACs won't match
         on CID since PAC CMteIDs start with 'C', not 'N')

    Adds inflation-adjusted Amount_2024 and partisan_direction.
    """
    log.info("Creating indivs_to_pres …")
    drop_and_run(conn, "indivs_to_pres", f"""
        CREATE TABLE indivs_to_pres AS
        SELECT
            i.Cycle,
            c.era,
            i.FECTransID,
            i.ContribID,
            i.Contributor,
            i.RecipID,
            c.Party          AS RecipParty,
            c.CRPName        AS RecipName,
            i.RealCode,
            i.Date,
            CAST(NULLIF(i.Amount, '') AS REAL)                        AS Amount,
            CAST(NULLIF(i.Amount, '') AS REAL) * cf.factor            AS Amount_2024,
            i.City,
            i.State,
            i.RecipCode,
            i.Type,
            i.CmteID,
            i.Gender,
            i.Occupation,
            i.Employer,
            {PARTISAN_CASE_INDIVS.replace('c.Party', 'c.Party')}      AS partisan_direction
        FROM individual_contributions i
        INNER JOIN pres_candidates c
            ON i.RecipID = c.CID AND i.Cycle = c.Cycle
        LEFT JOIN cpi_factors cf
            ON i.Cycle = cf.Cycle
        WHERE
            (i.RealCode NOT LIKE 'Z9%' OR i.RealCode IS NULL)
            AND (i.RealCode NOT LIKE 'Z4%' OR i.RealCode IS NULL)
            AND i.Type IN ('10','11','15','15E','15J','22Y')
    """)

    n = conn.execute("SELECT COUNT(*) FROM indivs_to_pres").fetchone()[0]
    log.info(f"  {n:,} individual contribution records to presidential candidates")

    for row in conn.execute(
        "SELECT Cycle, COUNT(*), SUM(Amount_2024)/1e9 FROM indivs_to_pres GROUP BY Cycle ORDER BY Cycle"
    ).fetchall():
        log.info(f"    {row[0]}: {row[1]:>8,} records, ${row[2]:.2f}B (2024 $)")


# ── 3. pacs_to_pres ───────────────────────────────────────────────────────────

def create_pacs_to_pres(conn: sqlite3.Connection) -> None:
    """
    PAC contributions and independent expenditures to presidential candidates.

    Also checks pac_to_pac for PAC→candidate transfers where the recipient
    is a presidential candidate (CLAUDE.md note: FEC sometimes places these there).

    The DI field is the core Citizens United metric:
      'D' = Direct contribution (subject to limits — not affected by CU)
      'I' = Independent expenditure (unlimited post-CU — exploded after 2010)

    Partisan reclassification uses the Type field:
      24A / 24N (IE against a candidate) → reclassified as supporting the OPPONENT
    """
    log.info("Creating pacs_to_pres …")
    drop_and_run(conn, "pacs_to_pres", f"""
        CREATE TABLE pacs_to_pres AS

        -- Primary source: pacs_to_candidates
        SELECT
            p.Cycle,
            c.era,
            p.FECTransID,
            p.CommID,
            p.CandID,
            c.Party          AS RecipParty,
            c.CRPName        AS RecipName,
            p.PrimCode,
            p.Type,
            p.DI,
            p.Date,
            CAST(NULLIF(p.Amount, '') AS REAL)                AS Amount,
            CAST(NULLIF(p.Amount, '') AS REAL) * cf.factor    AS Amount_2024,
            {PARTISAN_CASE_PACS.strip()}                       AS partisan_direction,
            'pacs_to_candidates' AS source_table
        FROM pacs_to_candidates p
        INNER JOIN pres_candidates c
            ON p.CandID = c.CID AND p.Cycle = c.Cycle
        LEFT JOIN cpi_factors cf
            ON p.Cycle = cf.Cycle
        WHERE
            (p.PrimCode NOT LIKE 'Z9%' OR p.PrimCode IS NULL)
            AND (p.PrimCode NOT LIKE 'Z4%' OR p.PrimCode IS NULL)

        UNION ALL

        -- Secondary source: pac_to_pac (FEC sometimes places PAC→candidate here)
        SELECT
            po.Cycle,
            c.era,
            po.FECTransID,
            po.CommID,
            po.RecipCommID   AS CandID,
            c.Party          AS RecipParty,
            c.CRPName        AS RecipName,
            po.PrimCode,
            po.FECType       AS Type,
            NULL             AS DI,              -- DI not available in this table
            po.Date,
            CAST(NULLIF(po.Amount, '') AS REAL)                AS Amount,
            CAST(NULLIF(po.Amount, '') AS REAL) * cf.factor    AS Amount_2024,
            CASE c.Party
                WHEN 'R' THEN 'pro_R'
                WHEN 'D' THEN 'pro_D'
                ELSE 'unaligned'
            END              AS partisan_direction,
            'pac_to_pac'     AS source_table
        FROM pac_to_pac po
        INNER JOIN pres_candidates c
            ON po.RecipCommID = c.CID AND po.Cycle = c.Cycle
        LEFT JOIN cpi_factors cf
            ON po.Cycle = cf.Cycle
        WHERE
            (po.RealCode NOT LIKE 'Z9%' OR po.RealCode IS NULL)
            AND (po.RealCode NOT LIKE 'Z4%' OR po.RealCode IS NULL)
    """)

    n = conn.execute("SELECT COUNT(*) FROM pacs_to_pres").fetchone()[0]
    log.info(f"  {n:,} PAC contribution/IE records to presidential candidates")

    for row in conn.execute("""
        SELECT Cycle, DI, COUNT(*), SUM(Amount_2024)/1e9
        FROM pacs_to_pres
        WHERE DI IS NOT NULL
        GROUP BY Cycle, DI
        ORDER BY Cycle, DI
    """).fetchall():
        log.info(f"    {row[0]} DI={row[1]}: {row[2]:>8,} records, ${row[3]:.3f}B (2024 $)")


# ── 4. exp527_aligned ─────────────────────────────────────────────────────────

def create_exp527_aligned(conn: sqlite3.Connection) -> None:
    """
    527 expenditures joined with committee partisan alignment (ViewPt).

    ViewPt mapping:
      'C' (conservative) → pro_R
      'L' (liberal)      → pro_D
      'N' (none)         → unaligned
      'U' (unknown)      → unaligned

    Filters:
      - Ctype = 'F': federal-focus committees only (exclude state-level)
      - Cycle derived from QuarterYr (only our 4 election cycles)

    For EINs with multiple cmtes_527 rows (different quarters), uses the
    most recent year's ViewPt via a subquery.
    """
    log.info("Creating exp527_aligned …")

    # Build a deduped ViewPt lookup: one row per EIN using most recent Year
    conn.execute("DROP TABLE IF EXISTS _tmp_cmte527_latest")
    conn.execute("""
        CREATE TEMP TABLE _tmp_cmte527_latest AS
        SELECT c1.EIN, c1.ViewPt, c1.Ctype
        FROM cmtes_527 c1
        INNER JOIN (
            SELECT EIN, MAX(Year) AS MaxYear
            FROM cmtes_527
            GROUP BY EIN
        ) c2 ON c1.EIN = c2.EIN AND c1.Year = c2.MaxYear
        GROUP BY c1.EIN
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS _idx_tmp_cmte527 ON _tmp_cmte527_latest(EIN)")
    conn.commit()

    drop_and_run(conn, "exp527_aligned", f"""
        CREATE TABLE exp527_aligned AS
        SELECT
            e.QuarterYr,
            e.EIN,
            e.TransSeqNo,
            e.CMteName,
            e.PaidByEIN,
            e.PayeeLong     AS Payee,
            e.Amount,
            CAST(NULLIF(e.Amount, '') AS REAL)                AS Amount_real,
            e.Date,
            e.ExpCategoryCode,
            e.Description,
            e.City,
            e.State,
            cv.ViewPt,
            cv.Ctype,
            CASE cv.ViewPt
                WHEN 'C' THEN 'pro_R'
                WHEN 'L' THEN 'pro_D'
                ELSE 'unaligned'
            END                                               AS partisan_direction,
            ({CYCLE_FROM_QUARTER.strip()})                    AS Cycle,
            CASE ({CYCLE_FROM_QUARTER.strip()})
                WHEN '2004' THEN 'pre_CU'
                WHEN '2008' THEN 'pre_CU'
                WHEN '2012' THEN 'post_CU'
                WHEN '2016' THEN 'post_CU'
            END                                               AS era,
            CAST(NULLIF(e.Amount, '') AS REAL) * cf.factor    AS Amount_2024
        FROM expenditures_527 e
        LEFT JOIN _tmp_cmte527_latest cv ON e.EIN = cv.EIN
        LEFT JOIN cpi_factors cf ON ({CYCLE_FROM_QUARTER.strip()}) = cf.Cycle
        WHERE
            cv.Ctype = 'F'
            AND ({CYCLE_FROM_QUARTER.strip()}) IS NOT NULL
    """)

    # Clean up temp table
    conn.execute("DROP TABLE IF EXISTS _tmp_cmte527_latest")
    conn.commit()

    n = conn.execute("SELECT COUNT(*) FROM exp527_aligned").fetchone()[0]
    log.info(f"  {n:,} 527 expenditure records (federal, relevant cycles)")

    for row in conn.execute("""
        SELECT Cycle, partisan_direction, COUNT(*), SUM(Amount_2024)/1e9
        FROM exp527_aligned
        GROUP BY Cycle, partisan_direction
        ORDER BY Cycle, partisan_direction
    """).fetchall():
        log.info(f"    {row[0]} {row[1]:<12}: {row[2]:>8,} records, ${row[3]:.3f}B (2024 $)")


# ── 5. partisan_spending_monthly ──────────────────────────────────────────────

def create_partisan_monthly(conn: sqlite3.Connection) -> None:
    """
    Monthly time series of all presidential election spending.

    Columns: Cycle, era, Year, Month, spending_type, partisan_direction,
             total_amount, total_amount_2024, n_transactions

    spending_type values:
      'individual'      — direct donations from individuals to candidates
      'pac_direct'      — direct PAC-to-candidate contributions (DI='D')
      'pac_independent' — PAC independent expenditures (DI='I')
      '527'             — 527 organization expenditures

    This is the CORE analytical table for Q2 (partisan moderation)
    and Q3 (counter-spending arms race).
    """
    log.info("Creating partisan_spending_monthly …")
    drop_and_run(conn, "partisan_spending_monthly", f"""
        CREATE TABLE partisan_spending_monthly AS

        -- Individual contributions
        SELECT
            Cycle,
            era,
            {YEAR_EXPR}          AS Year,
            {MONTH_EXPR}         AS Month,
            'individual'         AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM indivs_to_pres
        WHERE Date IS NOT NULL AND Date != ''
        GROUP BY Cycle, era, Year, Month, spending_type, partisan_direction

        UNION ALL

        -- PAC direct contributions (DI = 'D')
        SELECT
            Cycle,
            era,
            {YEAR_EXPR}          AS Year,
            {MONTH_EXPR}         AS Month,
            'pac_direct'         AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM pacs_to_pres
        WHERE DI = 'D'
          AND Date IS NOT NULL AND Date != ''
        GROUP BY Cycle, era, Year, Month, spending_type, partisan_direction

        UNION ALL

        -- PAC independent expenditures (DI = 'I')
        SELECT
            Cycle,
            era,
            {YEAR_EXPR}          AS Year,
            {MONTH_EXPR}         AS Month,
            'pac_independent'    AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM pacs_to_pres
        WHERE DI = 'I'
          AND Date IS NOT NULL AND Date != ''
        GROUP BY Cycle, era, Year, Month, spending_type, partisan_direction

        UNION ALL

        -- 527 expenditures
        SELECT
            Cycle,
            era,
            substr(Date, 7, 4)   AS Year,
            substr(Date, 1, 2)   AS Month,
            '527'                AS spending_type,
            partisan_direction,
            SUM(Amount_real)     AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM exp527_aligned
        WHERE Date IS NOT NULL AND Date != ''
        GROUP BY Cycle, era, Year, Month, spending_type, partisan_direction
    """)

    n = conn.execute("SELECT COUNT(*) FROM partisan_spending_monthly").fetchone()[0]
    log.info(f"  {n:,} monthly spending aggregate rows")

    log.info("  Summary by era and partisan direction (2024 $B):")
    for row in conn.execute("""
        SELECT era, partisan_direction, SUM(total_amount_2024)/1e9
        FROM partisan_spending_monthly
        GROUP BY era, partisan_direction
        ORDER BY era, partisan_direction
    """).fetchall():
        log.info(f"    {row[0]:<10} {row[1]:<12}: ${row[2]:.2f}B")


# ── 6. partisan_spending_weekly ───────────────────────────────────────────────

def create_partisan_weekly(conn: sqlite3.Connection) -> None:
    """
    Weekly time series — same structure as monthly but at week granularity.

    YearWeek uses ISO year-week format (YYYY-WW) via strftime('%Y-%W', iso_date).

    Used for Q3 counter-spending / Granger causality analysis.
    The high temporal resolution allows detection of spending spikes
    and lagged responses between parties.
    """
    log.info("Creating partisan_spending_weekly …")
    drop_and_run(conn, "partisan_spending_weekly", f"""
        CREATE TABLE partisan_spending_weekly AS

        -- Individual contributions
        SELECT
            Cycle,
            era,
            {WEEK_EXPR}          AS YearWeek,
            'individual'         AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM indivs_to_pres
        WHERE Date IS NOT NULL AND Date != ''
          AND length(Date) = 10
        GROUP BY Cycle, era, YearWeek, spending_type, partisan_direction

        UNION ALL

        -- PAC direct contributions (DI = 'D')
        SELECT
            Cycle,
            era,
            {WEEK_EXPR}          AS YearWeek,
            'pac_direct'         AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM pacs_to_pres
        WHERE DI = 'D'
          AND Date IS NOT NULL AND Date != ''
          AND length(Date) = 10
        GROUP BY Cycle, era, YearWeek, spending_type, partisan_direction

        UNION ALL

        -- PAC independent expenditures (DI = 'I')
        SELECT
            Cycle,
            era,
            {WEEK_EXPR}          AS YearWeek,
            'pac_independent'    AS spending_type,
            partisan_direction,
            SUM(Amount)          AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM pacs_to_pres
        WHERE DI = 'I'
          AND Date IS NOT NULL AND Date != ''
          AND length(Date) = 10
        GROUP BY Cycle, era, YearWeek, spending_type, partisan_direction

        UNION ALL

        -- 527 expenditures
        SELECT
            Cycle,
            era,
            strftime('%Y-%W',
                substr(Date, 7, 4) || '-' ||
                substr(Date, 1, 2) || '-' ||
                substr(Date, 4, 2)
            )                    AS YearWeek,
            '527'                AS spending_type,
            partisan_direction,
            SUM(Amount_real)     AS total_amount,
            SUM(Amount_2024)     AS total_amount_2024,
            COUNT(*)             AS n_transactions
        FROM exp527_aligned
        WHERE Date IS NOT NULL AND Date != ''
          AND length(Date) = 10
        GROUP BY Cycle, era, YearWeek, spending_type, partisan_direction
    """)

    n = conn.execute("SELECT COUNT(*) FROM partisan_spending_weekly").fetchone()[0]
    log.info(f"  {n:,} weekly spending aggregate rows")


# ── 7. Derived indexes ────────────────────────────────────────────────────────

def create_derived_indexes(conn: sqlite3.Connection) -> None:
    """Add indexes on derived tables for fast analytical queries."""
    log.info("Creating derived table indexes …")

    indexes = [
        ("idx_pres_cands_cycle",     "pres_candidates(Cycle)"),
        ("idx_pres_cands_cid",       "pres_candidates(CID)"),
        ("idx_pres_cands_party",     "pres_candidates(Party)"),

        ("idx_indivs_pres_cycle",    "indivs_to_pres(Cycle)"),
        ("idx_indivs_pres_party",    "indivs_to_pres(RecipParty)"),
        ("idx_indivs_pres_dir",      "indivs_to_pres(partisan_direction)"),
        ("idx_indivs_pres_date",     "indivs_to_pres(Date)"),

        ("idx_pacs_pres_cycle",      "pacs_to_pres(Cycle)"),
        ("idx_pacs_pres_di",         "pacs_to_pres(DI)"),
        ("idx_pacs_pres_dir",        "pacs_to_pres(partisan_direction)"),
        ("idx_pacs_pres_type",       "pacs_to_pres(Type)"),
        ("idx_pacs_pres_date",       "pacs_to_pres(Date)"),

        ("idx_exp527_cycle",         "exp527_aligned(Cycle)"),
        ("idx_exp527_viewpt",        "exp527_aligned(ViewPt)"),
        ("idx_exp527_dir",           "exp527_aligned(partisan_direction)"),
        ("idx_exp527_date",          "exp527_aligned(Date)"),

        ("idx_monthly_cycle",        "partisan_spending_monthly(Cycle)"),
        ("idx_monthly_era",          "partisan_spending_monthly(era)"),
        ("idx_monthly_dir",          "partisan_spending_monthly(partisan_direction)"),
        ("idx_monthly_type",         "partisan_spending_monthly(spending_type)"),

        ("idx_weekly_cycle",         "partisan_spending_weekly(Cycle)"),
        ("idx_weekly_yearweek",      "partisan_spending_weekly(YearWeek)"),
        ("idx_weekly_dir",           "partisan_spending_weekly(partisan_direction)"),
    ]

    for name, definition in indexes:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {definition}")
    conn.commit()

    log.info(f"  {len(indexes)} derived indexes created")


# ── Validation suite ──────────────────────────────────────────────────────────

def validate(conn: sqlite3.Connection) -> None:
    """
    Run all validation checks; log warnings for any failures.
    These are sanity checks, not hard stops — the data can still be used.
    """
    log.info("=" * 60)
    log.info("VALIDATION — derived table checks")
    log.info("=" * 60)
    passed = 0
    failed = 0

    def check(label: str, query: str, expected, compare="=="):
        nonlocal passed, failed
        result = conn.execute(query).fetchone()[0]
        ok = (
            result == expected if compare == "==" else
            result >= expected if compare == ">=" else
            result <= expected if compare == "<=" else
            result > 0
        )
        status = "PASS" if ok else "FAIL ⚠"
        log.info(f"  [{status}] {label}: {result} (expected {compare} {expected})")
        if ok:
            passed += 1
        else:
            failed += 1

    # pres_candidates
    check("pres_candidates has rows",
          "SELECT COUNT(*) FROM pres_candidates", 5, ">=")
    check("All expected eras present",
          "SELECT COUNT(DISTINCT era) FROM pres_candidates", 2)

    # Check key candidates are present
    log.info("  Key candidate presence check:")
    key_candidates = [
        ("N00008072", "G.W. Bush",   "2004"),
        ("N00009638", "Obama",       "2008"),
        ("N00009638", "Obama",       "2012"),
    ]
    for cid, name, cycle in key_candidates:
        row = conn.execute(
            "SELECT COUNT(*) FROM pres_candidates WHERE CID = ? AND Cycle = ?",
            (cid, cycle)
        ).fetchone()[0]
        status = "FOUND" if row > 0 else "NOT FOUND ⚠"
        log.info(f"    {name} ({cycle}): {status}")

    # Anti-double-counting: no PAC receipients in indivs_to_pres
    # (CID for candidates starts with 'N'; PAC CMteIDs start with 'C' — JOIN handles this,
    #  but we verify that RecipID doesn't contain obvious PAC IDs)
    check("No PAC-recipient rows in indivs_to_pres",
          "SELECT COUNT(*) FROM indivs_to_pres WHERE RecipID LIKE 'C%'", 0)

    # Era classification: only pre_CU and post_CU
    check("indivs_to_pres has only known eras",
          "SELECT COUNT(*) FROM indivs_to_pres WHERE era NOT IN ('pre_CU','post_CU')", 0)
    check("pacs_to_pres has only known eras",
          "SELECT COUNT(*) FROM pacs_to_pres WHERE era NOT IN ('pre_CU','post_CU')", 0)

    # Partisan direction completeness
    check("indivs partisan coverage (pro_R + pro_D fraction)",
          "SELECT CAST(SUM(CASE WHEN partisan_direction IN ('pro_R','pro_D') THEN 1 ELSE 0 END) AS REAL) / COUNT(*) FROM indivs_to_pres",
          0.90, ">=")

    # Amount sanity: total IE spending post-CU should dwarf pre-CU
    log.info("  Partisan spending totals (2024 $B):")
    for row in conn.execute("""
        SELECT era, partisan_direction,
               ROUND(SUM(total_amount_2024)/1e9, 2) AS billions
        FROM partisan_spending_monthly
        GROUP BY era, partisan_direction
        ORDER BY era, partisan_direction
    """).fetchall():
        log.info(f"    {row[0]:<10} {row[1]:<12}: ${row[2]}B")

    # 527 data cycle coverage
    check("exp527_aligned has all 4 cycles",
          "SELECT COUNT(DISTINCT Cycle) FROM exp527_aligned", 4)

    # Monthly table sanity
    check("partisan_spending_monthly has rows", "SELECT COUNT(*) FROM partisan_spending_monthly", 1, ">=")
    check("partisan_spending_weekly has rows",  "SELECT COUNT(*) FROM partisan_spending_weekly",  1, ">=")

    log.info(f"\n  Results: {passed} passed, {failed} failed")
    log.info("=" * 60)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    t_start = time.time()
    log.info("=" * 60)
    log.info("Citizens United Cleaning Pipeline — 02_clean.py")
    log.info(f"Database: {DB_PATH}")
    log.info("=" * 60)

    conn = connect_db()

    # Verify raw tables exist
    raw_tables = ["candidates", "committees", "individual_contributions",
                  "pacs_to_candidates", "cpi_factors"]
    for tbl in raw_tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if n == 0:
            log.error(f"Table '{tbl}' is empty — run 01_import.py first and verify import succeeded.")
            sys.exit(1)

    log.info("All required raw tables present. Starting cleaning pipeline …\n")

    # ── Run in dependency order ───────────────────────────────────────────────
    create_pres_candidates(conn)
    log.info("")

    create_indivs_to_pres(conn)
    log.info("")

    create_pacs_to_pres(conn)
    log.info("")

    create_exp527_aligned(conn)
    log.info("")

    create_partisan_monthly(conn)
    log.info("")

    create_partisan_weekly(conn)
    log.info("")

    create_derived_indexes(conn)
    log.info("")

    validate(conn)

    conn.close()
    elapsed = (time.time() - t_start) / 60
    log.info(f"\nCleaning pipeline complete in {elapsed:.1f} minutes.")
    log.info("Next step: run  python scripts/03_q1_spending.py")


if __name__ == "__main__":
    main()
