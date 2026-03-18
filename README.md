### Final Project — Money, and the State

This project analyzes how *Citizens United v. FEC* (2010) transformed presidential election spending. The central question is whether the explosion of Super PAC and dark money spending after CU represented genuinely new money entering politics, or a reshuffling of outside spending that was already happening through 527 organizations. We look at four presidential cycles — 2004, 2008, 2012, and 2020 — spanning the pre- and post-CU eras.

---

## Getting Started

### Prerequisites

- Python 3.8+
- pip
- Jupyter (for the analysis notebooks)

### Installation

```bash
git clone <repo-url>
cd qss_20_final_proj
pip install -r requirements.txt
```

### Data Setup

The raw data files and SQLite database are too large to track with git, so you'll need to download them separately. There are two options depending on how much you want to reproduce:

**Option A: Download the pre-built database (faster)**
Download the SQLite database [here](https://drive.google.com/file/d/1g9gIubmGwO2Fs-B9h06QGxR7-zqXlevt/view?usp=sharing) and place it at `data/campaign_finance.db`. Then skip straight to running the notebooks.

**Option B: Download raw files and run the full pipeline**
Download the raw data archive [here](https://drive.google.com/file/d/17PC6LPCypeEOMBU7gHM3LcGgfPcWXeXH/view?usp=sharing), unzip it, and place the files in `data/raw/`. Then run the pipeline scripts below.

### Running the Pipeline (Option B only)

```bash
python scripts/01_import.py   # ~13 min, loads raw CSV files into SQLite
python scripts/02_clean.py    # ~5–15 min, builds analysis tables
```

Run these in order. The import script creates `data/campaign_finance.db`; the clean script adds the derived tables that the notebooks read from.

### Running the Analysis Notebooks

```bash
jupyter notebook
```

Then open any of:
- `scripts/03_q1.ipynb` —> outside spending trends across cycles
- `scripts/04_q2.ipynb` —> donor composition shifts
- `scripts/05_q3.ipynb` —> partisan spending patterns
- `scripts/06_sankey.ipynb` —> flow diagram of spending by type

Each notebook is independent, where they all read directly from the SQLite database, so you can run them in any order.

### Outputs

- `figures/` —> PDF and HTML charts produced by the notebooks
- `tables/` —> CSV summary tables

---

## Cycle Windows

Both data sources are filtered to the same window per cycle: **Jan 1 of the odd year through presidential election day**.

| Cycle | Window | Era |
|---|---|---|
| 2004 | 2003-01-01 – 2004-11-02 | pre-Citizens United |
| 2008 | 2007-01-01 – 2008-11-04 | pre-Citizens United |
| 2012 | 2011-01-01 – 2012-11-06 | post-Citizens United |
| 2020 | 2019-01-01 – 2020-11-03 | post-Citizens United |

Campaign finance tables (individuals, PACs) use OpenSecrets' pre-assigned `Cycle` field from the raw FEC files. 527 organization data is a single continuous file filtered by transaction date to match these same windows. Both sources are trimmed to election day to exclude post-election activity.

---

## Spending Types

#### PAC Direct (`pac_direct`, `DI = 'D'`)

A traditional PAC collects donations and writes a check directly to a candidate's campaign. The candidate's campaign then spends however it wants. Since the money goes to the campaign, it's subject to hard FEC contribution limits ($5,000/candidate/election). *Citizens United* didn't change this. It's a small, capped, tightly regulated channel. Hence why totals here are only ~$3–5M per cycle.

#### Independent Expenditures (`pac_independent`, `DI = 'I'`)

A Super PAC (post-CU) or traditional PAC spends money on its own (TV ads, mailers, digital) explicitly advocating for or against a candidate. The money never touches the campaign. Because there's no coordination with the candidate, the Supreme Court ruled in *Citizens United* that limiting this spending violates the First Amendment. Post-CU, IE spending can be unlimited. This is the channel that exploded from $330M (2008) to $890M (2012).

#### 527 Spending

527s are named after the IRS tax code section that governs them. They were the pre-CU vehicle for unlimited outside spending. 527s could raise and spend without limits but were restricted to "issue advocacy," meaning they technically weren't supposed to say "vote for X," but can say "support for Y issue." In practice the line was blurry. Once *Citizens United* created Super PACs, which *can* explicitly say "vote for X," 527s declined as a vehicle for outside spending. The data shows this clearly: 527 spending fell from $311M (2008) to $179M (2012) as IE spending tripled.

#### The Substitution Story

The relationship between 527s and IEs is mostly substitution, not new money entering politics. That's why the outside spending ratio in Q1 was relatively stable across all four cycles. The total outside footprint didn't change as dramatically as the IE numbers alone suggest. What changed was the legal form and the explicit partisan direction of that spending.

---

## Data Caveats

**ViewPt retroactive alignment:** 527 committee partisan alignment (`ViewPt`) is assigned from the committee's most recent IRS filing year in the `cmtes_527` table. A committee that changed partisan alignment during the study period will have its final alignment applied retroactively to all earlier expenditures. This affects a small number of organizations and is unlikely to change aggregate totals materially, but individual committee-level analysis should cross-check `ViewPt` against the filing year.

**CPI adjustment methodology:** All dollar amounts are adjusted to constant 2024 dollars using a single annual CPI-U multiplier per cycle year (e.g., all 2008-cycle spending × 1.4611). The multiplier is pegged to the election year, not averaged over the full ~23-month cycle window. Spending from the odd year within a cycle is therefore very slightly over-deflated. This has negligible impact on aggregate comparisons.
