### Final Project — Money, and the State

Analyzes how *Citizens United v. FEC* (2010) transformed presidential election spending, examining the shift from traditional PAC contributions to Super PAC and dark money expenditures across four presidential cycles: 2004, 2008, 2012, 2020.

Raw data and database are too large to track with git. The file structure exists in this repo. Raw data can be downloaded [Here](https://drive.google.com/file/d/17PC6LPCypeEOMBU7gHM3LcGgfPcWXeXH/view?usp=sharing) and the databased can be downloaded [Here](https://drive.google.com/file/d/1g9gIubmGwO2Fs-B9h06QGxR7-zqXlevt/view?usp=sharing)

#### Cycle Windows

Both data sources are filtered to the same window per cycle: **Jan 1 of the odd year through presidential election day**.

| Cycle | Window | Era |
|---|---|---|
| 2004 | 2003-01-01 – 2004-11-02 | pre-Citizens United |
| 2008 | 2007-01-01 – 2008-11-04 | pre-Citizens United |
| 2012 | 2011-01-01 – 2012-11-06 | post-Citizens United |
| 2020 | 2019-01-01 – 2020-11-03 | post-Citizens United |

**Campaign finance tables** (individuals, PACs) use OpenSecrets' pre-assigned `Cycle` field from the raw FEC files. **527 organization data** is a single continuous file filtered by transaction date to match these same windows. Both sources are trimmed to election day to exclude post-election activity.

#### Data Caveats

**ViewPt retroactive alignment:** 527 committee partisan alignment (`ViewPt`) is assigned from the committee's most recent IRS filing year in the `cmtes_527` table. A committee that changed partisan alignment during the study period will have its final alignment applied retroactively to all earlier expenditures. This affects a small number of organizations and is unlikely to change aggregate totals materially, but individual committee-level analysis should cross-check `ViewPt` against the filing year.

**CPI adjustment methodology:** All dollar amounts are adjusted to constant 2024 dollars using a single annual CPI-U multiplier per cycle year (e.g., all 2008-cycle spending × 1.4611). The multiplier is pegged to the election year, not averaged over the full ~23-month cycle window. Spending from the odd year within a cycle is therefore very slightly over-deflated. This has negligible impact on aggregate comparisons.
