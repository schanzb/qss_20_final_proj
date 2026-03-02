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

### Spending Types
#### PAC Direct (pac_direct, DI = 'D')
A traditional PAC collects donations and writes a check directly to a candidate's campaign. The candidate's campaign then spends the money however they want. Because it goes to the campaign, it's subject to hard FEC contribution limits ($5,000/candidate/election). CU did not change this. It's a small, capped, tightly regulated channel — which is why your numbers show only ~$3–5M per cycle.


#### Independent Expenditures (pac_independent, DI = 'I')
A Super PAC (post-CU) or traditional PAC spends money on its own — buying TV ads, mailers, digital ads — expressly advocating for or against a candidate. The money never touches the campaign. Because there's no coordination with the candidate, the Supreme Court ruled in Citizens United that limits on this spending violate the First Amendment. Post-CU: unlimited. This is the channel that exploded from $330M (2008) to $890M (2012).


#### 527 Spending
527s are named after the IRS tax code section that governs them. They were the pre-CU workaround for unlimited outside spending. They could raise and spend unlimited money but were restricted to "issue advocacy" — they weren't supposed to explicitly say "vote for X." In practice the line was blurry. After CU created Super PACs (which can explicitly say "vote for X"), 527s declined as a vehicle. Your data shows this substitution clearly: 527 spending fell from $311M (2008) to $179M (2012) as IE spending tripled.

#### Summary
The key relationship: 527s → IEs is a substitution story, not new money entering politics. That's why the outside_ratio in Q1 was relatively stable across all four cycles — the total outside spending footprint didn't change as dramatically as the IE numbers alone suggest. What changed was the legal form and the explicit partisan direction of that spending.

#### Data Caveats

**ViewPt retroactive alignment:** 527 committee partisan alignment (`ViewPt`) is assigned from the committee's most recent IRS filing year in the `cmtes_527` table. A committee that changed partisan alignment during the study period will have its final alignment applied retroactively to all earlier expenditures. This affects a small number of organizations and is unlikely to change aggregate totals materially, but individual committee-level analysis should cross-check `ViewPt` against the filing year.

**CPI adjustment methodology:** All dollar amounts are adjusted to constant 2024 dollars using a single annual CPI-U multiplier per cycle year (e.g., all 2008-cycle spending × 1.4611). The multiplier is pegged to the election year, not averaged over the full ~23-month cycle window. Spending from the odd year within a cycle is therefore very slightly over-deflated. This has negligible impact on aggregate comparisons.
