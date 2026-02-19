### Final Project — Money, and the State

Analyzes how *Citizens United v. FEC* (2010) transformed presidential election spending, examining the shift from traditional PAC contributions to Super PAC and dark money expenditures across four presidential cycles: 2004, 2008, 2012, 2016.

Raw data and database are too large to track with git. The file structure exists in this repo. Raw data can be downloaded from [Google Drive](https://drive.google.com/file/d/16Q7fw9DEty8_A7-Xs9zoUxQrSjRW4kBu/view?usp=sharing).

See `CLAUDE.md` for full project specification, data dictionary, and pipeline documentation.

#### Cycle Windows

Both data sources are filtered to the same window per cycle: **Jan 1 of the odd year through presidential election day**.

| Cycle | Window | Era |
|---|---|---|
| 2004 | 2003-01-01 – 2004-11-02 | pre-Citizens United |
| 2008 | 2007-01-01 – 2008-11-04 | pre-Citizens United |
| 2012 | 2011-01-01 – 2012-11-06 | post-Citizens United |
| 2016 | 2015-01-01 – 2016-11-08 | post-Citizens United |

**Campaign finance tables** (individuals, PACs) use OpenSecrets' pre-assigned `Cycle` field from the raw FEC files. **527 organization data** is a single continuous file filtered by transaction date to match these same windows.

