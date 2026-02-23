Pseudocode

 SETUP
   connect to citizens_united.db
   create output/figures/ and output/tables/ if they don't exist
   set matplotlib style, figure dpi

 ─────────────────────────────────────────────
 BLOCK 1: Load and aggregate
 ─────────────────────────────────────────────
   query = """
     SELECT Cycle, era, spending_type,
            SUM(total_amount_2024) AS total,
            SUM(n_transactions)    AS n_trans
     FROM partisan_spending_monthly
     WHERE partisan_direction != 'unaligned'
     GROUP BY Cycle, era, spending_type
     ORDER BY Cycle, spending_type
   """
   df = pd.read_sql(query, conn)

   pivot = df.pivot(index='Cycle',
                    columns='spending_type',
                    values='total').fillna(0)
   # columns: individual, pac_direct, pac_independent, 527

   pivot['outside_spending'] = pivot['pac_independent'] + pivot['527']
   pivot['total']             = pivot.sum(axis=1)
   pivot['ie_ratio']          = pivot['pac_independent'] / pivot['total']
   pivot['outside_ratio']     = pivot['outside_spending'] / pivot['total']

 ─────────────────────────────────────────────
 BLOCK 2: Key metrics (print + save to CSV)
 ─────────────────────────────────────────────
   # PAC IE growth: 2008→2012 (the CU transition)
   ie_2008 = pivot.loc['2008', 'pac_independent']
   ie_2012 = pivot.loc['2012', 'pac_independent']
   ie_growth_pct = (ie_2012 - ie_2008) / ie_2008 * 100   # ~594% per literature

   # PAC direct growth: 2008→2012 (the ~14% claim)
   direct_2008 = pivot.loc['2008', 'pac_direct']
   direct_2012 = pivot.loc['2012', 'pac_direct']
   direct_growth_pct = (direct_2012 - direct_2008) / direct_2008 * 100

   # Outside spending pre vs post CU (average of 2 cycles each)
   pre_outside  = pivot.loc[['2004','2008'], 'outside_spending'].mean()
   post_outside = pivot.loc[['2012','2020'], 'outside_spending'].mean()

   print summary table of all metrics
   save pivot to output/tables/q1_spending_by_type.csv

 ─────────────────────────────────────────────
 BLOCK 3: Figure 1 — Stacked bar by spending type
 ─────────────────────────────────────────────
   # One bar per cycle, stacked by spending type
   # Order from bottom: individual, pac_direct, pac_independent, 527
   # Colors: individual=blue, pac_direct=teal, pac_independent=orange, 527=red

   fig, ax = plt.subplots()
   bottom = zeros array length 4

   for each spending_type in [individual, pac_direct, pac_independent, 527]:
       ax.bar(cycles, pivot[spending_type]/1e9, bottom=bottom, label=label)
       bottom += pivot[spending_type]/1e9

   add vertical dashed line between 2008 and 2012 (CU decision Jan 2010)
   label it "Citizens United (Jan 2010)"
   ax.set_ylabel("Spending (2024 $B)")
   ax.set_title("Presidential Election Spending by Type")
   ax.legend()
   save to output/figures/q1_spending_stacked_bar.png

 ─────────────────────────────────────────────
 BLOCK 4: Figure 2 — IE ratio over time
 ─────────────────────────────────────────────
   # Line chart: pac_independent / total per cycle
   # Shows the structural shift CU caused

   fig, ax = plt.subplots()
   ax.plot(cycles, pivot['ie_ratio'] * 100, marker='o')
   add vertical dashed line between 2008/2012 for CU
   ax.set_ylabel("PAC Independent Expenditures as % of Total Spending")
   ax.set_title("Rise of Independent Expenditures")
   save to output/figures/q1_ie_ratio.png

 ─────────────────────────────────────────────
 BLOCK 5: Figure 3 — IE vs Direct PAC growth comparison
 ─────────────────────────────────────────────
   # Grouped bar: 2008 vs 2012 for pac_direct and pac_independent
   # Illustrates the ~14% vs ~594% contrast

   fig, ax = plt.subplots()
   x = [0, 1]   # two groups: pac_direct, pac_independent
   ax.bar([0-0.2, 1-0.2], [direct_2008/1e9, ie_2008/1e9], width=0.35,
 label='2008 (pre-CU)')
   ax.bar([0+0.2, 1+0.2], [direct_2012/1e9, ie_2012/1e9], width=0.35,
 label='2012 (post-CU)')
   annotate growth % on bars
   ax.set_xticks([0,1]); ax.set_xticklabels(['PAC Direct', 'PAC Independent'])
   ax.set_ylabel("Spending (2024 $B)")
   ax.set_title("Direct vs Independent PAC Spending: 2008→2012")
   ax.legend()
   save to output/figures/q1_pac_direct_vs_ie.png

 ─────────────────────────────────────────────
 BLOCK 6: Save summary stats table
 ─────────────────────────────────────────────
   summary = pivot with extra columns:
     - ie_ratio (%)
     - outside_ratio (%)
     - ie_growth_from_prev_cycle (%)
   save to output/tables/q1_summary_stats.csv
   print nicely formatted table to console

 ---
 Notes

 - All dollar amounts in 2024 $ — already in total_amount_2024
 - "Outside spending" = pac_independent + 527 (the pre-CU vehicle was 527,
 post-CU is IE)
 - Don't filter by partisan_direction in Q1 — we want total spending regardless
  of party
 (but do exclude 'unaligned' to avoid double-counting noise)
 - The CU line goes between 2008 and 2012 bars — decision was Jan 21, 2010
 - Source attribution on every figure: "Source: Center for Responsive Politics,
  OpenSecrets.org"
