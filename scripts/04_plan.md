Pseudocode

 SETUP
   connect to citizens_united.db
   import pandas, matplotlib, statsmodels
   create output/figures/ and output/tables/ if they don't exist
   set matplotlib style, figure dpi

─────────────────────────────────────────────
BLOCK 1: Load partisan spending by direction and type
─────────────────────────────────────────────
  query = """
    SELECT Cycle, era, spending_type, partisan_direction,
           SUM(total_amount_2024) AS total,
           SUM(n_transactions)    AS n_trans
    FROM partisan_spending_monthly
    WHERE partisan_direction IN ('pro_R', 'pro_D')
    GROUP BY Cycle, era, spending_type, partisan_direction
    ORDER BY Cycle, spending_type, partisan_direction
  """
  df = pd.read_sql(query, conn)

  # Pivot: rows=Cycle, columns=(spending_type, partisan_direction)
  pivot = df.pivot_table(index='Cycle',
                         columns=['spending_type', 'partisan_direction'],
                         values='total',
                         aggfunc='sum').fillna(0)

─────────────────────────────────────────────
BLOCK 2: Compute partisan gap per cycle per spending type
─────────────────────────────────────────────
  # Partisan gap = (pro_R − pro_D) / (pro_R + pro_D)
  # Ranges from -1 (all Dem) to +1 (all Rep), 0 = parity
  # Compute separately for each spending vehicle

  for each spending_type in [individual, pac_direct, pac_independent, 527]:
      pro_R = pivot[(spending_type, 'pro_R')]
      pro_D = pivot[(spending_type, 'pro_D')]
      gap[spending_type] = (pro_R - pro_D) / (pro_R + pro_D)

  gaps_df = DataFrame of partisan gaps indexed by Cycle
  gaps_df['era'] = ['pre_CU', 'pre_CU', 'post_CU', 'post_CU']

─────────────────────────────────────────────
BLOCK 3: Difference-in-Differences (DID) setup
─────────────────────────────────────────────
  # Direct contributions = control group (limits unchanged by CU)
  # Independent expenditures = treatment group (unlimited post-CU)
  # DID estimate captures the differential change in partisan gap

  # "pre" era = average of 2004 + 2008
  # "post" era = average of 2012 + 2020

  pre_direct_gap  = gaps_df.loc[['2004','2008'], 'pac_direct'].mean()
  post_direct_gap = gaps_df.loc[['2012','2020'], 'pac_direct'].mean()
  pre_ie_gap      = gaps_df.loc[['2004','2008'], 'pac_independent'].mean()
  post_ie_gap     = gaps_df.loc[['2012','2020'], 'pac_independent'].mean()

  # DID = (change in IE gap) − (change in direct gap)
  # If positive → IE channel became MORE Republican-favoring after CU
  # If near zero → both channels moved in parallel → no CU partisan tilt
  did_estimate = (post_ie_gap - pre_ie_gap) - (post_direct_gap - pre_direct_gap)

  print DID estimate with interpretation
  # Note: 4 cycles means this is descriptive, not inferential

─────────────────────────────────────────────
BLOCK 4: Figure 1 — Pro-R vs Pro-D total spending per cycle (grouped bar)
─────────────────────────────────────────────
  # Two bars per cycle: pro_R (red) and pro_D (blue)
  # Sum across ALL spending types (individual + pac_direct + pac_independent + 527)
  # Y-axis in 2024 $B

  total_proR = pivot.xs('pro_R', axis=1, level=1).sum(axis=1)
  total_proD = pivot.xs('pro_D', axis=1, level=1).sum(axis=1)

  fig, ax = plt.subplots()
  x = [0, 1, 2, 3]  # one position per cycle
  ax.bar([xi - 0.2 for xi in x], total_proR/1e9, width=0.35, color='red',  label='Pro-Republican')
  ax.bar([xi + 0.2 for xi in x], total_proD/1e9, width=0.35, color='blue', label='Pro-Democratic')
  add vertical dashed line between 2008 and 2012 (CU decision)
  label "Citizens United (Jan 2010)"
  ax.set_xticks(x); ax.set_xticklabels(['2004','2008','2012','2020'])
  ax.set_ylabel("Total Spending (2024 $B)")
  ax.set_title("Pro-Republican vs. Pro-Democratic Presidential Spending by Cycle")
  ax.legend()
  add OpenSecrets source attribution
  save to output/figures/q2_partisan_totals.png

─────────────────────────────────────────────
BLOCK 5: Figure 2 — Partisan gap over time by spending type
─────────────────────────────────────────────
  # Line chart with one line per spending vehicle
  # X-axis: 4 cycles; Y-axis: partisan gap (%)
  # Positive = Republican-favoring; Negative = Democratic-favoring
  # Key visual: if IE gap diverges from direct gap post-CU → CU partisan effect

  fig, ax = plt.subplots()
  colors = {'pac_direct': 'green', 'pac_independent': 'orange',
            'individual': 'blue', '527': 'purple'}
  labels = {'pac_direct': 'PAC Direct', 'pac_independent': 'PAC Independent (IE)',
            'individual': 'Individual', '527': '527 Spending'}

  for spending_type in [individual, pac_direct, pac_independent, 527]:
      ax.plot(cycles, gaps_df[spending_type]*100, marker='o',
              color=colors[spending_type], label=labels[spending_type])

  add horizontal dashed line at y=0 (parity)
  add vertical dashed line between 2008/2012 (CU)
  ax.set_ylabel("Partisan Gap (%)\n← Pro-Democratic | Pro-Republican →")
  ax.set_title("Partisan Spending Gap by Vehicle Type: 2004–2020")
  ax.legend()
  add OpenSecrets source attribution
  save to output/figures/q2_partisan_gap_by_type.png

─────────────────────────────────────────────
BLOCK 6: Figure 3 — DID visualization
─────────────────────────────────────────────
  # Two-panel bar chart:
  # Left panel: direct contributions gap (pre vs post)
  # Right panel: independent expenditures gap (pre vs post)
  # Arrows showing direction of change in each group
  # DID = (IE change) − (Direct change) annotated on figure

  fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True)
  ax1.bar(['Pre-CU', 'Post-CU'], [pre_direct_gap*100, post_direct_gap*100], color='green')
  ax1.set_title("PAC Direct (Control)")
  ax2.bar(['Pre-CU', 'Post-CU'], [pre_ie_gap*100,    post_ie_gap*100],    color='orange')
  ax2.set_title("PAC Independent (Treatment)")
  annotate DID estimate on figure
  ax1.set_ylabel("Partisan Gap (%)")
  fig.suptitle("Difference-in-Differences: CU Effect on Partisan Spending Gap")
  add OpenSecrets source attribution
  save to output/figures/q2_did.png

─────────────────────────────────────────────
BLOCK 7: Save results tables
─────────────────────────────────────────────
  save gaps_df to output/tables/q2_partisan_gaps.csv
  save DID summary (pre/post gaps, DID estimate) to output/tables/q2_did_results.csv
  save full pivot (pro_R + pro_D by cycle + spending_type) to output/tables/q2_spending_by_direction.csv
  print nicely formatted summary to console

---
Notes

- Partisan gap is directional: positive = more Republican, negative = more Democratic
- DID interpretation: if IE gap diverges from direct gap post-CU, CU created partisan tilt
- With only 4 cycles (2 pre, 2 post), treat DID as descriptive, not causal
- 527 data uses ViewPt field for partisan direction — already handled in exp527_aligned
- Source attribution required on every figure: "Source: Center for Responsive Politics, OpenSecrets.org"
- pre_CU = average of 2004 + 2008; post_CU = average of 2012 + 2020
