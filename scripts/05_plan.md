Pseudocode

 SETUP
   connect to citizens_united.db
   import pandas, matplotlib, statsmodels
   from statsmodels.tsa.stattools import grangercausalitytests
   create output/figures/ and output/tables/ if they don't exist
   set matplotlib style, figure dpi

─────────────────────────────────────────────
BLOCK 1: Load weekly partisan spending
─────────────────────────────────────────────
  query = """
    SELECT Cycle, era, year_week, partisan_direction, spending_type,
           SUM(total_amount_2024) AS total
    FROM partisan_spending_weekly
    WHERE partisan_direction IN ('pro_R', 'pro_D')
    GROUP BY Cycle, era, year_week, partisan_direction, spending_type
    ORDER BY Cycle, year_week, partisan_direction
  """
  df = pd.read_sql(query, conn)

  # Pivot to get one column per party per cycle
  # For each cycle separately:
  for each cycle in [2004, 2008, 2012, 2020]:
      sub = df[df.Cycle == cycle]
      weekly = sub.pivot_table(index='year_week',
                               columns='partisan_direction',
                               values='total',
                               aggfunc='sum').fillna(0)
      # weekly has columns: pro_R, pro_D
      weekly_by_cycle[cycle] = weekly

─────────────────────────────────────────────
BLOCK 2: Compute Arms Race Index per cycle
─────────────────────────────────────────────
  # Arms Race Index = min(pro_R, pro_D) / max(pro_R, pro_D)
  # 1.0 = perfect parity (both sides spending equally)
  # 0.0 = one side monopolizes all spending
  # Apply rolling 4-week window to smooth noise

  for each cycle in [2004, 2008, 2012, 2020]:
      w = weekly_by_cycle[cycle]
      w['total'] = w['pro_R'] + w['pro_D']
      w['arms_race_index'] = w[['pro_R','pro_D']].min(axis=1) / \
                             w[['pro_R','pro_D']].max(axis=1).replace(0, NaN)
      w['ari_rolling']     = w['arms_race_index'].rolling(4, min_periods=1).mean()

      # Cycle-level summary stats
      mean_ari[cycle]   = w['arms_race_index'].mean()
      median_ari[cycle] = w['arms_race_index'].median()

─────────────────────────────────────────────
BLOCK 3: Granger causality tests
─────────────────────────────────────────────
  # Test: does pro_R spending in week t predict pro_D spending in week t+k?
  # And vice versa? Positive result = evidence of reactive counter-spending.
  # Test at lags 1, 2, 4 weeks (short-cycle campaign response)
  # statsmodels.tsa.stattools.grangercausalitytests

  MAX_LAG = 4

  for each cycle in [2004, 2008, 2012, 2020]:
      w = weekly_by_cycle[cycle]
      data = w[['pro_D', 'pro_R']]  # test: does pro_R Granger-cause pro_D?

      results_RtoD[cycle] = grangercausalitytests(data[['pro_D','pro_R']], maxlag=MAX_LAG)
      results_DtoR[cycle] = grangercausalitytests(data[['pro_R','pro_D']], maxlag=MAX_LAG)

      # Extract p-values for each lag
      for lag in [1, 2, 3, 4]:
          p_RtoD[cycle][lag] = results_RtoD[cycle][lag][0]['ssr_ftest'][1]
          p_DtoR[cycle][lag] = results_DtoR[cycle][lag][0]['ssr_ftest'][1]

  # Build summary table: cycle × lag, p-values for both directions
  # Flag significance at α=0.05 and α=0.10

  # Note: weekly series has ~90 observations per cycle → reasonable power
  # Note: interpret carefully — spurious correlation common in time series

─────────────────────────────────────────────
BLOCK 4: Figure 1 — Weekly time series (2×2 grid, one panel per cycle)
─────────────────────────────────────────────
  # Each panel: pro_R (red) and pro_D (blue) weekly spending over the cycle
  # X-axis: weeks (number of weeks since cycle start, i.e. Jan of odd year)
  # Y-axis: weekly spending in 2024 $M
  # Mark weeks where Granger p-value < 0.10 with vertical dotted line
  # Add annotation for major spending spikes (top-5 by magnitude)

  fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=False)
  cycle_ax = dict(zip([2004, 2008, 2012, 2020], axes.flatten()))

  for cycle in [2004, 2008, 2012, 2020]:
      ax = cycle_ax[cycle]
      w  = weekly_by_cycle[cycle]
      ax.plot(range(len(w)), w['pro_R']/1e6, color='red',  label='Pro-Republican', lw=1.5)
      ax.plot(range(len(w)), w['pro_D']/1e6, color='blue', label='Pro-Democratic', lw=1.5)
      ax.fill_between(range(len(w)), w['pro_R']/1e6, w['pro_D']/1e6, alpha=0.1,
                      where=w['pro_R']>w['pro_D'], color='red',  label='Rep advantage')
      ax.fill_between(range(len(w)), w['pro_R']/1e6, w['pro_D']/1e6, alpha=0.1,
                      where=w['pro_D']>w['pro_R'], color='blue', label='Dem advantage')
      ax.set_title(f"{cycle} ({era_labels[cycle]})")
      ax.set_xlabel("Week of cycle")
      ax.set_ylabel("Weekly spending (2024 $M)")
      ax.legend(fontsize=7)

  fig.suptitle("Weekly Pro-R vs Pro-D Presidential Spending by Cycle")
  add OpenSecrets source attribution
  save to output/figures/q3_weekly_timeseries.png

─────────────────────────────────────────────
BLOCK 5: Figure 2 — Arms Race Index across cycles
─────────────────────────────────────────────
  # One panel per cycle: ARI rolling 4-week average over the campaign timeline
  # Y-axis: 0 to 1 (0=monopoly, 1=parity)
  # Horizontal dashed line at y=0.5 (moderate parity threshold)
  # Horizontal dashed line at y=0.8 (strong parity threshold)
  # Color-fill: green when ARI > 0.8 (true arms race), red when ARI < 0.2

  fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=True)
  for cycle in [2004, 2008, 2012, 2020]:
      ax = cycle_ax[cycle]
      w  = weekly_by_cycle[cycle]
      ax.plot(range(len(w)), w['ari_rolling'], color='black', lw=1.5)
      ax.axhline(0.5, color='gray',  linestyle='--', lw=0.8, label='0.5 threshold')
      ax.axhline(0.8, color='green', linestyle='--', lw=0.8, label='0.8 threshold')
      ax.fill_between(range(len(w)), w['ari_rolling'], 0,
                      where=w['ari_rolling']>0.8, alpha=0.2, color='green')
      ax.set_ylim(0, 1)
      ax.set_title(f"{cycle} — mean ARI = {mean_ari[cycle]:.2f}")
      ax.set_xlabel("Week of cycle")
      ax.set_ylabel("Arms Race Index")

  fig.suptitle("Arms Race Index (min/max ratio) by Presidential Cycle")
  add OpenSecrets source attribution
  save to output/figures/q3_arms_race_index.png

─────────────────────────────────────────────
BLOCK 6: Figure 3 — Granger causality p-value heatmap
─────────────────────────────────────────────
  # Heatmap: rows = cycle, columns = lag (1-4 weeks)
  # Two panels: R→D causality and D→R causality
  # Color scale: p < 0.05 = dark (significant), p > 0.1 = light
  # Annotate each cell with exact p-value

  import seaborn as sns
  fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

  pvals_RtoD = DataFrame of p_RtoD keyed by cycle (rows) and lag (cols)
  pvals_DtoR = DataFrame of p_DtoR keyed by cycle (rows) and lag (cols)

  sns.heatmap(pvals_RtoD, annot=True, fmt='.3f', vmin=0, vmax=0.1,
              cmap='RdYlGn_r', ax=ax1)
  ax1.set_title("Pro-R spending → Pro-D response\n(p-value, lower = more significant)")

  sns.heatmap(pvals_DtoR, annot=True, fmt='.3f', vmin=0, vmax=0.1,
              cmap='RdYlGn_r', ax=ax2)
  ax2.set_title("Pro-D spending → Pro-R response\n(p-value, lower = more significant)")

  fig.suptitle("Granger Causality Tests: Counter-Spending by Lag and Cycle")
  add OpenSecrets source attribution
  save to output/figures/q3_granger_heatmap.png

─────────────────────────────────────────────
BLOCK 7: Save results tables
─────────────────────────────────────────────
  save Granger p-value tables (both directions) to output/tables/q3_granger_results.csv
  save ARI summary (mean, median per cycle) to output/tables/q3_arms_race_index.csv
  save weekly data (all cycles combined) to output/tables/q3_weekly_spending.csv
  print nicely formatted Granger summary to console

---
Notes

- Granger causality is not true causation — it tests predictive precedence
- Weekly series: ~90 obs/cycle (Jan odd-year to election day) — sufficient for lags 1-4
- Use log(1 + spending) transformation if series are highly skewed
- ARI = NaN when both sides spend $0 in a week — exclude these weeks from ARI stats
- Rolling 4-week window smooths noise from lumpy campaign spending patterns
- The hypothesis: post-CU cycles show higher ARI AND stronger Granger causality
  than pre-CU cycles (arms race intensified by unlimited outside spending)
- Source attribution required on every figure: "Source: Center for Responsive Politics, OpenSecrets.org"
