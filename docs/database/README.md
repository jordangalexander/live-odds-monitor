# Live Odds Monitor - Database Documentation

Visual documentation for the database architecture and data flows.

## Files

| File | Description |
|------|-------------|
| [schema.md](schema.md) | Entity-relationship diagram and table definitions |
| [data-flow.md](data-flow.md) | Sequence diagrams showing data flow between components |
| [scripts.md](scripts.md) | Script reference and relationships |

## Quick Reference

### Database: `odds_monitor.db` (SQLite)

```
┌─────────────────────────────────────────────────────────────────┐
│                        CORE TABLES                               │
├─────────────────────────────────────────────────────────────────┤
│  games              - Central registry of all games              │
│  game_results       - Final scores                               │
│  opening_odds       - First recorded odds per game               │
│  odds_history       - All odds changes over time                 │
├─────────────────────────────────────────────────────────────────┤
│                      TRACKING TABLES                             │
├─────────────────────────────────────────────────────────────────┤
│  line_snapshots     - Live line movements (from watch_live.py)  │
│  alerts             - Triggered alerts                           │
│  bet_outcomes       - Live monitor bets                          │
├─────────────────────────────────────────────────────────────────┤
│                      BACKFILL TABLES                             │
├─────────────────────────────────────────────────────────────────┤
│  historical_odds_cache  - Cached API responses (saves credits)  │
│  simulated_bets         - Backtest simulation results           │
└─────────────────────────────────────────────────────────────────┘
```

### Key Scripts

```
watch_live.py          → bet_outcomes (live bets)
historical_backfill.py → simulated_bets (backtest bets)
analyze_bets.py        ← reads BOTH tables for analysis
```

## Viewing Diagrams

These diagrams use [Mermaid](https://mermaid.js.org/) syntax. To view them:

1. **GitHub/GitLab** - Renders automatically in markdown preview
2. **VS Code** - Install "Markdown Preview Mermaid Support" extension
3. **Online** - Paste into [mermaid.live](https://mermaid.live)
4. **CLI** - Use `mmdc` (mermaid-cli) to generate images:
   ```bash
   npm install -g @mermaid-js/mermaid-cli
   mmdc -i schema.md -o schema.png
   ```
