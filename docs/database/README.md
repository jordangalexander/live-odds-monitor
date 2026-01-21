# Live Odds Monitor - Database Documentation

Visual documentation for the database architecture and data flows.

## Files

| File | Description |
|------|-------------|
| [schema.md](schema.md) | Entity-relationship diagram and table definitions |
| [data-flow.md](data-flow.md) | Data flow diagrams between components |
| [scripts.md](scripts.md) | Script reference and relationships |

## Quick Reference

### Database: `odds_monitor.db` (SQLite)

```
┌─────────────────────────────────────────────────────────────────┐
│                        CORE TABLES (v2)                          │
├─────────────────────────────────────────────────────────────────┤
│  games              - Central registry of all games              │
│  game_results       - Final scores                               │
│  odds_snapshots     - ALL odds (opening + live + backtest)       │
│  bets               - ALL bets (live + backtest)                 │
│  alerts             - Triggered alerts                           │
├─────────────────────────────────────────────────────────────────┤
│                      CACHE TABLE                                 │
├─────────────────────────────────────────────────────────────────┤
│  historical_odds_cache  - Cached API responses (saves credits)  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Scripts

```
watch_live.py          → bets (source='live')
backfill.py            → bets (source='backtest')
analyze.py             ← reads unified bets table
```

### Schema v2 Migration

The schema was consolidated from 9 tables to 5:
- `opening_odds` + `odds_history` + `line_snapshots` → `odds_snapshots`
- `bet_outcomes` + `simulated_bets` → `bets`

Migration happens automatically on first run.

## Viewing Diagrams

These diagrams use [Mermaid](https://mermaid.js.org/) syntax. To view them:

1. **GitHub/GitLab** - Renders automatically in markdown preview
2. **VS Code** - Install "Markdown Preview Mermaid Support" extension
3. **Online** - Paste into [mermaid.live](https://mermaid.live)
