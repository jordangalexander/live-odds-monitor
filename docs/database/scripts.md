# Script Reference

## Overview

```mermaid
mindmap
  root((Live Odds Monitor))
    Data Collection
      watch_live.py
        Live game monitoring
        Real-time alerts
        Records to bet_outcomes
      historical_backfill.py
        Past game analysis
        Cached API calls
        Records to simulated_bets
    Analysis
      analyze_bets.py
        Reads all bet data
        Finds optimal thresholds
        Multi-factor analysis
      backtest.py
        Simple bet analysis
        Quick stats
    Configuration
      config.py
        Team watchlist
        Alert thresholds
        Spread filters
    Core Library
      odds_api.py
        API client
      storage.py
        Database operations
      tracker.py
        Historical tracking
```

## Script Details

### watch_live.py

**Purpose:** Monitor live games and alert on spread changes

**Data Written:**
| Table | Data |
|-------|------|
| `games` | New games discovered |
| `opening_odds` | First odds seen |
| `odds_history` | All odds snapshots |
| `alerts` | Triggered alerts |
| `line_snapshots` | Live line movements |
| `bet_outcomes` | Hypothetical bets |

**Key Functions:**
```
main()
└── poll_loop()
    ├── client.get_live_odds()
    ├── storage.save_game()
    ├── storage.save_opening_odds()
    ├── tracker.record_opening_line()
    ├── check_for_alerts()
    │   └── tracker.record_alert()
    └── sleep(poll_interval)
```

---

### historical_backfill.py

**Purpose:** Fetch historical games and simulate betting strategies

**Data Written:**
| Table | Data |
|-------|------|
| `games` | Historical games |
| `game_results` | Final scores |
| `historical_odds_cache` | Cached API responses |
| `simulated_bets` | Strategy simulations |

**Key Functions:**
```
main()
├── get_completed_games()
├── fetch_historical_odds_for_game()
│   ├── check_cache()
│   └── client.get_historical_odds()
├── simulate_bets_for_game()
│   └── test_all_thresholds()
└── print_strategy_analysis()
```

**API Credit Usage:**
- Scores endpoint: Free
- Historical odds: 10 credits per call
- Uses cache to avoid duplicate calls

---

### analyze_bets.py

**Purpose:** Analyze all bet data to find optimal parameters

**Data Read:**
| Table | Source |
|-------|--------|
| `simulated_bets` | Historical backfill |
| `bet_outcomes` | Live monitor |

**Analysis Types:**
1. **Threshold Optimization** - Find best % change
2. **Spread Size Analysis** - Small/Medium/Large
3. **Bet Direction** - Favorites vs Underdogs
4. **Line Movement** - Toward favorite/underdog
5. **Margin Analysis** - How close are losses?
6. **Multi-Factor** - Combine all filters

**Key Functions:**
```
main()
├── load_all_bets()
│   ├── query simulated_bets
│   └── query bet_outcomes
├── analyze_by_threshold()
├── analyze_by_spread_size()
├── analyze_by_bet_direction()
├── analyze_by_line_movement_direction()
├── analyze_margin_of_victory()
├── analyze_combined_filters()
└── suggest_optimal_strategy()
```

---

## Relationships Between Scripts

```mermaid
graph LR
    subgraph Collection["Data Collection"]
        WL[watch_live.py]
        HB[historical_backfill.py]
    end
    
    subgraph Storage["Database"]
        BO[(bet_outcomes)]
        SB[(simulated_bets)]
    end
    
    subgraph Analysis["Analysis"]
        AB[analyze_bets.py]
    end
    
    subgraph Config["Config"]
        CFG[config.py]
    end
    
    WL -->|writes| BO
    HB -->|writes| SB
    BO -->|reads| AB
    SB -->|reads| AB
    CFG -->|thresholds| WL
    CFG -->|watchlist| HB
    AB -->|"suggests optimal\nthreshold"| CFG
    
    style AB fill:#90EE90
    style CFG fill:#FFD700
```

## Typical Workflow

```mermaid
graph TD
    A[1. Run historical_backfill.py] -->|Populate simulated_bets| B
    B[2. Run analyze_bets.py] -->|Find optimal params| C
    C[3. Update config.py] -->|Set thresholds| D
    D[4. Run watch_live.py] -->|Monitor with optimal settings| E
    E[5. Collect live data] -->|Over time| F
    F[6. Re-run analyze_bets.py] -->|Include live data| G
    G[7. Refine parameters] --> C
```
