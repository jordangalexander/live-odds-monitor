# Database Schema

## Entity Relationship Diagram

```mermaid
erDiagram
    games ||--o{ opening_odds : has
    games ||--o{ odds_history : has
    games ||--o{ alerts : has
    games ||--o{ game_results : has
    games ||--o{ line_snapshots : has
    games ||--o{ bet_outcomes : has
    games ||--o{ simulated_bets : has
    games ||--o{ historical_odds_cache : has
    alerts ||--o{ bet_outcomes : triggers

    games {
        text id PK "API game ID"
        text home_team
        text away_team
        text commence_time
        text sport
        text created_at
    }

    opening_odds {
        text game_id PK,FK
        real spread_home
        real spread_away
        int spread_home_price
        int spread_away_price
        int moneyline_home
        int moneyline_away
        real total
        int over_price
        int under_price
        text fetched_at
    }

    odds_history {
        int id PK
        text game_id FK
        real spread_home
        real spread_away
        int spread_home_price
        int spread_away_price
        int moneyline_home
        int moneyline_away
        real total
        int over_price
        int under_price
        text recorded_at
    }

    alerts {
        int id PK
        text game_id FK
        text alert_type
        text message
        text sent_at
    }

    game_results {
        text game_id PK,FK
        int final_score_home
        int final_score_away
        text completed_at
    }

    line_snapshots {
        int id PK
        text game_id FK
        text timestamp
        text bookmaker
        text spread_team
        real spread_value
        int spread_price
        int home_score
        int away_score
        real mins_remaining
        int is_opening
    }

    bet_outcomes {
        int id PK
        text game_id FK
        int alert_id FK
        text bet_type
        text bet_team
        real bet_spread
        real mins_remaining
        real opening_spread
        real pct_change
        int final_margin
        int covered
        real profit
    }

    historical_odds_cache {
        int id PK
        text game_id FK
        text sport
        text snapshot_type "opening, midgame, pregame"
        text timestamp
        text bookmaker
        real spread_home
        real spread_away
        int spread_home_price
        int spread_away_price
        int moneyline_home
        int moneyline_away
        real total
        int over_price
        int under_price
        text fetched_at
    }

    simulated_bets {
        int id PK
        text game_id FK
        text sport
        text strategy "spread_change_50pct, etc"
        text bet_team
        real bet_spread
        real opening_spread
        real pct_change
        text snapshot_type
        int final_margin
        int covered
        real profit
        text created_at
    }
```

## Table Purposes

| Table | Purpose | Written By | Read By |
|-------|---------|------------|---------|
| `games` | Core game registry | All scripts | All scripts |
| `opening_odds` | First recorded odds | watch_live.py | watch_live.py |
| `odds_history` | All odds snapshots over time | watch_live.py | Analysis |
| `alerts` | Record of triggered alerts | watch_live.py | Analysis |
| `game_results` | Final scores | Both | analyze_bets.py |
| `line_snapshots` | Live line movements | watch_live.py | Analysis |
| `bet_outcomes` | Live monitor bets | watch_live.py | analyze_bets.py |
| `historical_odds_cache` | Cached API responses | historical_backfill.py | historical_backfill.py |
| `simulated_bets` | Backfill simulated bets | historical_backfill.py | analyze_bets.py |

## Key Relationships

1. **games** is the central table - all other tables reference it via `game_id`
2. **bet_outcomes** can optionally link to an **alert** that triggered the bet
3. **historical_odds_cache** uses `UNIQUE(game_id, snapshot_type, bookmaker)` to prevent duplicate API calls
