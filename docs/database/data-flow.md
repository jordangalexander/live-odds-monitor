# Data Flow Architecture

## High-Level System Overview

```mermaid
flowchart TB
    subgraph External["External Services"]
        API[("The Odds API\n(20k credits/month)")]
    end

    subgraph Scripts["Python Scripts"]
        WL["watch_live.py\n(Live Monitor)"]
        HB["historical_backfill.py\n(Backfill)"]
        AB["analyze_bets.py\n(Analysis)"]
    end

    subgraph Database["SQLite Database"]
        DB[(odds_monitor.db)]
    end

    subgraph Config["Configuration"]
        CFG["config.py\n(Watchlist + Thresholds)"]
        ENV[".env\n(API Key)"]
    end

    API -->|Live Odds| WL
    API -->|Historical Odds| HB
    API -->|Scores| WL
    API -->|Scores| HB

    WL -->|bet_outcomes| DB
    HB -->|simulated_bets| DB
    
    DB -->|Read Both| AB
    
    CFG --> WL
    CFG --> HB
    ENV --> WL
    ENV --> HB
```

## Live Monitor Flow (watch_live.py)

```mermaid
sequenceDiagram
    participant User
    participant Script as watch_live.py
    participant API as The Odds API
    participant DB as SQLite
    participant Tracker as HistoricalTracker

    User->>Script: Run monitor
    
    loop Every 2 minutes
        Script->>API: Get live odds
        API-->>Script: Games + spreads
        
        loop Each game
            Script->>DB: Check opening_odds exists?
            alt No opening line
                Script->>DB: Save to opening_odds
                Script->>Tracker: record_opening_line()
                Tracker->>DB: Save to line_snapshots
            end
            
            Script->>Script: Calculate % change
            
            alt Change >= threshold
                Script->>DB: Save alert
                Script->>Tracker: record_alert()
                Tracker->>DB: Save to bet_outcomes
                Script->>User: Print alert! 🚨
            end
        end
        
        Script->>Script: Sleep 2 min
    end
```

## Historical Backfill Flow (historical_backfill.py)

```mermaid
sequenceDiagram
    participant User
    participant Script as historical_backfill.py
    participant API as The Odds API
    participant DB as SQLite
    participant Cache as historical_odds_cache

    User->>Script: Run --days 7
    
    loop Each sport (NCAAB, NBA)
        Script->>API: Get completed scores
        API-->>Script: List of finished games
        
        loop Each game
            Script->>Cache: Check if cached?
            alt Cached
                Cache-->>Script: Return cached odds
            else Not cached
                Script->>API: Get historical odds (10 credits)
                API-->>Script: Opening + midgame odds
                Script->>Cache: Save to cache
            end
            
            Script->>DB: Save game
            Script->>DB: Save game_result
            
            Script->>Script: Simulate bets at all thresholds
            Script->>DB: Save to simulated_bets
        end
    end
    
    Script->>User: Print strategy analysis
```

## Analysis Flow (analyze_bets.py)

```mermaid
flowchart LR
    subgraph Database
        SB[(simulated_bets\nBackfill Data)]
        BO[(bet_outcomes\nLive Data)]
    end

    subgraph Analysis["analyze_bets.py"]
        LOAD[Load All Bets]
        TH[Threshold\nOptimization]
        SS[Spread Size\nAnalysis]
        DIR[Direction\nAnalysis]
        MF[Multi-Factor\nCombinations]
        REC[Recommend\nStrategy]
    end

    subgraph Output
        REPORT[/"Strategy Report\n(Terminal Output)"/]
    end

    SB --> LOAD
    BO --> LOAD
    LOAD --> TH
    LOAD --> SS
    LOAD --> DIR
    TH --> MF
    SS --> MF
    DIR --> MF
    MF --> REC
    REC --> REPORT
```

## Bet Resolution Flow

```mermaid
stateDiagram-v2
    [*] --> GameStarted: Game begins
    
    GameStarted --> LineMonitored: watch_live.py polls
    LineMonitored --> AlertTriggered: Spread changes ≥ threshold
    AlertTriggered --> BetRecorded: Save to bet_outcomes
    
    BetRecorded --> Pending: covered = NULL
    
    GameStarted --> GameCompleted: Game ends
    GameCompleted --> ResultsSaved: Save to game_results
    
    Pending --> Resolved: resolve_pending_bets()
    ResultsSaved --> Resolved
    
    Resolved --> Won: covered = 1, profit = +100
    Resolved --> Lost: covered = 0, profit = -110
    
    Won --> [*]
    Lost --> [*]
```
