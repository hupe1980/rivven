# Rivven Demo

Quick demo setup for manual testing.

## Quick Start

```bash
# Terminal 1 - Start broker
cd demo && ./demo.sh broker

# Terminal 2 - Run connect pipeline
cd demo && ./demo.sh connect

# Browser - View dashboard
open http://localhost:9094
```

## Commands

| Command | Description |
|---------|-------------|
| `./demo.sh broker` | Start broker with dashboard on port 9094 |
| `./demo.sh connect` | Run datagen → stdout pipeline |
| `./demo.sh clean` | Clean up demo data directory |

## What to Expect

- **Broker**: Starts on port 9092 with dashboard on 9094
- **Connect**: Generates 2 events/sec (max 50 total)
- **Events**: Printed to console with pretty JSON formatting
- **Dashboard**: Shows topics, consumer groups, and lag metrics

## Files

- `demo.sh` - Demo runner script
- `connect.yaml` - Connect pipeline configuration
- `data/` - Broker data directory (created on first run)
