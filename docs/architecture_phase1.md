# Suplock Protocol Ecosystem & Suplock Nexus Platform
## Autonomous, Enterprise-Grade AI Market Signal & Execution Engine
### Phase 1: System Architecture, Scaffolding, & Data Contracts

This document contains the detailed system architecture, directory topology, initialization guidelines, and strict data contracts for the enterprise-grade multi-agent quantitative pipeline.

---

## 1. Optimal Monorepo Directory Tree

A clean, modular monorepo structure guarantees complete decoupling of our decision (Pro-Trader) and execution (dgclaw-skill) pipelines, whilst allowing the Loop Orchestrator to govern the overall system orchestration and budget management.

```text
suplock-nexus-monorepo/
├── docker-compose.yml                  # Stands up Postgres/pgvector & local service mesh
├── package.json                        # Root package (Workspaces config)
├── lerna.json or turbo.json            # High-performance monorepo build runner
├── docs/                               # System documentation and manuals
│   └── architecture_phase1.md          # This architecture specification
│
├── apps/
│   └── trading-dashboard/              # Live React + Vite interactive verification UI (Port 3000)
│
├── services/
│   ├── loop-orchestrator/              # [Loop Engineering] TypeScript orchestration daemon
│   │   ├── src/
│   │   │   ├── index.ts                # App entrypoint (runs 15-min cron cadences)
│   │   │   ├── state-machine.ts        # Long-running system state & context management
│   │   │   └── budget-controller.ts    # Tracks LLM and API token spend
│   │   └── Dockerfile
│   │
│   ├── gbrain/                         # [GBrain] TS memory engine + Postgres/pgvector interface
│   │   ├── src/
│   │   │   ├── db/                     # Prisma / Drizzle Postgres models
│   │   │   ├── entity-graph.ts         # Links news/whitepapers to founders/protocols
│   │   │   └── memory-synthesizer.ts   # Summarizes historical regimes for Pro-Trader
│   │   └── Dockerfile
│   │
│   ├── pro-trader/                     # [Pro-Trader] LangGraph Python multi-agent pipeline
│   │   ├── agents/
│   │   │   ├── bull_agent.py           # Optimistic tech/fundamental catalyst analyst
│   │   │   ├── bear_agent.py           # Liquidity/hedging risks analyst
│   │   │   ├── regime_agent.py         # DeFi regime & volatility classifier
│   │   │   └── risk_manager.py         # Absolute veto-power risk boundary validator
│   │   ├── graph.py                    # LangGraph workflow compilation
│   │   ├── main.py                     # Fast API server to trigger reasoning loops
│   │   └── requirements.txt
│   │
│   └── dgclaw-skill/                   # [dgclaw-skill] TS Execution Layer calling ACP CLI
│       ├── src/
│       │   ├── execution-listener.ts   # Polls or listens for signed payloads from Pro-Trader
│       │   └── acp-wrapper.ts          # Safe wrapper executing: virtual-acp-cli trade exec
│       └── Dockerfile
│
├── shared/                             # Code and schemas shared across the monorepo
│   ├── schemas/                        # Strict JSON schemas (defined below)
│   │   ├── firecrawl-to-gbrain.json
│   │   ├── gbrain-to-protrader.json
│   │   └── protrader-to-execution.json
│   └── types/                          # Shared TypeScript declarations
│
└── infrastructure/
    └── opensre/                        # [OpenSRE] Infrastructure Guardian configs
        ├── monitors/                   # Custom WebSocket and db heartbeat monitors
        └── runbooks/                   # Automated bash runbooks for system crash recovery
```

---

## 2. Scaffolding & Initial Command Sequences

Run the following shell commands to initialize the monorepo workspace, setup the workspaces structure, and run `loop-init` to set up our orchestration engine:

```bash
# 1. Initialize Monorepo Root
mkdir -p suplock-nexus-monorepo/{apps,services,shared,infrastructure/opensre}
cd suplock-nexus-monorepo
npm init -y

# 2. Configure package.json for monorepo workspaces
npm pkg set workspaces[]="apps/*" workspaces[]="services/*" workspaces[]="shared/*"

# 3. Create Services Directories
mkdir -p services/loop-orchestrator/src
mkdir -p services/gbrain/src
mkdir -p services/pro-trader/agents
mkdir -p services/dgclaw-skill/src
mkdir -p shared/schemas
mkdir -p infrastructure/opensre/{monitors,runbooks}

# 4. Initialize Loop Orchestrator (Loop Engineering)
cd services/loop-orchestrator
npm init -y
npm install typescript @types/node tsx --save-dev
npx tsc --init

# Initialize loop engineering folder structure and configurations
mkdir -p .loop
cat <<EOF > .loop/config.json
{
  "cadence": "*/15 * * * *",
  "budget": {
    "max_daily_token_usd": 15.00,
    "model_limits": {
      "gemini-2.5-pro": 1000000,
      "gpt-4o": 500000
    }
  },
  "persistence": "local-cache"
}
EOF

# 5. Initialize GBrain Node Service
cd ../gbrain
npm init -y
npm install pg dotenv pgvector @types/pg --save

# 6. Set up Pro-Trader (Python / LangGraph)
cd ../pro-trader
python3 -m venv venv
source venv/bin/activate
pip install langgraph langchain-openai fastapi uvicorn pydantic

# Return to root
cd ../../
```

---

## 3. Strict Data Contracts (JSON Schemas)

To guarantee safety, predictability, and full static validation, all data crossing service boundaries conforms to these strict JSON Schemas.

### Contract A: Firecrawl ➔ GBrain (Raw Scrape Ingestion)
*Source: Firecrawl Scraper | Destination: GBrain Entity-Graph Memory Node*

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FirecrawlToGBrainPayload",
  "type": "object",
  "properties": {
    "sourceUrl": { "type": "string", "format": "uri" },
    "scrapedAt": { "type": "string", "format": "date-time" },
    "cleanMarkdown": { "type": "string" },
    "metadata": {
      "type": "object",
      "properties": {
        "title": { "type": "string" },
        "author": { "type": "string" },
        "domain": { "type": "string" }
      },
      "required": ["title"]
    },
    "extractedTokens": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["sourceUrl", "scrapedAt", "cleanMarkdown"]
}
```

### Contract B: GBrain ➔ Pro-Trader (Synthesized Graph Context)
*Source: GBrain Vector Store & SQL Relational Database | Destination: Pro-Trader 9-Agent Debate Input*

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "GBrainToProTraderContext",
  "type": "object",
  "properties": {
    "targetAsset": { "type": "string", "pattern": "^[A-Z0-9]+-USD$" },
    "retrievedAt": { "type": "string", "format": "date-time" },
    "marketSentimentScore": { "type": "number", "minimum": 0, "maximum": 100 },
    "entityGraphRelations": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "nodeA": { "type": "string" },
          "nodeB": { "type": "string" },
          "relationType": { "type": "string" },
          "confidence": { "type": "number" }
        },
        "required": ["nodeA", "nodeB", "relationType"]
      }
    },
    "historicalRegimeType": { 
      "type": "string", 
      "enum": ["HIGH_VOLATILITY_ACCUMULATION", "LOW_VOLATILITY_COMPRESSION", "BULL_TRENDING", "BEAR_DISTRIBUTION"] 
    }
  },
  "required": ["targetAsset", "retrievedAt", "marketSentimentScore", "entityGraphRelations", "historicalRegimeType"]
}
```

### Contract C: Pro-Trader ➔ Execution Layer (Signed Execution Order)
*Source: Pro-Trader Risk Manager Agent (Passed Veto) | Destination: dgclaw-skill ACP CLI*

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ProTraderSignedExecutionOrder",
  "type": "object",
  "properties": {
    "signalId": { "type": "string", "pattern": "^sig_[a-z0-9_]+$" },
    "timestamp": { "type": "string", "format": "date-time" },
    "asset": { "type": "string", "pattern": "^[A-Z0-9]+-USD$" },
    "action": { "type": "string", "enum": ["LONG", "SHORT"] },
    "entryZone": { "type": "number", "minimum": 0 },
    "takeProfit": { "type": "number", "minimum": 0 },
    "stopLoss": { "type": "number", "minimum": 0 },
    "maxSlippage": { "type": "number", "default": 0.005 },
    "positionSizing": {
      "type": "object",
      "properties": {
        "portfolioSizeUsd": { "type": "number" },
        "allocatedRiskPercent": { "type": "number", "maximum": 1.5 },
        "computedMarginUsd": { "type": "number" }
      },
      "required": ["portfolioSizeUsd", "allocatedRiskPercent", "computedMarginUsd"]
    },
    "riskManagerVerificationSignature": { "type": "string" }
  },
  "required": [
    "signalId", 
    "timestamp", 
    "asset", 
    "action", 
    "entryZone", 
    "takeProfit", 
    "stopLoss", 
    "positionSizing", 
    "riskManagerVerificationSignature"
  ]
}
```