import sys
from pathlib import Path

# 1. Setup absolute paths to enable clean imports when running locally outside Docker
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

# Register project root & backend folders
for path in [ROOT_DIR, BASE_DIR]:
    if str(path) not in sys.path:
        sys.path.append(str(path))

# Register agent directory specifically (now located inside backend)
AGENT_DIR = BASE_DIR / "agent"
if str(AGENT_DIR) not in sys.path:
    sys.path.append(str(AGENT_DIR))

# Ensure agent providers subdirectory is in sys.path
PROVIDERS_DIR = AGENT_DIR / "providers"
if str(PROVIDERS_DIR) not in sys.path:
    sys.path.append(str(PROVIDERS_DIR))

# Import FastAPI resources after setting up the sys.path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import local routers
from app.routers import health, chat

app = FastAPI(
    title="Crypto Copilot Chat Backend",
    description="REST API wrapping the LangGraph Cryptocurrency Data Agent and ClickHouse database",
    version="1.0.0"
)

# Enable CORS for flexible development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register Routers
app.include_router(health.router)
app.include_router(chat.router)
