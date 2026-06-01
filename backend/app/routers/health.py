import os
from typing import Dict, Any
from fastapi import APIRouter, Response, HTTPException, status

from app.database import get_db_client
from app import config

# We need to ensure agent provider is importable
from agent.providers import get_provider

router = APIRouter()


@router.get("/api/health")
def health_check(response: Response) -> Dict[str, Any]:
    """
    Validates general service status, checking ClickHouse and LLM provider connectivity.
    Returns HTTP 503 if any dependency is offline.
    """
    db_status = "connected"
    db_details = None
    try:
        client = get_db_client()
        client.command("SELECT 1")
    except HTTPException as e:
        db_status = "disconnected"
        db_details = e.detail
    except Exception as e:
        db_status = "disconnected"
        db_details = str(e)
        
    provider_status = "available"
    provider_details = None
    try:
        get_provider(config.AI_PROVIDER)
    except Exception as e:
        # Fallback check
        fallback_name = "gemini" if config.GEMINI_API_KEY else "hf"
        try:
            get_provider(fallback_name)
        except Exception as fallback_err:
            provider_status = "unavailable"
            provider_details = f"Default '{config.AI_PROVIDER}' failed: {e}. Fallback '{fallback_name}' also failed: {fallback_err}"
        
    status_code = status.HTTP_200_OK
    if db_status == "disconnected" or provider_status == "unavailable":
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        response.status_code = status_code
        
    return {
        "status": "healthy" if status_code == status.HTTP_200_OK else "unhealthy",
        "database": {
            "status": db_status,
            "details": db_details
        },
        "provider": {
            "status": provider_status,
            "details": provider_details
        }
    }
