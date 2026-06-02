import uuid
from fastapi import APIRouter, HTTPException, status

from app.database import get_db_client
from app.models import ChatRequest, ChatResponse
from app import config

# Agent imports from the renamed agent folder
from agent.providers import get_provider
from agent.nodes import agent_app

router = APIRouter()


@router.post("/api/chat", response_model=ChatResponse)
def run_chat_query(request: ChatRequest) -> ChatResponse:
    """
    Orchestrates the agent flow: evaluates intent, plans SQL, runs on ClickHouse, and synthesizes markdown.
    Ensures safe database extraction and standardizes HTTP 503 payloads for offline states.
    """
    # 1. Establish DB Client (raises 503 error automatically if unreachable)
    db_client = get_db_client()
    
    # 2. Determine LLM Provider (defaulting to 'hf')
    requested_provider = request.provider or "hf"
    if requested_provider.lower() in ("huggingface", "hf"):
        provider_name = "hf"
    elif requested_provider.lower() == "gemini":
        provider_name = "gemini"
    else:
        provider_name = requested_provider

    try:
        provider_instance = get_provider(provider_name)
    except Exception as e:
        # Fallback to the alternate provider if the requested one fails
        fallback_name = "gemini" if provider_name == "hf" else "hf"
        try:
            provider_instance = get_provider(fallback_name)
        except Exception as fallback_err:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "LLM Provider Initialization Failure",
                    "details": f"Failed to load provider '{provider_name}': {str(e)}. Fallback '{fallback_name}' also failed: {str(fallback_err)}"
                }
            )
            
    # 3. Setup Session/Thread config
    thread_id = request.thread_id or f"session_{uuid.uuid4().hex}"
    run_config = {
        "configurable": {
            "thread_id": thread_id,
            "provider": provider_instance,
            "db_client": db_client
        }
    }
    
    # 4. Formulate LangGraph State & Invoke
    initial_state = {
        "question": request.question,
        "history": request.history if request.history is not None else [],
        "thought": "",
        "intent": "",
        "planned_sql": "",
        "sql_results": "",
        "row_count": 0,
        "execution_error": None,
        "response": ""
    }
    
    try:
        final_state = agent_app.invoke(initial_state, run_config)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "LangGraph Execution Failure",
                "details": f"The state machine failed during compilation/execution: {str(e)}"
            }
        )
        
    return ChatResponse(
        response=final_state.get("response", ""),
        thought=final_state.get("thought", ""),
        intent=final_state.get("intent", "strict_quantitative"),
        planned_sql=final_state.get("planned_sql", "NONE"),
        row_count=final_state.get("row_count", 0),
        execution_error=final_state.get("execution_error"),
        thread_id=thread_id
    )
