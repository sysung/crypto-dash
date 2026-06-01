from typing import List, Optional, Dict
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., description="The user's query or prompt")
    history: Optional[List[Dict[str, str]]] = Field(
        default=None, 
        description="Optional list of conversation history, e.g. [{'role': 'user', 'content': '...'}]"
    )
    thread_id: Optional[str] = Field(
        default=None, 
        description="Optional thread ID to preserve session memory checkpointer context"
    )


class ChatResponse(BaseModel):
    response: str = Field(..., description="The final synthesized markdown response")
    thought: str = Field(..., description="Stage 0 chain-of-thought planner reasoning")
    intent: str = Field(..., description="The classified intent of the user request")
    planned_sql: str = Field(..., description="The planned ClickHouse SQL query (if any)")
    row_count: int = Field(..., description="Number of database rows returned")
    execution_error: Optional[str] = Field(default=None, description="Any database execution error details")
    thread_id: str = Field(..., description="The session thread ID")
