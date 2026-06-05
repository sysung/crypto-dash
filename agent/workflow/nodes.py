import re
from typing import Any, Dict, Optional

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from prompts import PLANNER_INSTRUCTION, RESPONDER_INSTRUCTION
from workflow.state import AgentState
from utils.security import is_query_safe


# --- LANGGRAPH NODE FUNCTIONS ---

def planner_node(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """
    Stage 0 Node: Evaluates intent, reasons via Chain-of-Thought, and plans database SQL query.
    """
    question = state["question"]
    
    configurable = config.get("configurable", {})
    provider = configurable.get("provider")
    
    planner_prompt = f"Current Question: {question}"
    
    try:
        raw_planner_text = provider.generate(PLANNER_INSTRUCTION, planner_prompt)
        
        thought_match = re.search(r"Thought:\s*(.*?)(?=\bIntent:|$)", raw_planner_text, re.DOTALL | re.IGNORECASE)
        intent_match = re.search(r"Intent:\s*(.*?)(?=\bPlanned SQL:|$)", raw_planner_text, re.DOTALL | re.IGNORECASE)
        sql_match = re.search(r"Planned SQL:\s*(.*)", raw_planner_text, re.DOTALL | re.IGNORECASE)

        thought = thought_match.group(1).strip() if thought_match else "No explicit thoughts recorded."
        intent = intent_match.group(1).strip().lower() if intent_match else "strict_quantitative"
        planned_sql = sql_match.group(1).strip() if sql_match else "NONE"

        sql_block_match = re.search(r"```(?:sql)?\n?(.*?)\n?```", planned_sql, re.IGNORECASE | re.DOTALL)
        if sql_block_match:
            planned_sql = sql_block_match.group(1).strip()
            
        return {
            "thought": thought,
            "intent": intent,
            "planned_sql": planned_sql,
            "execution_error": None
        }
    except Exception as e:
        return {
            "thought": "Error occurred during planning.",
            "intent": "conversational_refusal",
            "planned_sql": "NONE",
            "execution_error": f"Stage 0 Intent Planner Failed: {e}"
        }


def refusal_node(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """
    Stage 1 Fallback Node: Synthesizes a friendly out-of-scope refusal.
    """
    question = state["question"]
    
    configurable = config.get("configurable", {})
    provider = configurable.get("provider")
    
    refusal_instruction = """
You are a specialized Cryptocurrency Data Agent.
The user has asked a question that is completely out of scope (unrelated to cryptocurrency, or completely unrelated to our ClickHouse streaming dataset).
Politely refuse to answer, and clearly explain that you only have access to real-time Coinbase streaming metrics (OHLCV, order book depth, volatility, volume spikes, and altcoin beta) and structural token metadata facts. Do not answer general questions (e.g. tell a joke, write a poem, explain historical facts outside our token metadata).
"""
    try:
        synthesized_answer = provider.generate(refusal_instruction, question)
    except Exception as e:
        synthesized_answer = f"I am unable to complete this request because the provider failed: {e}"
        
    return {
        "response": synthesized_answer
    }


def db_executor_node(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """
    Stage 1 Node: Performs comment-stripping SQL safety validation and runs query on ClickHouse.
    """
    planned_sql = state["planned_sql"]
    
    configurable = config.get("configurable", {})
    client = configurable.get("db_client")
    
    if planned_sql == "NONE" or not planned_sql:
        return {
            "sql_results": "No query executed.",
            "row_count": 0,
            "execution_error": "No query planned."
        }
        
    is_safe, error_msg = is_query_safe(planned_sql)
    if not is_safe:
        return {
            "sql_results": "Blocked by safety guard.",
            "row_count": 0,
            "execution_error": f"Security Guard Blocked Query: {error_msg}"
        }
        
    try:
        result = client.query(planned_sql)
        raw_results = []

        MAX_ROWS = 50
        rows = list(result.named_results())
        row_count = len(rows)

        for idx, row in enumerate(rows):
            if idx >= MAX_ROWS:
                raw_results.append(f"... [Truncated: {row_count - MAX_ROWS} additional rows returned]")
                break
            raw_results.append(str(row))

        results_str = "\n".join(raw_results)
        return {
            "sql_results": results_str,
            "row_count": row_count,
            "execution_error": None
        }
    except Exception as e:
        return {
            "sql_results": f"Failed to execute query: {e}",
            "row_count": 0,
            "execution_error": f"ClickHouse Execution Error: {e}"
        }


def insights_responder_node(state: AgentState, config: RunnableConfig) -> Dict[str, Any]:
    """
    Stage 2 Node: Synthesizes database results into a beautiful, grounded markdown answer.
    """
    question = state["question"]
    thought = state["thought"]
    planned_sql = state["planned_sql"]
    sql_results = state["sql_results"]
    execution_error = state["execution_error"]
    
    configurable = config.get("configurable", {})
    provider = configurable.get("provider")
    
    if execution_error:
        error_response = f"I encountered an issue executing your request: {execution_error}"
        return {
            "response": error_response
        }
        
    responder_prompt = f"""Current User Question: {question}
Planner CoT Thought: {thought}
SQL Executed: {planned_sql}
Database Results:
{sql_results if sql_results else 'No rows returned.'}
"""
    try:
        synthesized_answer = provider.generate(RESPONDER_INSTRUCTION, responder_prompt)
    except Exception as e:
        synthesized_answer = f"Failed to synthesize response: {e}"
        
    return {
        "response": synthesized_answer
    }


# --- LANGGRAPH WORKFLOW SETUP & COMPILATION ---

def route_intent(state: AgentState) -> str:
    """Routes the workflow based on Stage 0 Planner intent classification."""
    intent = state.get("intent", "strict_quantitative").strip().lower()
    if intent == "conversational_refusal":
        return "refusal"
    return "executor"


workflow = StateGraph(AgentState)

# Add Nodes
workflow.add_node("planner", planner_node)
workflow.add_node("refusal", refusal_node)
workflow.add_node("executor", db_executor_node)
workflow.add_node("responder", insights_responder_node)

# Set Entry Point
workflow.add_edge(START, "planner")

# Set Conditional Routing after Planner
workflow.add_conditional_edges(
    "planner",
    route_intent,
    {
        "refusal": "refusal",
        "executor": "executor"
    }
)

# Connect Executor node directly to Responder node
workflow.add_edge("executor", "responder")

# Connect end nodes to the END marker
workflow.add_edge("refusal", END)
workflow.add_edge("responder", END)

# Compile LangGraph App statelessly
agent_app = workflow.compile()
