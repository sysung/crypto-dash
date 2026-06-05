import os
import sys
import argparse
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


# 1. Setup absolute paths to enable clean imports when running from any Cwd
AGENT_DIR = Path(__file__).resolve().parent
if str(AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_DIR))

# 2. Load environment variables from the project root .env
ROOT_DIR = AGENT_DIR.parent
env_path = ROOT_DIR / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

# Import LangGraph and Provider resources after setting up the sys.path
from hf import HFProvider
from workflow.nodes import agent_app
from utils.db import get_clickhouse_client



def run_query(question: str, model: Optional[str] = None):
    """
    Orchestrates the agent flow: evaluates intent, plans SQL, runs on ClickHouse, and synthesizes markdown.
    """
    # 1. Establish DB Client (exits on failure)
    db_client = get_clickhouse_client()
    
    # 2. Initialize LLM Provider (Hugging Face)
    try:
        provider_instance = HFProvider(model_id=model)
    except Exception as e:
        print(f"❌ LLM Provider Initialization Failure: {e}", file=sys.stderr)
        sys.exit(1)
            
    # 3. Setup Config
    run_config = {
        "configurable": {
            "provider": provider_instance,
            "db_client": db_client
        }
    }
    
    # 4. Formulate LangGraph State & Invoke (stateless)
    initial_state = {
        "question": question,
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
        print(f"❌ LangGraph Execution Failure: {e}", file=sys.stderr)
        sys.exit(1)
        
    # 5. Format & Print Results
    print("=" * 80)
    print("🧠 Intent Classifier:")
    print(f"  {final_state.get('intent', 'UNKNOWN')}")
    print("-" * 80)
    print("💭 Chain-of-Thought (Thought):")
    print(f"  {final_state.get('thought', 'No explicit thoughts recorded.')}")
    print("-" * 80)
    print("🔌 Executed SQL Query:")
    print(f"  {final_state.get('planned_sql', 'NONE')}")
    print("-" * 80)
    print(f"📊 Rows Returned: {final_state.get('row_count', 0)}")
    if final_state.get("execution_error"):
        print(f"  ❌ Execution Error: {final_state.get('execution_error')}")
    elif final_state.get("sql_results"):
        print("  Database Results Snippet:")
        # Indent results slightly
        indented_results = "\n".join(f"    {line}" for line in final_state.get("sql_results", "").splitlines())
        print(indented_results)
    else:
        print("  No rows returned or query bypassed.")
    print("-" * 80)
    print("✨ Synthesized Markdown Response:")
    print(final_state.get("response", ""))
    print("=" * 80)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Conversational Copilot AI Agent CLI")
        parser.add_argument(
            "-q", "--query",
            type=str,
            required=True,
            help="Natural language question to ask the agent"
        )
        parser.add_argument(
            "-m", "--model",
            type=str,
            default=None,
            help="Override default Hugging Face model ID"
        )
        
        args = parser.parse_args()
        run_query(args.query, args.model)
    except KeyboardInterrupt:
        print("\n⚠️ Query execution interrupted by user. Exiting...", file=sys.stderr)
        sys.exit(130)

