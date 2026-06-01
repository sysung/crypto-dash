import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

import clickhouse_connect
from dotenv import load_dotenv

# Add the local providers folder to sys.path to enable clean absolute imports
providers_dir = str(Path(__file__).resolve().parent / "providers")
if providers_dir not in sys.path:
    sys.path.append(providers_dir)

from providers import BaseLLMProvider, get_provider
from nodes import agent_app
from utils.db import get_clickhouse_client

# Dynamically locate and load the .env file in the root directory
ROOT_ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=ROOT_ENV_PATH)


def run_agent(
    question: str,
    provider: BaseLLMProvider,
    client: clickhouse_connect.driver.Client,
    history: Optional[List[dict]] = None
) -> Optional[str]:
    """
    Orchestrates the 3-Stage Conversational Agentic Planner & Intent Router pipeline.
    Utilizes a LangGraph compiled state machine workflow under the hood.
    """
    # Create the initial state
    initial_state = {
        "question": question,
        "history": history if history is not None else [],
        "thought": "",
        "intent": "",
        "planned_sql": "",
        "sql_results": "",
        "row_count": 0,
        "execution_error": None,
        "response": ""
    }
    
    # Configure a session context thread ID and pass execution dependencies
    config = {
        "configurable": {
            "thread_id": "crypto-agent-repl-thread",
            "provider": provider,
            "db_client": client
        }
    }
    
    try:
        # Invoke the LangGraph workflow
        final_state = agent_app.invoke(initial_state, config)
        
        # Extract and print intermediate results for premium CLI feel
        thought = final_state.get("thought", "No explicit thoughts recorded.")
        intent = final_state.get("intent", "strict_quantitative")
        planned_sql = final_state.get("planned_sql", "NONE")
        execution_error = final_state.get("execution_error")
        row_count = final_state.get("row_count", 0)
        response = final_state.get("response", "")
        
        print(f"🧠 Stage 0 Thought:\n   {thought}")
        print(f"🎯 Stage 0 Intent: {intent.upper()}")
        
        if planned_sql != "NONE":
            print(f"🤖 Stage 1 Generated SQL: {planned_sql}")
            
        if execution_error:
            if "Blocked" in execution_error:
                print(f"🛑 Security Guard Blocked Query: {execution_error}", file=sys.stderr)
            else:
                print(f"❌ ClickHouse Execution Error: {execution_error}", file=sys.stderr)
        elif planned_sql != "NONE":
            print(f"📊 clickhouse-connect Executed (Returned {row_count} rows)")
            
        # Output final synthesized answer
        if intent == "conversational_refusal":
            print("\n✨ Response:")
        else:
            print("\n✨ Synthesized Response:")
            
        print(response)
        print("-" * 50)
        
        # Mutate the caller's history parameter in-place to preserve compatibility
        if history is not None:
            history.clear()
            history.extend(final_state.get("history", []))
            
        return response
        
    except Exception as e:
        print(f"❌ LangGraph Pipeline Execution Failed: {e}", file=sys.stderr)
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Conversational Agentic Planner & Intent Router")
    parser.add_argument(
        "--provider",
        choices=["gemini", "hf", "local"],
        default="hf",
        help="Select the AI provider to run (gemini, hf, or local)"
    )
    parser.add_argument(
        "--question", "-q",
        type=str,
        help="Run the agent on a single custom question"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run the evaluation suite of standard questions"
    )
    parsed_args, _ = parser.parse_known_args()

    # Initialize and validate the requested provider
    try:
        provider_instance = get_provider(parsed_args.provider)
    except Exception as e:
        print(f"❌ Booting Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"🚀 Booting 3-Stage Conversational Agentic Planner (Provider: {parsed_args.provider.upper()})...")

    # Establish dynamic, lazy connection to ClickHouse database
    db_client = get_clickhouse_client()

    if parsed_args.question:
        # User specified a single custom question
        run_agent(parsed_args.question, provider_instance, db_client)

    elif parsed_args.test:
        # User wants to run the evaluation suite
        test_questions = [
            # --- Category 1: Market Trends & Volumetrics ---
            "Summarize the overall price trend of ETH-USD over the last hour using 1-minute OHLCV candles. Highlight any interesting momentum.",
            "Did the volume for ETH-USD spike over the last hour, or is the market quiet?",
            "Identify the hourly price returns and volatility trend for BTC-USD over the last 24 hours. Is the risk rising or stabilizing?",
            "What was the highest price of BTC-USD recorded in the 1-minute OHLCV table so far?",

            # --- Category 2: Market Anomalies (Spikes, Decoupling, & Drawdowns) ---
            "Is SOL-USD experiencing a volume spike right now? Give me the exact ratio compared to the last month.",
            "Analyze all streaming tokens to find the most anomalous volume spike recorded in our spikes view today. Which token experienced it?",
            "Are there any assets currently decoupled from Bitcoin? Show me altcoins whose rolling 24-hour beta is near or below zero, indicating an interesting anomaly.",
            "Summarize the worst drawdowns currently happening across all active tokens. Which asset has the most anomalous drop from its historical peak?",
            "Show me the current drawdown and 30-day rolling max drawdown for ETH-USD.",

            # --- Category 3: Causal Analysis (Explaining Anomalous Market Events) ---
            "If BTC-USD is experiencing a sudden price drop or drawdown, explain how its core utility as digital gold and consensus design might justify its status as a relative safe haven compared to altcoins.",
            "Analyze why an altcoin like SOL-USD might experience extreme volume spikes or volatility. Explain this behavior in relation to its underlying technology, utility, and security facts.",
            "DOGE-USD is historically known for meme-driven anomalous runs. Explain if we see any sudden decoupling (hourly beta < 0.5) from BTC today, and describe how its meme utility explains this volatile behavior.",
            "Review the current maximum drawdowns across ETH-USD and BTC-USD. Explain why Ethereum's drawdown profile differs from Bitcoin's based on their different utility sectors (Smart Contracts vs. Digital Gold).",

            # --- Category 4: Speculative & Out-of-Scope Queries (CoT Planner & Refusals) ---
            "Which coin will make me a millionaire the fastest? Explain your reasoning",
            "Write a poem about Bitcoin's historical block size wars.",
            "What is the capital city of France?"
        ]
        for q in test_questions:
            run_agent(q, provider_instance, db_client)

        print("\n🧠 Running Multi-Turn Memory Verification Suite...")
        test_history = []
        multi_turn_questions = [
            "What is the current drawdown and 30-day rolling max drawdown for ETH-USD?",
            "What about SOL-USD?",
            "Compare the utility of these two tokens that we just discussed."
        ]
        for q in multi_turn_questions:
            run_agent(q, provider_instance, db_client, history=test_history)

    else:
        # Launch beautiful, premium interactive REPL session with slash commands
        print("\n✨ Entered Interactive REPL Mode. Type your question and press Enter.")
        print("Special Commands:")
        print("  /clear          - Clear conversation memory")
        print("  /history        - View active session history")
        print("  /save <file>    - Save active session history to a JSON file")
        print("  /load <file>    - Load a session history from a JSON file")
        print("  /help           - Show this help menu")
        print("Type 'exit', 'quit', or Ctrl+C to terminate the session.\n")
        
        history = []
        try:
            while True:
                turn_count = len(history) // 2
                prompt_suffix = f" [Memory: {turn_count} turns]" if turn_count > 0 else ""
                user_input = input(f"💡 Ask a question{prompt_suffix}: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ["exit", "quit"]:
                    print("👋 Goodbye!")
                    break
                
                if user_input.startswith("/"):
                    cmd_parts = user_input.split(maxsplit=1)
                    cmd = cmd_parts[0].lower()
                    arg = cmd_parts[1].strip() if len(cmd_parts) > 1 else ""
                    
                    if cmd == "/clear":
                        history.clear()
                        # Also clear the LangGraph checkpointer memory by invoking with empty history
                        config = {"configurable": {"thread_id": "crypto-agent-repl-thread"}}
                        agent_app.update_state(config, {"history": []})
                        print("🧹 Conversational memory cleared!")
                        continue
                    elif cmd == "/history":
                        if not history:
                            print("📭 Conversational memory is empty.")
                        else:
                            print("\n📜 Active Session History:")
                            for turn in history:
                                role = "🗣️ User" if turn["role"] == "user" else "✨ Assistant"
                                print(f"{role}: {turn['content']}\n")
                        continue
                    elif cmd == "/help":
                        print("\n🛠️ Available Commands:")
                        print("  /clear          - Clear conversation memory")
                        print("  /history        - View active session history")
                        print("  /save <file>    - Save active session history to a JSON file")
                        print("  /load <file>    - Load a session history from a JSON file")
                        print("  /help           - Show this help menu")
                        continue
                    elif cmd == "/save":
                        if not arg:
                            print("❌ Please specify a filename, e.g., /save my_session.json")
                            continue
                        filepath = Path(arg)
                        if filepath.suffix != ".json":
                            filepath = filepath.with_suffix(".json")
                        try:
                            import json
                            with open(filepath, "w") as f:
                                json.dump(history, f, indent=2)
                            print(f"💾 Session history saved successfully to {filepath}")
                        except Exception as e:
                            print(f"❌ Failed to save session: {e}", file=sys.stderr)
                        continue
                    elif cmd == "/load":
                        if not arg:
                            print("❌ Please specify a filename to load, e.g., /load my_session.json")
                            continue
                        filepath = Path(arg)
                        if filepath.suffix != ".json":
                            filepath = filepath.with_suffix(".json")
                        if not filepath.exists():
                            print(f"❌ File not found: {filepath}")
                            continue
                        try:
                            import json
                            with open(filepath, "r") as f:
                                loaded_history = json.load(f)
                            if isinstance(loaded_history, list) and all(isinstance(t, dict) and "role" in t and "content" in t for t in loaded_history):
                                history[:] = loaded_history
                                # Also update LangGraph checkpointer memory
                                config = {"configurable": {"thread_id": "crypto-agent-repl-thread"}}
                                agent_app.update_state(config, {"history": history})
                                print(f"📂 Loaded session history from {filepath} ({len(history) // 2} turns)")
                            else:
                                print("❌ Invalid session history file format.")
                        except Exception as e:
                            print(f"❌ Failed to load session: {e}", file=sys.stderr)
                        continue
                    else:
                        print(f"❓ Unknown command: {cmd}. Type /help for assistance.")
                        continue
                
                try:
                    run_agent(user_input, provider_instance, db_client, history=history)
                except KeyboardInterrupt:
                    print("\n⚠️ Query cancelled by user.")
                except Exception as e:
                    print(f"❌ Error executing agent: {e}", file=sys.stderr)
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Goodbye!")