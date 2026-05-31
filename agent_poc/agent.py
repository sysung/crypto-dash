import os
import re
import sys
import argparse
from pathlib import Path
from typing import Tuple, List, Optional
import clickhouse_connect
from dotenv import load_dotenv

# Add the local providers folder to sys.path to enable clean absolute imports
providers_dir = str(Path(__file__).resolve().parent / "providers")
if providers_dir not in sys.path:
    sys.path.append(providers_dir)

from providers import get_provider, BaseLLMProvider

# 1. Dynamically locate and load the .env file in the root directory
ROOT_ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=ROOT_ENV_PATH)


def get_clickhouse_client() -> clickhouse_connect.driver.Client:
    """
    Initializes and returns a connection to the ClickHouse database.
    Configuration values are fetched from environment variables with safe defaults.
    """
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "password123")

    try:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password
        )
    except Exception as e:
        print(f"❌ ClickHouse Database Connection Failed: {e}", file=sys.stderr)
        sys.exit(1)


# 2. Define Stage 1: The SQL Router Prompt (PEP 8 Uppercase Constant)
ROUTER_INSTRUCTION = """
You are a specialized SQL Router Agent for a real-time cryptocurrency pipeline.
Your only job is to convert the user's natural language question into a valid, read-only ClickHouse SQL query targeting our tables and views.

The database has the following tables and views:

1. 'default.crypto_ticks_raw' stores high-frequency individual trade ticker events.
Columns:
- symbol (String): Crypto token pair, e.g. 'BTC-USD', 'ETH-USD', 'SOL-USD'
- price (Float64): Tick price in USD
- volume_24h (Float64): Rolling 24-hour trading volume
- best_bid (Float64), best_ask (Float64)
- best_bid_quantity (Float64), best_ask_quantity (Float64)
- low_24h (Float64), high_24h (Float64), low_52w (Float64), high_52w (Float64)
- price_percent_chg_24h (Float64)
- sequence_num (Int64)
- timestamp (String): ISO timestamp string
- server_time (DateTime64(9)): Parsed server envelope time.

2. 'default.crypto_l2_raw' stores flattened L2 order book depth updates.
Columns:
- symbol (String)
- side (String): 'bid' or 'offer' (meaning ask)
- price (Float64)
- volume (Float64): Size/quantity at this price level
- event_time (String)
- sequence_num (Int64)
- timestamp (String)
- trade_time (DateTime64(6)): Parsed event trade time. Highly optimized for sorting and partition pruning.
- server_time (DateTime64(9)): Parsed server envelope time.

3. 'default.crypto_ohlcv_1m', 'default.crypto_ohlcv_5m', 'default.crypto_ohlcv_24h' (OHLCV aggregated target tables)
Columns:
- symbol (String)
- window_start (DateTime)
- open (Float64), high (Float64), low (Float64), close (Float64), volume (Float64)

4. 'default.view_volume_spikes' (View detecting 5-minute volume spikes relative to a rolling 30-day average baseline)
Columns:
- symbol (String)
- window_start (DateTime)
- volume (Float64)
- rolling_30d_avg_volume (Float64)
- volume_spike_ratio (Float64)

5. 'default.view_risk_and_volatility' (View tracking hourly volatility, current drawdown, and rolling 30-day max drawdown)
Columns:
- symbol (String)
- window_start (DateTime)
- close (Float64)
- rolling_30d_volatility_hourly (Float64)
- current_drawdown (Float64)
- rolling_30d_max_drawdown (Float64)

6. 'default.view_altcoin_beta' (View calculating hourly altcoin beta relative to BTC-USD over rolling 24-hour windows)
Columns:
- symbol (String)
- window_start (DateTime)
- r_alt (Float64)
- r_btc (Float64)
- covariance_24h (Float64)
- variance_btc_24h (Float64)
- hourly_beta_24h (Float64)

7. 'default.token_metadata' (Static table with structural coin facts)
Columns:
- symbol (String), name (String), utility (String), consensus_mechanism (String), security_checklist (String), launched_year (UInt16)

Rules:
1. Return ONLY the raw SQL query. Do not include markdown formatting like ```sql or ```.
2. Do not include any conversational text, explanations, or pleasantries.
3. Always search or filter for specific symbols exactly as provided (e.g., 'BTC-USD').
4. For volume spike checks, query 'default.view_volume_spikes'.
5. For volatility, drawdowns, and maximum drawdowns, query 'default.view_risk_and_volatility'.
6. For asset beta or correlation relative to Bitcoin, query 'default.view_altcoin_beta'.
7. For structural token facts (utility, consensus, audits), query 'default.token_metadata'.
8. For standard price charts, candle bars, or Open/High/Low/Close metrics, query the appropriate OHLCV table ('default.crypto_ohlcv_1m', 'default.crypto_ohlcv_5m', or 'default.crypto_ohlcv_24h').
9. For hybrid queries combining structural token facts (utility, consensus) and live risk/drawdown metrics, perform a JOIN on the `symbol` column. Make sure that metrics columns like `close`, `window_start`, and `rolling_30d_max_drawdown` are selected from the risk view subquery/alias, and NOT from the static `token_metadata` table.
Example hybrid query format:
SELECT 
    m.symbol,
    m.utility,
    m.consensus_mechanism,
    r.close,
    r.rolling_30d_max_drawdown
FROM default.token_metadata AS m
JOIN (
    SELECT *
    FROM default.view_risk_and_volatility
    WHERE symbol = 'BTC-USD'
    ORDER BY window_start DESC
    LIMIT 1
) AS r ON m.symbol = r.symbol
WHERE m.symbol = 'BTC-USD';
"""

# 3. Define Stage 2: The Insights Responder Prompt (PEP 8 Uppercase Constant)
RESPONDER_INSTRUCTION = """
You are a specialized Cryptocurrency Insights Assistant.
Your job is to synthesize raw data queries returned from a ClickHouse database into a clean, grounded, dashboard-friendly conversational response.

You will receive:
1. The user's original natural-language question.
2. The exact SQL query that was executed.
3. The raw rows/metrics returned from ClickHouse.

Guidelines:
1. Synthesize the results into a concise, professional, and visually engaging response.
2. Ground all numbers and statements strictly in the database results. Do not hallucinate or assume values.
3. If no rows were returned, politely inform the user that no data is currently available in the database for their query.
4. Format your output cleanly in Markdown, using bullet points, bolding, or lists where appropriate to make it highly readable.
5. If the user asked a complex analytical question (like volatility or beta), briefly explain the financial context of the returned metric in one sentence.
"""


def is_query_safe(sql: str) -> Tuple[bool, str]:
    """
    Validates that the generated SQL query is read-only and safe for execution.
    Strips single-line and multi-line SQL comments and checks for forbidden keywords.
    """
    cleaned_sql = re.sub(r"--.*", "", sql)
    cleaned_sql = re.sub(r"/\*.*?\*/", "", cleaned_sql, flags=re.DOTALL)
    cleaned_sql = cleaned_sql.strip()

    if not cleaned_sql:
        return False, "Query is empty."

    upper_sql = cleaned_sql.upper()

    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        return False, "Query must start with SELECT or WITH."

    forbidden_patterns = [
        r"\bDROP\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bCREATE\b",
        r"\bREPLACE\b"
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, upper_sql):
            keyword = pattern.replace(r"\b", "")
            return False, f"Forbidden SQL keyword detected: {keyword}"

    return True, ""


def run_agent(question: str, provider: BaseLLMProvider, client: clickhouse_connect.driver.Client) -> None:
    """
    Orchestrates the 2-Stage translation and response synthesis pipeline.
    Translates natural language questions to read-only ClickHouse SQL, executes it,
    and feeds the results back to the LLM to compile natural language insights.
    """
    print(f"\n🗣️ Question: {question}")

    # --- STAGE 1: SQL ROUTING & TRANSLATION ---
    try:
        raw_text = provider.generate(ROUTER_INSTRUCTION, question)
        # Extract SQL from markdown blocks if present
        match = re.search(r"```(?:sql)?\n?(.*?)\n?```", raw_text, re.IGNORECASE | re.DOTALL)
        generated_sql = match.group(1).strip() if match else raw_text
        print(f"🤖 Stage 1 Generated SQL: {generated_sql}")

    except Exception as e:
        print(f"❌ Stage 1 Router Failed to Translate: {e}", file=sys.stderr)
        return

    # Verify query safety before running it
    is_safe, error_msg = is_query_safe(generated_sql)
    if not is_safe:
        print(f"🛑 Security Guard Blocked Query: {error_msg}", file=sys.stderr)
        return

    # --- INTERMEDIATE: DATABASE EXECUTION ---
    try:
        result = client.query(generated_sql)
        raw_results = []

        # Hardened with maximum row count limit protection (Context Protection)
        MAX_ROWS = 50
        rows = list(result.named_results())
        row_count = len(rows)

        for idx, row in enumerate(rows):
            if idx >= MAX_ROWS:
                raw_results.append(f"... [Truncated: {row_count - MAX_ROWS} additional rows returned]")
                break
            raw_results.append(str(row))

        results_str = "\n".join(raw_results)
        print(f"📊 clickhouse-connect Executed (Returned {row_count} rows)")

    except Exception as e:
        print(f"❌ ClickHouse Execution Error: {e}", file=sys.stderr)
        return

    # --- STAGE 2: INSIGHTS RESPONDER & SYNTHESIS ---
    try:
        # Prompt construction for Stage 2
        responder_prompt = f"""
User Question: {question}
SQL Executed: {generated_sql}
Database Results:
{results_str if results_str else 'No rows returned.'}
"""
        synthesized_answer = provider.generate(RESPONDER_INSTRUCTION, responder_prompt)

        print("\n✨ Synthesized Response:")
        print(synthesized_answer)
        print("-" * 50)

    except Exception as e:
        print(f"❌ Stage 2 Response Synthesis Failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="2-Stage Crypto Data Agent")
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

    print(f"🚀 Booting 2-Stage Data Agent (Provider: {parsed_args.provider.upper()})...")

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
            "Review the current maximum drawdowns across ETH-USD and BTC-USD. Explain why Ethereum's drawdown profile differs from Bitcoin's based on their different utility sectors (Smart Contracts vs. Digital Gold)."
        ]
        for q in test_questions:
            run_agent(q, provider_instance, db_client)

    else:
        # Launch beautiful, premium interactive REPL session
        print("\n✨ Entered Interactive REPL Mode. Type your question and press Enter.")
        print("Type 'exit', 'quit', or Ctrl+C to terminate the session.\n")
        try:
            while True:
                user_input = input("💡 Ask a question: ").strip()
                if not user_input:
                    continue
                if user_input.lower() in ["exit", "quit"]:
                    print("👋 Goodbye!")
                    break
                try:
                    run_agent(user_input, provider_instance, db_client)
                except KeyboardInterrupt:
                    print("\n⚠️ Query cancelled by user.")
                except Exception as e:
                    print(f"❌ Error executing agent: {e}", file=sys.stderr)
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Goodbye!")