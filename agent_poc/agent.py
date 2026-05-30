import os
import re
from pathlib import Path
import clickhouse_connect
import google.generativeai as genai
from dotenv import load_dotenv

# 1. Dynamically locate the .env file in the root directory
# Path(__file__).resolve().parent gets the 'agent_poc' folder
# .parent goes up one level to 'crypto-streaming-pipeline'
root_env_path = Path(__file__).resolve().parent.parent / '.env'

# Load the API key from that specific file path
load_dotenv(dotenv_path=root_env_path)
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# 2. Connect to your local ClickHouse container
try:
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123, 
        username='default', 
        password='password123'
    )
except Exception as e:
    print(f"Database Connection Failed: {e}")
    exit(1)

# 3. Define the Agent's Brain (System Prompt & Schema)
system_instruction = """
You are a specialized data analytics agent for a real-time cryptocurrency pipeline.
Your only job is to convert the user's question into a valid ClickHouse SQL query.

The database has a table named 'default.crypto_trades_raw' that stores individual trade events.
Here is the schema:
- symbol (String): The crypto currency token pair, e.g., 'BTC-USD', 'ETH-USD', 'SOL-USD'
- price (Float64): The transaction price in USD
- volume (Float64): The trade transaction size/quantity
- timestamp (Int64): Epoch millisecond timestamp of the trade
- trade_time (DateTime): The parsed trade time. Highly optimized for partition pruning, sorting, and time grouping.

Rules:
1. Return ONLY the raw SQL query. Do not include markdown formatting like ```sql or ```.
2. Do not include any conversational text, explanations, or pleasantries.
3. Always search or filter for specific symbols exactly as provided (e.g., 'BTC-USD').
4. A single row in this table represents one transaction/trade event. To find the transaction count, use `count()`.
5. For temporal filtering or grouping, prefer using the `trade_time` column (e.g., `toStartOfSecond(trade_time)` or `toStartOfMinute(trade_time)`).
"""

# Initialize the ultra-fast Gemini 1.5 Flash model
model = genai.GenerativeModel(
    model_name="gemini-3.5-flash",
    system_instruction=system_instruction
)

def run_agent(question: str):
    print(f"\n🗣️ Question: {question}")
    
    # Ask Gemini to translate the question into SQL
    response = model.generate_content(question)
    
    # Strip any accidental markdown formatting the LLM might have included
    generated_sql = re.sub(r"```sql\n|```\n?", "", response.text).strip()
    print(f"🤖 Generated SQL: {generated_sql}")
    
    # Execute the query against ClickHouse
    try:
        result = client.query(generated_sql)
        
        # Format the output beautifully
        print("📊 Results:")
        for row in result.named_results():
            print(f"   {row}")
            
    except Exception as e:
        print(f"❌ ClickHouse Execution Error: {e}")

# --- Test the POC ---
if __name__ == "__main__":
    print("🚀 Booting Data Agent...")
    
    # You can change these questions to test different SQL generation!
    test_questions = [
        "What was the highest price of BTC-USD recorded in the table so far?",
        "How many total transactions have we seen across all coins combined?",
        "What are the unique symbols currently in the database?"
    ]
    
    for q in test_questions:
        run_agent(q)