import sys
import json
import time
from typing import Dict, Any

try:
    import requests
except ImportError:
    print("❌ Testing requires the 'requests' library. Please run: pip install requests")
    sys.exit(1)

BASE_URL = "http://localhost:8000"


def print_json(data: Dict[str, Any]):
    print(json.dumps(data, indent=2))


def test_health():
    print("\n--- 🏥 Testing GET /api/health ---")
    try:
        start_time = time.time()
        response = requests.get(f"{BASE_URL}/api/health")
        duration = time.time() - start_time
        
        print(f"Status Code: {response.status_code} ({duration:.2f}s)")
        print_json(response.json())
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Failed to reach /api/health: {e}")
        return False


def test_chat(question: str, history=None, provider: str = None) -> Dict[str, Any]:
    print(f"\n--- 🗣️ Testing POST /api/chat (Provider: {provider or 'default'}) ---")
    print(f"Question: \"{question}\"")
    
    payload = {
        "question": question,
        "history": history or []
    }
    if provider:
        payload["provider"] = provider
    
    try:
        start_time = time.time()
        response = requests.post(f"{BASE_URL}/api/chat", json=payload)
        duration = time.time() - start_time
        
        print(f"Status Code: {response.status_code} ({duration:.2f}s)")
        
        if response.status_code != 200:
            print("❌ Request failed with error:")
            print_json(response.json())
            return None
            
        data = response.json()
        print("\n🧠 Diagnostics:")
        print(f"  Intent: {data.get('intent')}")
        print(f"  Thought: {data.get('thought')}")
        print(f"  Planned SQL: {data.get('planned_sql')}")
        print(f"  Rows Returned: {data.get('row_count')}")
        print(f"  Error: {data.get('execution_error')}")
        
        print("\n✨ Assistant Response:")
        print(data.get("response"))
        print("-" * 50)
        
        return data
    except Exception as e:
        print(f"❌ Failed to reach /api/chat: {e}")
        return None


if __name__ == "__main__":
    print("🚀 Starting FastAPI Chat Copilot API Verification Test Suite...")
    
    # 1. Run Healthcheck
    health_ok = test_health()
    if not health_ok:
        print("\n⚠️  Health check returned non-200. ClickHouse or LLM provider might be offline.")
        print("We will still attempt to run chat tests to verify the 503 Service Unavailable handling.")

    # 2. Conversational Refusal / Scope Test
    print("\n[Test 1] Conversational / Out-of-Scope Query (Default: Hugging Face)")
    test_chat("Who are you and what cryptos do you track?")

    # 3. Quantitative ClickHouse Query Test
    print("\n[Test 2] Quantitative ClickHouse Query (Gemini)")
    test_chat("What is the worst drawdown currently happening across all active tokens?", provider="gemini")

