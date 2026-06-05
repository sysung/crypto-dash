import os
import requests
from typing import Optional

class HFProvider:
    """
    Inference provider for Hugging Face Serverless Inference API.
    """
    def __init__(self, model_id: Optional[str] = None):
        self.model_id = model_id or os.getenv("HF_MODEL_ID", "google/gemma-4-26B-A4B-it")
        self.token = os.getenv("HF_TOKEN")
        if not self.token:
            raise ValueError("❌ Missing HF_TOKEN inside your .env configuration file.")


    def generate(self, system_instruction: str, user_prompt: str) -> str:
        url = "https://router.huggingface.co/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model_id,
            "messages": [
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 1000
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30.0)
        if response.status_code != 200:
            raise RuntimeError(f"Hugging Face API returned error ({response.status_code}): {response.text}")
            
        return response.json()["choices"][0]["message"]["content"]
