import os
import google.generativeai as genai
from base_provider import BaseLLMProvider

class GeminiProvider(BaseLLMProvider):
    """
    Inference provider for Google Gemini models.
    """
    def __init__(self, model_id: str = "gemini-3.5-flash"):
        super().__init__(model_id)
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("❌ Missing GEMINI_API_KEY inside your .env configuration file.")
        genai.configure(api_key=self.api_key)

    def generate(self, system_instruction: str, user_prompt: str) -> str:
        model = genai.GenerativeModel(
            model_name=self.model_id,
            system_instruction=system_instruction
        )
        return model.generate_content(user_prompt).text.strip()
