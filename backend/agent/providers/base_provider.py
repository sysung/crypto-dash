from abc import ABC, abstractmethod

class BaseLLMProvider(ABC):
    """
    Abstract Base Class for LLM providers.
    Manages connections and handles prompt text generation.
    """
    def __init__(self, model_id: str):
        self.model_id = model_id

    @abstractmethod
    def generate(self, system_instruction: str, user_prompt: str) -> str:
        """Generates a text completion based on system instructions and a user prompt."""
        pass
