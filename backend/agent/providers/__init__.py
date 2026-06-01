from base_provider import BaseLLMProvider
from gemini_provider import GeminiProvider
from huggingface_provider import HuggingFaceProvider
from local_provider import LocalHuggingFaceProvider

def get_provider(name: str) -> BaseLLMProvider:
    """
    Factory function to instantiate the correct provider subclass cleanly.
    """
    if name == "gemini":
        return GeminiProvider()
    elif name == "hf":
        return HuggingFaceProvider()
    elif name == "local":
        return LocalHuggingFaceProvider()
    else:
        raise ValueError(f"Unknown AI Provider: {name}")
