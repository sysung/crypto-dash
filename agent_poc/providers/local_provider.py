import os
from base_provider import BaseLLMProvider

class LocalHuggingFaceProvider(BaseLLMProvider):
    """
    Inference provider for running Hugging Face models locally using transformers.
    """
    def __init__(self, model_id: str = None):
        if model_id is None:
            model_id = os.getenv("LOCAL_MODEL_ID", "Qwen/Qwen2.5-1.5B-Instruct")
        super().__init__(model_id)
        self.tokenizer = None
        self.model = None

    def _lazy_init(self):
        """
        Lazily imports PyTorch and Transformers and loads weights into memory.
        Prevents slow startup times when running other remote providers.
        """
        if self.model is not None:
            return
            
        print("\n📦 Loading Hugging Face deep learning libraries...")
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM
        
        token = os.getenv("HF_TOKEN")
        
        print(f"⏳ Downloading/Loading local model '{self.model_id}'...")
        print("   (Note: This might take a few minutes on first run to download model weights)...")
        
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id, token=token)
        
        # Auto-detect best hardware acceleration backend (MPS on Mac, CUDA on Nvidia, or CPU)
        if torch.backends.mps.is_available():
            device_map = "mps"
            torch_dtype = torch.float16
            print("🍏 Apple Silicon MPS Hardware Acceleration active!")
        elif torch.cuda.is_available():
            device_map = "auto"
            torch_dtype = torch.float16
            print("🎮 Nvidia CUDA Hardware Acceleration active!")
        else:
            device_map = "cpu"
            torch_dtype = torch.float32
            print("💻 Falling back to standard CPU execution backend.")
            
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_id,
            dtype=torch_dtype,
            device_map=device_map,
            token=token
        )
        print("✅ Local Hugging Face model loaded successfully!")

    def generate(self, system_instruction: str, user_prompt: str) -> str:
        self._lazy_init()
        import torch
        
        chat = [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_prompt}
        ]
        
        formatted_prompt = self.tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)
        inputs = self.tokenizer(formatted_prompt, return_tensors="pt").to(self.model.device)
        
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=512,
                do_sample=False
            )
            
        input_length = inputs.input_ids.shape[1]
        generated_tokens = outputs[0][input_length:]
        return self.tokenizer.decode(generated_tokens, skip_special_tokens=True).strip()
