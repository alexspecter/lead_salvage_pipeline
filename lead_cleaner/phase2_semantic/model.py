try:
    from mlx_lm import load, generate
    from mlx_lm.sample_utils import make_sampler
except ImportError:
    load = None
    generate = None
    make_sampler = None

from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.config import ENABLE_LLM

class LocalLLM:
    def __init__(self, logger: PipelineLogger, model_path: str = "mlx-community/Meta-Llama-3.1-8B-Instruct-4bit"):
        self.logger = logger
        self.model_path = model_path
        self.model = None
        self.tokenizer = None
        
    def load_model(self):
        if not ENABLE_LLM:
            self.logger.log_event("PHASE_2", "MODEL_SKIP", reason="LLM Disabled in Config")
            return

        if load is None:
             self.logger.log_error("PHASE_2", "Import Error", Exception("mlx_lm not installed"))
             return

        self.logger.log_event("PHASE_2", "MODEL_LOAD_START", reason=self.model_path)
        try:
            # Use the model path from __init__ (defaults to 8B for performance)
            self.model, self.tokenizer = load(self.model_path)
            self.logger.log_event("PHASE_2", "MODEL_LOAD_COMPLETE")
        except Exception as e:
            self.logger.log_error("PHASE_2", "MODEL_LOAD_FAILED", e)
            raise e

    def generate_response(self, prompt: str) -> str:
        if not self.model:
            return "{}" # Fail safe
        
        # Create sampler for near-deterministic output (temp=0.1)
        sampler = make_sampler(temp=0.1)
        
        # Note: stop strings not supported in current mlx_lm version
        # Rely on max_tokens to limit generation
            
        response = generate(
            self.model, 
            self.tokenizer, 
            prompt=prompt, 
            verbose=False, 
            max_tokens=150,
            sampler=sampler
        )
        return response

