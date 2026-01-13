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
            # Using 4-bit quantized model as per directive (though directive said 70B, using 8B for dev/test if 70B not available or for speed)
            # Directive said: Llama 3.1 70B 4-bit.
            # I will default to that path but allow overriding.
            # For this environment I might need to stick to what I can run or just code it.
            # I'll stick to the directive's requested model in the code but use a smaller one for default if allowed?
            # Directive: "Llama 3.1 70B 4-bit". 
            # CAUTION: 70B requires ~40GB VRAM. m4 max has 64gb so it fits.
            
            # For now I will put the string here.
            model_id = "mlx-community/Meta-Llama-3.1-70B-Instruct-4bit"
            self.model, self.tokenizer = load(model_id)
            self.logger.log_event("PHASE_2", "MODEL_LOAD_COMPLETE")
        except Exception as e:
            self.logger.log_error("PHASE_2", "MODEL_LOAD_FAILED", e)
            raise e

    def generate_response(self, prompt: str) -> str:
        if not self.model:
            return "{}" # Fail safe
        
        # Create sampler for deterministic output (temp=0)
        # mlx_lm's make_sampler handles temp=0 correctly (usually greedy)
        sampler = make_sampler(temp=0.0)
            
        response = generate(
            self.model, 
            self.tokenizer, 
            prompt=prompt, 
            verbose=False, 
            max_tokens=512,
            sampler=sampler
        )
        return response
