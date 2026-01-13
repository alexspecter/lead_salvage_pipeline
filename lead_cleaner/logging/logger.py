import os
import csv
from datetime import datetime, timezone
from typing import Optional

from lead_cleaner.config import DEFAULT_LOG_DIR

class PipelineLogger:
    def __init__(self, run_id: str, log_dir: str = DEFAULT_LOG_DIR):
        self.run_id = run_id
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        
        self.log_file = os.path.join(log_dir, f"run_{run_id}.csv")
        
        # CSV Headers matching LOG EVENT SCHEMA
        self.headers = [
            "run_id",
            "row_id", 
            "phase",
            "action",
            "before",
            "after",
            "reason",
            "confidence",
            "timestamp"
        ]
        
        # Initialize CSV file with headers
        with open(self.log_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
    
    def log_event(
        self,
        phase: str,
        action: str,
        row_id: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        reason: Optional[str] = None,
        confidence: Optional[float] = None
    ):
        event = {
            "run_id": self.run_id,
            "row_id": row_id or "",
            "phase": phase,
            "action": action,
            "before": str(before) if before is not None else "",
            "after": str(after) if after is not None else "",
            "reason": reason or "",
            "confidence": confidence if confidence is not None else "",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(event)
            
        # Console output for visibility
        msg = f"[{phase}] {action}"
        if row_id:
            msg += f" (Row: {row_id})"
        if reason:
            msg += f" - {reason}"
        print(msg)
    
    def log_error(self, phase: str, message: str, error: Exception):
        self.log_event(
            phase=phase,
            action="ERROR",
            reason=f"{message}: {str(error)}"
        )
