import logging
import sys
import json
from datetime import datetime, timezone

def setup_audit_logger(name="audit_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate logs if already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        # Custom formatter that adds an [AUDIT] tag
        class AuditFormatter(logging.Formatter):
            def format(self, record):
                audit_event = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": record.levelname,
                    "event": record.getMessage(),
                    "module": record.module,
                }
                return f"[AUDIT] {json.dumps(audit_event)}"
                
        handler.setFormatter(AuditFormatter())
        logger.addHandler(handler)
        
    return logger

audit_logger = setup_audit_logger()
