# utils.py
import json
import os

def load_config(config_path="config.json"):
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # [중요] JSON의 String Key를 Python의 Integer Key로 변환
    # "emb_configs": {"1": ...} -> {1: ...}
    if "emb_configs" in config:
        new_emb_configs = {}
        for k, v in config["emb_configs"].items():
            new_emb_configs[int(k)] = v
        config["emb_configs"] = new_emb_configs
        
    return config