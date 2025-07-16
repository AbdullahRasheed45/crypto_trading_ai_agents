# YAML Config Loader
import yaml
from typing import Dict

def load_config(path: str = 'config.yml') -> Dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)