import os

# Enhanced list of directories with submodules for agents and modular scalability
directories = [
    "src",
    "src/data",
    "src/data/data_ingestion",
    "src/features",
    "src/models",
    "src/agent",
    "src/agent/sentiment",
    "src/agent/rl",
    "src/agent/onchain",
    "src/agent/fusion",
    "src/agent/execution",
    "src/core",
    "src/utils",
    "src/services",
    "src/logger",
    "data",
    "data/raw",
    "data/processed",
    "logs",
    "notebooks",
    "tests"
]

# Create the directories
for directory in directories:
    os.makedirs(directory, exist_ok=True)

# Placeholder files with detailed starter content
files_to_create = {
    ".gitignore": "# Ignore sensitive files and environment-specific folders\n\n# Secrets\nconfig.yml\n\n# Python cache\n__pycache__/\n*.pyc\n\n# Virtual environment\n.venv/\nvenv/\nenv/\n\n# Data files\n/data/raw/\n/data/processed/\n/logs/",
    "README.md": "# Agentic Crypto Trading Engine\n\nAn AI-powered modular agentic system to analyze cryptocurrency markets and generate profitable signals using RL, sentiment, on-chain, and ensemble models.\n\n## Setup\n```bash\npython -m venv venv\nsource venv/bin/activate\npip install -r requirements.txt\n```",
    "config.yml": "# Configuration File\napi_keys:\n  binance:\n    key: \"YOUR_BINANCE_API_KEY\"\n    secret: \"YOUR_BINANCE_API_SECRET\"\n  news_api:\n    key: \"YOUR_NEWS_API_KEY\"\ndata_paths:\n  raw: \"data/raw/\"\n  processed: \"data/processed/\"\nmodels:\n  lstm:\n    epochs: 100\n    batch_size: 32\n    lookback_period: 60\nlog:\n  level: INFO\n  filepath: logs/agent.log",
    "requirements.txt": "# Project Dependencies\npandas\nnumpy\nrequests\npython-binance\npyyaml\nscikit-learn\ntensorflow\nblack\nflake8\nmatplotlib\nnltk\ntransformers\nopenai\ntorch\nlangchain\npinecone-client",
    "setup.py": " ",
    "src/__init__.py": "",
    "src/logger/__init__.py": "",
    "src/data/__init__.py": "",
    "src/data/data_ingestion/__init__.py": "",
    "src/data/data_ingestion/market_data.py": "",
    "src/features/__init__.py": "",
    "src/models/__init__.py": "",
    "src/agent/__init__.py": "",
    "src/agent/sentiment/__init__.py": "",
    "src/agent/rl/__init__.py": "",
    "src/agent/onchain/__init__.py": "",
    "src/agent/fusion/__init__.py": "",
    "src/agent/execution/__init__.py": "",
    "src/core/__init__.py": "",
    "src/utils/__init__.py": "",
    "src/services/__init__.py": "",
    "tests/__init__.py": "",
    "tests/test_market_data.py": "",
    "src/config_loader.py": "# YAML Config Loader\nimport yaml\nfrom typing import Dict\n\ndef load_config(path: str = 'config.yml') -> Dict:\n    with open(path, 'r') as f:\n        return yaml.safe_load(f)",
    "src/utils/logger.py": "# Logger Setup\nimport logging\n\ndef get_logger(name):\n    logger = logging.getLogger(name)\n    logger.setLevel(logging.INFO)\n    fh = logging.FileHandler('logs/agent.log')\n    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')\n    fh.setFormatter(formatter)\n    logger.addHandler(fh)\n    return logger",
    "src/main.py": "# Main Entry\nif __name__ == '__main__':\n    from src.config_loader import load_config\n    config = load_config()\n    print(\"Agentic Crypto Trading Engine initialized.\")",
    "notebooks/01_data_exploration.ipynb": "{\n \"cells\": [],\n \"metadata\": {},\n \"nbformat\": 4,\n \"nbformat_minor\": 2\n}",
    "tests/test_data_connectors.py": "# Unit tests for data connectors",
    "src/agent/sentiment/sentiment_agent.py": "# Sentiment agent using news and social media",
    "src/agent/rl/rl_agent.py": "# RL agent for training with market environment",
    "src/agent/onchain/onchain_agent.py": "# On-chain analysis agent",
    "src/agent/fusion/fusion_agent.py": "# Combines multiple agent signals into one",
    "src/agent/execution/execution_agent.py": "# Trade execution agent",
    "src/core/risk_manager.py": "# Handles risk allocation logic",
    "src/services/api_service.py": "# Handles API integrations (e.g., news, social)"
}

# Create the files
for filepath, content in files_to_create.items():
    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            f.write(content)

