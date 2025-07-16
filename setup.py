from setuptools import find_packages, setup

setup(
    name="crypto_trading_ai_agents",  # Use a meaningful project name
    version="0.1.0",
    description="AI-powered crypto trading assistant",
    author="Muhammad Abdullah Rasheed",
    license="MIT",  # Specify a license (e.g., MIT) or leave empty
    packages=find_packages(where="src"),  # Look for packages in src/
    package_dir={"": "src"},  # Map package root to src/
    install_requires=[
        "pandas",
        "numpy",
        "requests",
        "python-binance",
        "pyyaml",
        "scikit-learn",
        "tensorflow",
        "black",
        "matplotlib",
        "nltk",
        "transformers",
        "openai",
        "torch",
        "langchain",
        "pinecone-client",
        "websocket-client",
        
    ],
    python_requires=">=3.9",
)