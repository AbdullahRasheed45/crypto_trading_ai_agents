# ₿ Crypto Trading AI Agents (Research-First Architecture)

A comprehensive research framework for developing AI-powered cryptocurrency trading agents with price prediction and signal generation capabilities. Built with a modular, config-driven architecture that prioritizes reproducible experiments and clean separation between research exploration and production-ready code.

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Lab-orange?style=for-the-badge&logo=jupyter&logoColor=white)](https://jupyter.org/)
[![Crypto](https://img.shields.io/badge/Crypto-Trading-gold?style=for-the-badge&logo=bitcoin&logoColor=white)](https://bitcoin.org/)
[![MIT](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](https://opensource.org/licenses/MIT)

✨ **Features**

🤖 **Agentic AI Pipeline**: Complete data-to-decision workflow with feature engineering, model training, signal generation, and performance evaluation

⚙️ **Config-Driven Research**: Centralized `config.yml` for reproducible experiments across different symbols, timeframes, and model configurations

🔬 **Research-First Design**: Jupyter notebooks for exploration and experimentation with clean `src/` package structure for production code

📊 **Technical Analysis Integration**: Built-in support for moving averages, RSI, MACD, Bollinger Bands, and custom technical indicators

🎯 **Signal Generation**: Intelligent buy/sell/hold signal generation with risk management, position sizing, and stop-loss integration

📈 **Comprehensive Backtesting**: Realistic evaluation with transaction costs, slippage, and performance metrics for strategy validation

🗂️ **Project structure**
```
.
├─ src/                 # Production-ready library code
│  ├─ data/            # Data loading and preprocessing utilities
│  ├─ features/        # Technical analysis and feature engineering
│  ├─ models/          # ML models for price prediction
│  ├─ agents/          # Trading agent logic and strategies
│  ├─ backtest/        # Backtesting engine and metrics
│  └─ utils/           # Shared utilities and helpers
├─ notebooks/          # Jupyter experiments and exploratory analysis
├─ tests/              # Unit tests for core modules
├─ config.yml          # Central configuration for reproducible runs
├─ template.py         # Starter template for new trading strategies
├─ requirements.txt    # Python dependencies
└─ setup.py           # Package installation metadata
```

🚀 **Quickstart**

**Prerequisites**
- Python 3.8 or higher
- Jupyter Lab (for notebook-based research)

**1) Clone & install**
```bash
git clone https://github.com/AbdullahRasheed45/crypto_trading_ai_agents.git
cd crypto_trading_ai_agents

# Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

**2) Configure your research**
```yaml
# Edit config.yml for your research parameters
symbols: ["BTC-USD", "ETH-USD", "ADA-USD"]
data:
  provider: "yfinance"          # Data source
  granularity: "1d"            # Timeframe (1m, 5m, 1h, 1d)
  lookback_days: 365           # Historical data window

features:
  technicals: ["SMA_20", "EMA_50", "RSI_14", "MACD"]
  returns: ["log_returns", "volatility"]
  
models:
  type: "gradient_boosting"     # Current: gradient_boosting
  hyperparameters:
    n_estimators: 100
    max_depth: 6
```

**3) Start experimenting**

**Option A: Research Mode (Jupyter)**
```bash
jupyter lab
# Open notebooks/ directory and explore the research workflow
```

**Option B: Script Mode (Reproducible)**
```bash
# Use the template for consistent experiments
python template.py --config config.yml
```

🧠 **Research pipeline (Phase 1 - Complete)**

**1. Data Acquisition**: Load OHLCV data for specified cryptocurrency pairs and timeframes

**2. Feature Engineering**: Generate technical indicators, rolling statistics, and market regime signals

**3. Model Development**: Train predictive models for price direction and volatility forecasting  

**4. Signal Generation**: Convert model predictions into actionable long/flat/short trading signals

**5. Performance Evaluation**: Comprehensive backtesting with realistic transaction costs and risk metrics

**6. Results Analysis**: Generate performance reports, visualizations, and strategy comparisons

📊 **Current capabilities (Phase 1)**

**Data Sources**:
- **Yahoo Finance**: Historical OHLCV data for major cryptocurrencies
- **Configurable Timeframes**: 1-minute to daily granularity
- **Multiple Assets**: Support for any Yahoo Finance crypto ticker

**Technical Analysis**:
- **Moving Averages**: SMA, EMA with configurable periods
- **Momentum Indicators**: RSI, MACD, Stochastic
- **Volatility Measures**: Bollinger Bands, Average True Range
- **Custom Features**: Log returns, rolling volatility, regime detection

**Machine Learning Models**:
- **Gradient Boosting**: Primary model for price direction prediction
- **Feature Importance**: Automated analysis of most predictive indicators
- **Cross-Validation**: Time-series aware validation for robust model selection

**Backtesting Framework**:
- **Realistic Costs**: Transaction fees and bid-ask spread simulation
- **Risk Management**: Position sizing and drawdown controls
- **Performance Metrics**: Sharpe ratio, maximum drawdown, win rate analysis

⚙️ **Configuration system**

**Flexible Research Parameters**:
```yaml
# Research configuration example
research:
  experiment_name: "btc_eth_momentum_v1"
  random_seed: 42
  
symbols: ["BTC-USD", "ETH-USD"]

data:
  provider: "yfinance"
  start_date: "2022-01-01"
  granularity: "1h"
  
features:
  technical_indicators:
    - name: "RSI"
      period: 14
    - name: "MACD" 
      fast: 12
      slow: 26
      signal: 9

backtesting:
  initial_capital: 10000
  transaction_cost: 0.001    # 0.1% per trade
  max_position_size: 0.25    # 25% of portfolio
```

🧪 **Development workflow**

**1. Notebook Exploration**:
```bash
# Start with exploratory data analysis
notebooks/01_data_exploration.ipynb

# Develop and test features
notebooks/02_feature_engineering.ipynb  

# Experiment with models
notebooks/03_model_development.ipynb
```

**2. Production Implementation**:
```python
# Move validated code to src/ modules
src/features/technical_indicators.py
src/models/gradient_boosting.py
src/agents/momentum_strategy.py
```

**3. Testing & Validation**:
```bash
# Run unit tests
pytest tests/ -v

# Validate with template script
python template.py --config config.yml
```

🔬 **Research methodology**

**Reproducible Experiments**:
- **Configuration Management**: All parameters in version-controlled YAML
- **Seed Control**: Deterministic results for model training and backtesting  
- **Environment Isolation**: Virtual environment with pinned dependencies

**Robust Validation**:
- **Time Series Splits**: No look-ahead bias in model validation
- **Walk-Forward Testing**: Simulate realistic trading conditions
- **Out-of-Sample Evaluation**: Reserve recent data for final validation

**Performance Analysis**:
```python
# Key metrics tracked in Phase 1
metrics = {
    'returns': ['total_return', 'cagr', 'volatility'],
    'risk': ['max_drawdown', 'sharpe_ratio', 'sortino_ratio'], 
    'trading': ['win_rate', 'avg_win_loss', 'trade_frequency'],
    'costs': ['total_fees', 'slippage_impact', 'turnover']
}
```

🛠️ **Extending the framework**

**Adding New Features** (Future Phases):
```python
# src/features/sentiment_analysis.py
def add_social_sentiment_features(df):
    """Add Twitter/Reddit sentiment indicators"""
    # Implementation for Phase 2
    pass

# src/features/on_chain_metrics.py  
def add_blockchain_metrics(df, symbol):
    """Add on-chain analysis features"""
    # Implementation for Phase 3
    pass
```

**New Model Integration**:
```python
# src/models/lstm_predictor.py
class LSTMPredictor(BaseModel):
    """Deep learning model for sequence prediction"""
    # Planned for Phase 2
    
    def fit(self, X, y):
        # Neural network training logic
        pass
```

**Custom Trading Strategies**:
```python
# src/agents/mean_reversion.py
class MeanReversionAgent(BaseAgent):
    """Mean reversion trading strategy"""
    # Framework ready for new strategies
    
    def generate_signals(self, predictions, features):
        # Custom signal generation logic
        pass
```

🧪 **Testing framework**

**Current Test Coverage**:
```bash
# Run existing tests
pytest tests/ -v

# Test categories implemented:
# - Feature calculation accuracy
# - Model training/prediction interfaces  
# - Signal generation logic
# - Backtest metric calculations
```

**Test-Driven Development**:
```python
# tests/test_features.py
def test_rsi_calculation():
    """Verify RSI matches expected values"""
    assert calculated_rsi == expected_rsi

def test_macd_crossover():
    """Test MACD signal generation"""
    assert signal_detected == expected_signal
```

📈 **Performance tracking**

**Research Metrics Dashboard**:
```python
# Comprehensive performance analysis
performance_report = {
    'strategy_metrics': {
        'total_return': 0.156,      # 15.6% total return
        'cagr': 0.089,              # 8.9% annualized
        'max_drawdown': -0.078,     # 7.8% max loss
        'sharpe_ratio': 1.24,       # Risk-adjusted return
        'win_rate': 0.547           # 54.7% winning trades
    },
    'benchmark_comparison': {
        'vs_buy_hold': 0.034,       # 3.4% outperformance
        'vs_random': 0.089          # 8.9% vs random signals
    }
}
```

🔒 **Risk management & ethics**

⚠️ **Important Disclaimers**:
- **Research Purpose Only**: This framework is for educational and research purposes
- **Not Financial Advice**: All outputs are experimental and should not guide real trading decisions
- **High Risk Warning**: Cryptocurrency trading involves substantial risk of loss

**Built-in Risk Controls**:
```python
# Risk management parameters in config
risk_management:
  max_position_size: 0.20        # Limit single position risk
  stop_loss: 0.05               # 5% stop loss
  max_daily_trades: 10          # Prevent overtrading
  drawdown_threshold: 0.15      # Stop trading at 15% drawdown
```

**Research Ethics**:
- **No Market Manipulation**: Educational research only, not for actual trading
- **Data Usage Compliance**: Respect all data provider terms of service  
- **Open Source**: Transparent methodology for peer review and improvement

🐛 **Troubleshooting (Phase 1)**

**Common Setup Issues**:
- **Import Errors** → Ensure `pip install -e .` was run in project root
- **Config Not Found** → Use `--config config.yml` and verify file path
- **Missing Dependencies** → Run `pip install -r requirements.txt` again

**Data Issues**:
- **API Rate Limits** → Reduce data frequency or add delays between requests
- **Missing Historical Data** → Adjust `start_date` in config for available data range
- **NaN Values in Features** → Check rolling window sizes and data continuity

**Performance Issues**:
- **Slow Backtesting** → Reduce data range or simplify feature calculations
- **Memory Usage** → Process data in smaller chunks for large datasets
- **Unrealistic Results** → Verify transaction costs and slippage are included

🚧 **Roadmap & future phases**

**Phase 2 (In Development)**:
- **Deep Learning Models**: LSTM and Transformer architectures for sequence prediction
- **Alternative Data**: Social sentiment, news analysis, on-chain metrics
- **Multi-Asset Strategies**: Portfolio-level optimization and correlation analysis

**Phase 3 (Planned)**:
- **Real-Time Integration**: Live data feeds and paper trading capabilities
- **Advanced Risk Management**: VaR models, portfolio optimization
- **Web Interface**: Dashboard for strategy monitoring and configuration

**Phase 4 (Future)**:
- **Cloud Deployment**: Scalable backtesting on cloud infrastructure  
- **Strategy Marketplace**: Share and compare trading strategies
- **Regulatory Compliance**: Framework for professional trading applications

📜 **License**

MIT License - see [LICENSE](LICENSE) file for complete terms. Open source framework designed for educational and research purposes.

## 📞 Connect & Support

<div align="center">

### 🚀 Ready to Build AI-Powered Trading Systems?

[![Portfolio](https://img.shields.io/badge/Portfolio-000000?style=for-the-badge&logo=About.me&logoColor=white)](https://techvibes360.com)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/abdullahrasheed-/)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:abdullahrasheed45@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/AbdullahRasheed45)

**Let's revolutionize crypto trading with responsible AI research!**

</div>

---

*Built with ❤️ for the crypto research community. Perfect for learning quantitative trading, AI model development, and building reproducible financial research frameworks.*
