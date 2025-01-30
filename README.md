# Solana Token Monitor

A comprehensive monitoring and analysis system for tokens launched on pump.fun (Solana-based). This system performs extensive analysis of new tokens, tracking their performance, analyzing their metrics, and identifying potentially successful opportunities while filtering out suspicious tokens.

## Core Features

### 1. Token Launch Monitoring
- Real-time monitoring of new token launches on pump.fun
- Automatic market cap tracking
- Triggers detailed analysis when tokens reach 30,000 market cap threshold

### 2. Comprehensive Analysis System

#### Deployer Analysis
- Tracks deployer's historical token launches
- Calculates success rates:
  - Tokens reaching 3M market cap
  - Tokens failing to reach 200k market cap
- Automatic rejection if 97% of past tokens failed to reach 200k

#### Holder & Transaction Analysis
- Total holder count tracking
- Developer sell detection
- Sniper and insider wallet detection
- Automatic disqualification criteria:
  - More than 2 sniper purchases
  - More than 2 insider purchases
  - Buy/sell ratio > 70/30
  - More than 2 wallets holding > 8% supply

#### Twitter Integration
- Contract address monitoring
- Notable mention detection
- Name change tracking
- Sentiment analysis
- Risk assessment based on social signals

#### Top Holder Analysis
- Analyzes top 30 holders (excluding dev/liquidity)
- 14-day win rate calculation
- 30-day PNL tracking
- Historical performance metrics

### 3. Advanced Security Features
- Fake volume detection
- Circular trading detection
- Supply allocation analysis
- Blacklist system for suspicious addresses
- Good wallet tracking system

## Setup & Installation

1. **Clone the Repository**
   ```bash
   git clone [repository-url]
   cd solana_token_monitor
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Setup**
   Create a `.env` file with the following required keys:
   ```env
   HELIUS_API_KEY=your_helius_api_key
   DISCORD_WEBHOOK_URL=your_discord_webhook_url
   TWITTER_BEARER_TOKEN=your_twitter_token
   ```

## Usage Guide

### 1. Main Monitoring System
Run the main pump.fun monitoring script:
```bash
python main_pump_fun.py
```
This script:
- Connects to pump.fun websocket
- Monitors new token launches
- Triggers analysis when tokens reach 30k market cap

### 2. Token Analysis System
The token analysis system (`token_monitor_prime.py`) is automatically triggered by the main script but can also be run independently:
```bash
python token_monitor_prime.py [token_address]
```

### 3. Configuration Options

#### Market Cap Thresholds
```python
INITIAL_MCAP_THRESHOLD = 30000  # Initial analysis threshold
SUCCESS_MCAP_THRESHOLD = 3000000  # Success threshold for deployer
FAILURE_MCAP_THRESHOLD = 200000  # Failure threshold for deployer
```

#### Holder Analysis
```python
MAX_SNIPER_COUNT = 2
MAX_INSIDER_COUNT = 2
LARGE_HOLDER_THRESHOLD = 0.08  # 8% of supply
MAX_LARGE_HOLDERS = 2
```

#### Trading Patterns
```python
BUY_SELL_RATIO_THRESHOLD = 0.70
MIN_SELL_RATIO = 0.30
```

## Discord Notifications

The system sends detailed Discord notifications including:
- Token address and basic info
- Market cap and holder metrics
- Safety scores and analysis
- Links to Solscan and Jupiter
- Color-coded risk assessment

## Database Schema

The system uses SQLite with the following main tables:
- `transactions`: Token transaction history
- `tokens`: Token metadata and metrics
- `deployers`: Deployer history and stats
- `holders`: Holder balances and metrics
- `wallet_labels`: Tracked wallets (snipers/insiders)

## Monitoring Output

The system provides real-time logging and monitoring:
- Transaction monitoring
- Token analysis progress
- Error reporting
- Performance metrics

## Best Practices

1. **API Rate Limits**
   - Helius: 10 req/sec
   - Twitter: 0.5 req/sec
   - Jupiter: 5 req/sec

2. **Database Maintenance**
   - Regular cleanup of old transactions
   - Index optimization
   - Regular backups

3. **Performance Optimization**
   - Cache frequently accessed data
   - Use async operations for API calls
   - Implement exponential backoff for retries

## Troubleshooting

Common issues and solutions:
1. **WebSocket Connection Issues**
   - Check internet connection
   - Verify API keys
   - Check pump.fun status

2. **Rate Limit Errors**
   - Adjust rate limiter settings
   - Implement request queuing
   - Use multiple API keys

3. **Database Errors**
   - Check disk space
   - Verify table schema
   - Run database maintenance

## License

MIT

## Support

For issues and feature requests, please create an issue in the repository.
