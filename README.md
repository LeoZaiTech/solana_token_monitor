# Solana Token Monitor

A monitoring script for tracking and analyzing tokens launched on pump.fun (Solana-based).

## Features

- Monitors new token launches on pump.fun
- Analyzes tokens when they reach 30,000 market cap
- Performs comprehensive checks:
  - Deployer history analysis
  - Holder and transaction analysis
  - Twitter sentiment analysis
  - Top holders performance tracking
- Maintains blacklists of suspicious wallets
- Sends notifications via Discord webhook

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your API keys:
   ```bash
   cp .env.example .env
   ```

4. Configure the following API keys in your `.env` file:
   - Discord Webhook URL
   - Helius API Key
   - Twitter API credentials

## Usage

Run the script:
```bash
python token_monitor.py
```

The script will continuously monitor new tokens and send notifications to your Discord webhook when tokens meeting the criteria are found.

## Configuration

You can adjust the following parameters in `token_monitor.py`:
- Minimum market cap threshold
- Maximum sniper/insider counts
- Supply percentage thresholds
- Buy/sell ratio thresholds
- Confidence score thresholds

## License

MIT
# solana_token_monitor
