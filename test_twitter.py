import asyncio
import logging
from token_monitor_twitter import TwitterAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def test_twitter_analysis():
    """Test the Twitter analyzer with a known token"""
    try:
        # Initialize analyzer
        analyzer = TwitterAnalyzer()
        
        # Test with BONK token address
        test_address = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        
        logging.info(f"Starting Twitter analysis for token: {test_address}")
        
        # Run analysis
        metrics = await analyzer.analyze_token(test_address)
        
        # Print results
        print("\n=== Twitter Analysis Results ===")
        print(f"Contract Address: {test_address}")
        print(f"Mention Count: {metrics['mention_count']}")
        print(f"Verified Mentions: {metrics['verified_mentions']}")
        print(f"Notable Mentions: {len(metrics['notable_mentions'])}")
        print(f"Sentiment Score: {metrics['sentiment_score']:.2f}")
        print(f"Name Changes: {len(metrics['name_changes'])}")
        
        if metrics['notable_mentions']:
            print("\nNotable Mentions:")
            for mention in metrics['notable_mentions'][:3]:  # Show top 3
                print(f"- @{mention['username']}: {mention['text'][:100]}...")
        
        if metrics['official_account']:
            print(f"\nOfficial Account: @{metrics['official_account']}")
            
        print("\nRecent Tweets Sample:")
        for tweet in metrics['recent_tweets'][:3]:  # Show top 3
            print(f"- @{tweet['author']}: {tweet['text'][:100]}...")
            
    except Exception as e:
        logging.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_twitter_analysis())
