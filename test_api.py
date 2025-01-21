import os
from dotenv import load_dotenv
import requests
import json

def test_bitquery_api():
    load_dotenv()
    api_key = os.getenv("BITQUERY_API_KEY")
    
    url = "https://graphql.bitquery.io"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    # Test with a simple Solana query
    query = """
    query {
      solana {
        instructions(
          options: {limit: 1}
        ) {
          block {
            timestamp {
              time
            }
          }
          transaction {
            signature
          }
          program {
            id
            name
          }
        }
      }
    }
    """
    
    print("Testing Solana API query...")
    try:
        response = requests.post(url, headers=headers, json={"query": query})
        print(f"Status Code: {response.status_code}")
        print("Response:")
        print(json.dumps(response.json(), indent=2))
        
        if response.status_code == 200 and "data" in response.json():
            print("\nAPI key is valid!")
            
            # Now test a more specific query
            token_query = """
            query {
              solana {
                instructions(
                  options: {
                    limit: 1,
                    desc: "block.timestamp.time"
                  }
                  where: {program: {in: ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]}}
                ) {
                  block {
                    timestamp {
                      time
                    }
                  }
                  transaction {
                    signature
                  }
                  program {
                    id
                    name
                  }
                  accounts {
                    address
                  }
                }
              }
            }
            """
            
            print("\nTesting Token Program query...")
            response = requests.post(url, headers=headers, json={"query": token_query})
            print(f"Status Code: {response.status_code}")
            print("Response:")
            print(json.dumps(response.json(), indent=2))
            
        else:
            print("\nAPI key validation failed!")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_bitquery_api()
