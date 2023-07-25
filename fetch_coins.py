import requests
import json
import argparse
import os
import time

def get_coins(owner, url, coin_type="0x2::sui::SUI", cursor=None, limit=1000):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getCoins",
        "params": [owner, coin_type, cursor, limit]
    }            
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers).json()    
    return response

def dump_to_json(coins, counter):
    with open(f'coins/{counter}.json', 'w') as f:
        json.dump(coins, f)

def fetch_coins(owner, url, coin_type="0x2::sui::SUI", cursor=None, limit=500):
    coins = []
    counter = 0
    try:
        while True:
            response = get_coins(owner, url, coin_type, cursor, limit)
            if response:
                fetched_coins = response['result']['data']                
                coins_to_merge = [
                    coin for coin in fetched_coins # SuiCoinObject
                ]
                coins.extend(coins_to_merge)
                
                has_next_page = response['result']['hasNextPage']
                cursor = response['result']['nextCursor']
                print(f"nextCursor: {cursor}")
                if not has_next_page or len(coins) >= limit * 50:
                    while len(coins) >= limit * 50:
                        dump_to_json(coins[:limit * 50], counter)                    
                        coins = coins[limit * 50:]
                        counter += 1
                    if not has_next_page and coins:
                        dump_to_json(coins, counter)                    
                        coins = []
                        counter += 1
                        break                
            else:
                break
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if coins:
            dump_to_json(coins, counter)
        


def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")    
    parser.add_argument("--owner", type=str, help="Signer address to use. str repr of SuiAddress.", required=True)
    
    args = parser.parse_args()

    os.makedirs("coins", exist_ok=True)

    start = time.time()
    fetch_coins(args.owner, args.rpc_url)
    end = time.time()
    print(f"Time taken: {end - start} seconds")


if __name__ == "__main__":
    main()