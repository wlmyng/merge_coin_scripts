import requests
import json
import argparse
import os
import time
import pandas as pd

column_names = ['balance', 'checkpoint', 'coin_object_id', 'version', 'digest', 'owner_type', 
                'owner_address', 'initial_shared_version', 'previous_transaction', 
                'coin_type', 'object_status', 'has_public_transfer', 'storage_rebate', 'bcs']


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


def fetch_coins(owner, url, coin_type="0x2::sui::SUI", cursor=None, limit=500):
    coins = []
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
                if not has_next_page:
                    break
            else:
                break
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if coins:
            return coins            
        


def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")    
    parser.add_argument("--owner", type=str, help="Signer address to use. str repr of SuiAddress.", required=True)
    
    args = parser.parse_args()

    os.makedirs("coins", exist_ok=True)

    start = time.time()
    data = fetch_coins(args.owner, args.rpc_url)
    df = pd.DataFrame(data)

    df = df.rename(columns={'coinType': 'coin_type', 'coinObjectId': 'coin_object_id'})
    for column in column_names:
        if column not in df.columns:
            df[column] = 'dummy_value'  # Replace 'dummy_value' with your actual dummy value

    # Reorder columns to match column_names
    df = df[column_names]

    # Write DataFrame to CSV
    df.to_csv('output.csv', index=False, header=False)
    end = time.time()
    print(f"Time taken: {end - start} seconds")


if __name__ == "__main__":
    main()