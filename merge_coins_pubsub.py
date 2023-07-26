import requests
import json
import argparse
import queue
import threading
import os
from datetime import datetime


from pysui import __version__, SuiConfig, SyncClient, SuiAddress
from pysui.sui.sui_txn import SyncTransaction

def dump_to_json(coins):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    with open(f'coins/{timestamp}.json', 'w') as f:
        json.dump(coins, f)

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

def fetch_coins(coin_queue, gas_object, owner, url, coin_type="0x2::sui::SUI", cursor=None, limit=500): # programmable transaction block limit is 512, undershoot a little    
    coins = []
    while True:
        response = get_coins(owner, url, coin_type, cursor, limit)
        if response:
            fetched_coins = response['result']['data']
            coins_to_merge = set([coin['coinObjectId'] for coin in fetched_coins])
            coins_to_merge.discard(gas_object)
            coins.extend(list(coins_to_merge))                        

            has_next_page = response['result']['hasNextPage']
            cursor = response['result']['nextCursor']
            if not has_next_page or len(coins) >= limit:
                # push to queue in chunks of limit
                while len(coins) >= limit:
                    coin_queue.put(coins[:limit])
                    coins = coins[limit:]
                if not has_next_page:
                    if coins:
                        coin_queue.put(coins) 
                    coin_queue.put(None)
                    break
        else:
            break

def merge_coins(coin_queue, client, signer, gas_object):
    leftover_coin = None
    while True:
        coins_to_merge = coin_queue.get()
        if coins_to_merge is None:
            break
        if leftover_coin is not None:
            coins_to_merge.append(leftover_coin)
        txn = SyncTransaction(client, initial_sender=SuiAddress(signer))
        txn.merge_coins(
            merge_to=coins_to_merge[0], merge_from=coins_to_merge[1:]
        )
        
        result = txn.execute(use_gas_object=gas_object)
        if not result.is_ok():
            print(result.result_string)
        else:
            print("ok")
            dump_to_json(coins_to_merge[1:])
        leftover_coin = coins_to_merge[0]

def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")
    parser.add_argument("--prv-key", type=str, help="Private key to use. This should be the Keystore formatted private key. You can convert private key from wallet with `sui keytool convert <VALUE>`")
    parser.add_argument("--signer", type=str, help="Signer address to use. str repr of SuiAddress.")
    parser.add_argument("--gas-object", type=str, help="Gas object to use. str repr of ObjectID.")
    args = parser.parse_args()

    cfg = SuiConfig.user_config(
        rpc_url = args.rpc_url,
        prv_keys = [args.prv_key]        
    )
    client = SyncClient(cfg)
    signer = args.signer
    gas_object = args.gas_object
                    
    coin_queue = queue.Queue()

    os.makedirs("coins", exist_ok=True)

    fetch_thread = threading.Thread(target=fetch_coins, args=(coin_queue, gas_object, signer, args.rpc_url))
    merge_thread = threading.Thread(target=merge_coins, args=(coin_queue, client, signer, gas_object))

    fetch_thread.start()
    merge_thread.start()

    fetch_thread.join()
    merge_thread.join()

if __name__ == "__main__":
    main()
    # invoke with python3 merge_coins_pubsub.py --prv-key "KEY" --signer "0xADDRESS" --gas-object "0xOBJECTID"