import argparse
import queue
import threading

import pandas as pd
from pysui import __version__, SuiConfig, SyncClient
from pysui.sui.sui_txresults import SuiCoinObject

from merge_coins_pubsub_v2 import merge_coins

def fetch_coins(queues, filename, gas_objects, chunksize=12500):
    column_names = ['balance', 'checkpoint', 'coin_object_id', 'version', 'digest', 'owner_type', 
                'owner_address', 'initial_shared_version', 'previous_transaction', 
                'coin_type', 'object_status', 'has_public_transfer', 'storage_rebate', 'bcs']
    for chunk in pd.read_csv(filename, names=column_names, chunksize=chunksize):
        coins_to_merge = []
        chunk = chunk[~chunk['coin_object_id'].isin(gas_objects)]        
        data_list = chunk.to_dict('records')
        for i in range(0, len(data_list), 250):
            sub_chunk = data_list[i:i+250]
            coins_to_merge = [SuiCoinObject.from_dict(obj) for obj in sub_chunk]
            queues[i // 250 % len(queues)].put(coins_to_merge)            
    for q in queues:
        q.put(None)


def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")
    parser.add_argument("--prv-key", type=str, help="Private key to use. This should be the Keystore formatted private key. You can convert private key from wallet with `sui keytool convert <VALUE>`")
    parser.add_argument("--signer", type=str, help="Signer address to use. str repr of SuiAddress.")
    parser.add_argument("--gas-objects", nargs='+', type=str, help="Gas objects to use. str repr of ObjectIDs.")    
    parser.add_argument("--filename", type=str, help="Filename to use.", default="output.csv")
    args = parser.parse_args()

    cfg = SuiConfig.user_config(
        rpc_url = args.rpc_url,
        prv_keys = [args.prv_key]        
    )
    client = SyncClient(cfg)
    signer = args.signer
    gas_objects = args.gas_objects
                    
    queues = [queue.Queue() for _ in range(len(gas_objects))]
    consumer_threads = [threading.Thread(target=merge_coins, args=(q, client, signer, gas_objects[i])) for i, q in enumerate(queues)]
    for t in consumer_threads:
        t.start()

    producer_thread = threading.Thread(target=fetch_coins, args=(queues, args.filename, gas_objects))
    producer_thread.start()

    producer_thread.join()
    for t in consumer_threads:
        t.join()
    
if __name__ == "__main__":
    main()    