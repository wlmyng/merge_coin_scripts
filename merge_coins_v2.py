import argparse
import queue
import threading
from typing import List
import ast

import pandas as pd
from pysui import __version__, SuiConfig, SyncClient
from pysui.sui.sui_txresults import SuiCoinObject

from merge_coins_pubsub_v2 import merge_coins_helper

def fetch_coins(queues, filename, gas_objects, chunksize=12500):
    column_names = ['balance', 'coin_object_id', 'version', 'digest', 'previous_transaction', 'coin_type']    
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

    

def process_coins(queue, dead_letter_queue, client, signer, gas_object):
    while True:
        coins_to_merge: List[SuiCoinObject] = queue.get()
        if coins_to_merge is None:
            break
        try:
            merge_coins_helper(coins_to_merge, client, signer, gas_object)
        except Exception as e:
            if "Transaction has non recoverable errors from at least 1/3 of validators" not in str(e):            
                dead_letter_queue.put((e, coins_to_merge))
            else:
                error_message = str(e)
                error_dict = ast.literal_eval(error_message)
                errors_array = error_dict['data']
                errors_array = [error[0] for error in errors_array]
                print(errors_array)
    
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
    dead_letter_queue = queue.Queue()

    consumer_threads = [threading.Thread(target=process_coins, args=(q, dead_letter_queue, client, signer, gas_objects[i])) for i, q in enumerate(queues)]
    for t in consumer_threads:
        t.start()

    producer_thread = threading.Thread(target=fetch_coins, args=(queues, args.filename, gas_objects))
    producer_thread.start()        

    producer_thread.join()
    for t in consumer_threads:
        t.join()

    counter = 0
    while not dead_letter_queue.empty():
        item = dead_letter_queue.get()        
        print(item)
        counter += 1

    print(counter)
    
if __name__ == "__main__":
    main()    