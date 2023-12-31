import argparse
import queue
import threading
import sqlite3
from sqlite3 import Connection
import json
import ast


import pandas as pd
from pysui import __version__, SuiConfig, SyncClient
from pysui.sui.sui_txresults import SuiCoinObject

from merge_coins_pubsub_v2 import merge_coins_helper

def setup_db(purge, filename):
    conn = sqlite3.connect("coins_data.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='coins'")
    
    if cursor.fetchone():
        if purge:                        
            conn.execute("DROP TABLE coins")
        else:            
            return conn
        
    column_names = ['balance', 'coin_object_id', 'version', 'digest', 'previous_transaction', 'coin_type']

    df = pd.read_csv(filename, chunksize=50000, names=column_names)
    for chunk in df:
        chunk.to_sql('coins', conn, if_exists='append', index_label='idx')

    conn.execute("ALTER TABLE coins ADD COLUMN status TEXT")
    conn.execute("ALTER TABLE coins ADD COLUMN error TEXT")
    conn.commit()
    return conn    


def fetch_coins(queues, results_queue, conn: Connection, gas_objects, retry_failed=False, chunksize=250):
    db_idx = 0
    cursor = conn.cursor()    
    gas_objects_placeholders = ', '.join(['?' for _ in gas_objects])    
    status_filter = 'status IS NULL' if not retry_failed else "status IS NULL or status != 'deleted'"
    fetch_query = f"SELECT * FROM coins WHERE {status_filter} AND coin_object_id NOT IN ({gas_objects_placeholders}) AND idx > ? ORDER BY idx ASC LIMIT ?"
    fetch_amount = len(queues) * chunksize                
    while True:        
        params = gas_objects + [db_idx, fetch_amount]
        cursor.execute(fetch_query, params)        
        data_list = [dict(row) for row in cursor.fetchall()]            
        if not data_list:                        
            break
        db_idx = data_list[-1]['idx']
        coins_to_merge = [SuiCoinObject.from_dict(obj) for obj in data_list]
        indices = [obj['idx'] for obj in data_list]        

        for i in range(0, len(data_list), chunksize):            
            queues[i // chunksize % len(queues)].put((
                indices[i:i+chunksize],
                coins_to_merge[i:i+chunksize]
            ))
                                    
        results_queue.put(('processing', None, indices))         
    for q in queues:
        q.put(None)

def write_results(results_queue, conn):    
    cursor = conn.cursor()

    while True:
        status, error, indices = results_queue.get()
        if status is None:
            break

        print(f"Coins completed with status {status}")

        placeholders = ', '.join('?' * len(indices))
        if status == 'processing':
            query = f"UPDATE coins SET status = 'processing' WHERE idx IN ({placeholders})"
            params = tuple(indices)        
        elif status == 'deleted':
            query = f"UPDATE coins SET status = 'deleted' WHERE idx IN ({placeholders})"
            params = tuple(indices)                    
        else:
            query = f"UPDATE coins SET status = '{status}', error = ? WHERE idx IN ({placeholders})"
            params = [error] + indices            
        cursor.execute(query, params)
        conn.commit()

def process_coins(read_queue, results_queue, client, signer, gas_object):
    while True:
        data = read_queue.get()
        if data is None:
            break
        (indices, coins_to_merge) = data        
        try:
            merge_coins_helper(coins_to_merge, client, signer, gas_object)
            results_queue.put(('deleted', None, indices))
        except Exception as e:
            error_message = str(e)
            error_type = None
            if "Transaction has non recoverable errors from at least 1/3 of validators" in error_message:                    
                error_dict = ast.literal_eval(error_message)                
                errors_array = error_dict['data']                
                errors_array = [error[0] for error in errors_array]        
                
                filtered_errors = []                
                for error in errors_array:                    
                    for error_name, error_details in error.items():
                        if error_name == "RpcError":
                            continue                                            
                        elif error_name == "UserInputError":
                            if error_details['error'].get("ObjectNotFound"):
                                object_not_found = error_details["error"]["ObjectNotFound"]                            
                                if object_not_found["object_id"] == gas_object:                                
                                    error_type = "gas_object_not_found"
                                    filtered_errors.append(error)                                    
                                else:
                                    continue
                        else:
                            filtered_errors.append(error)
                
                error_message = json.dumps(errors_array)
                error_type = error_type if error_type else "execution_error"                                
                results_queue.put((error_type, error_message, indices))
            else:
                error_type = "other_error"
                results_queue.put((error_type, error_message, indices))                
                
def main():    
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")
    parser.add_argument("--prv-key", type=str, help="Private key to use. This should be the Keystore formatted private key. You can convert private key from wallet with `sui keytool convert <VALUE>`")
    parser.add_argument("--signer", type=str, help="Signer address to use. str repr of SuiAddress.")
    parser.add_argument("--gas-objects", nargs='+', type=str, help="Gas objects to use. str repr of ObjectIDs.")    
    parser.add_argument("--filename", type=str, help="Filename to use.", default="output.csv")
    parser.add_argument("--purge", help="Whether to purge the table if it exists.", action='store_true')
    parser.add_argument("--retry-failed", help="Whether to retry failed coins.", action='store_true')
    args = parser.parse_args()

    cfg = SuiConfig.user_config(
        rpc_url = args.rpc_url,
        prv_keys = [args.prv_key]        
    )
    client = SyncClient(cfg)
    signer = args.signer
    gas_objects = args.gas_objects
                    
    queues = [queue.Queue() for _ in range(len(gas_objects))]    
    results_queue = queue.Queue()
    print("Setting up db")
    conn = setup_db(args.purge, args.filename)
    print("db setup complete")

    print("Gas smashing...")
    try:
        writer_thread = threading.Thread(target=write_results, args=(results_queue, conn))
        writer_thread.start()

        consumer_threads = [threading.Thread(target=process_coins, args=(q, results_queue, client, signer, gas_objects[i])) for i, q in enumerate(queues)]
        for t in consumer_threads:
            t.start()

        producer_thread = threading.Thread(target=fetch_coins, args=(queues, results_queue, conn, gas_objects, args.retry_failed))
        producer_thread.start()            
    finally:
        print("Gas smashing complete. Cleaning up...")
        # Cleanup. Close producer_thread, consumer_threads, writer_thread, conn.
        producer_thread.join()
        for t in consumer_threads:
            t.join()    
        results_queue.put((None, None, None))
        writer_thread.join()    
        conn.close()            
if __name__ == "__main__":
    main()    
