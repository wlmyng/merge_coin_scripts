import json
import glob
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, wait

from pysui import __version__, SuiConfig, SyncClient, SuiAddress
from pysui.sui.sui_txn import SyncTransaction
from pysui.sui.sui_txresults import SuiCoinObject, AddressOwner

def preprocess_chunk(chunk, owner, gas_objects: list[str]) -> list[SuiCoinObject]:
    unique_coins = []
    seen = set(gas_objects)
    for coin in chunk:        
        if coin['coinObjectId'] not in seen:
            unique_coins.append(coin)
            seen.add(coin['coinObjectId'])

    chunk = [SuiCoinObject.from_dict(coin) for coin in unique_coins]
    for coin in chunk:
        setattr(coin, 'owner', AddressOwner(address_owner=owner, owner_type="AddressOwner"))
    return chunk

def merge_coins(client: SyncClient, signer, coins: list[SuiCoinObject], gas_object: str):        
    print("start merge")
    txn = SyncTransaction(client, initial_sender=SuiAddress(signer))    
    txn.merge_coins(
        merge_to=coins[0], merge_from=coins[1:]
    )    
    result = txn.execute(use_gas_object=gas_object)            
    if not result.is_ok():
        print(result.result_string)
    else:
        print("ok")
    print("end merge")
    return [coins[0]]

def make_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]   

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rpc-url", type=str, help="RPC URL to use", default="https://fullnode.testnet.sui.io:443")
    parser.add_argument("--prv-key", type=str, help="Private key to use. This should be the Keystore formatted private key. You can convert private key from wallet with `sui keytool convert <VALUE>`")
    parser.add_argument("--signer", type=str, help="Signer address to use. str repr of SuiAddress.")
    parser.add_argument("--gas-object", type=str, help="Gas object to use. str repr of ObjectID.")
    parser.add_argument("--gas-to-split", type=str, help="Gas object to split. str repr of ObjectID.")
    parser.add_argument("--num-workers", type=int, help="Number of workers to use", default=5)
    args = parser.parse_args()

    cfg = SuiConfig.user_config(
        rpc_url = args.rpc_url,
        prv_keys = [args.prv_key]
    )
    client = SyncClient(cfg)
    signer = args.signer

    path_to_json_files = "coins/*.json"
    json_files = glob.glob(path_to_json_files)

    num_workers = args.num_workers

    def split_coin_equal(coin, split_count=num_workers):            
        txn = SyncTransaction(client, initial_sender=SuiAddress(signer))
        txn.split_coin_equal(coin=coin, split_count=split_count)
        result = txn.execute(
            use_gas_object=args.gas_object
        )   
        return result.result_data

    gas_objects = [args.gas_to_split]
    result = split_coin_equal(args.gas_to_split, num_workers)
    for object_changes in result.object_changes:
        if object_changes['type'] == 'created':
            object_id = object_changes['objectId']
            gas_objects.append(object_id)

    start = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for json_file in json_files:
            # Open the file
            with open(json_file, 'r') as file:
                # Load the JSON data from the file
                data = json.load(file)
               
            chunks = [preprocess_chunk(chunk, signer, gas_objects) for chunk in make_chunks(data, 500)]

            for idx in range(0, len(chunks), num_workers):
                futures = [executor.submit(merge_coins, client, signer, chunk, gas_objects[worker_idx]) for worker_idx, chunk in enumerate(chunks[idx:idx+num_workers])]
                wait(futures)

    end = time.time()
    print(f"Time taken: {end - start} seconds")

if __name__ == "__main__":
    main()