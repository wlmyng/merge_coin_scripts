import requests
import json
import argparse
import queue
import threading
from typing import Union, Optional, List
import base64
import logging

from pysui import __version__, SuiConfig, SyncClient, SuiAddress, SuiRpcResult
from pysui.sui.sui_txn import SyncTransaction
from pysui.sui.sui_types import bcs
from pysui.sui.sui_types.scalars import SuiString
from pysui.sui.sui_txn.signing_ms import SigningMultiSig
from pysui.sui.sui_txresults.complex_tx import TxInspectionResult
from pysui.sui.sui_txn.transaction import _DebugInspectTransaction
from pysui.sui.sui_txresults.single_tx import ObjectRead
from pysui.sui.sui_builders.exec_builders import (
    ExecuteTransaction,
)
from pysui.sui.sui_builders.base_builder import (
    SuiRequestType,
)
from pysui.sui.sui_txresults import SuiCoinObject

logger = logging.getLogger("pysui.sync_transaction")
if not logging.getLogger().handlers:
    logger.addHandler(logging.NullHandler())
    logger.propagate = False

class ModifiedSyncTransaction(SyncTransaction):
    def execute_with_multiple_gas(
        self,
        *,
        gas_budget: Optional[Union[str, SuiString]] = "1000000",
        options: Optional[dict] = None,
        use_gas_objects: Optional[List[Union[str, SuiCoinObject]]] = None,
    ) -> Union[SuiRpcResult, ValueError]:
        assert not self._executed, "Transaction already executed"
        gas_budget = gas_budget if gas_budget else "1000000"
        tx_b64 = base64.b64encode(
            self._build_for_execute_multiple_gas(gas_budget, use_gas_objects).serialize()
        ).decode()

        exec_tx = ExecuteTransaction(
            tx_bytes=tx_b64,
            signatures=self.signer_block.get_signatures(
                client=self.client, tx_bytes=tx_b64
            ),
            options=options,
            request_type=SuiRequestType.WAITFORLOCALEXECUTION,
        )
        iresult = self.client.execute(exec_tx)
        self._executed = True
        return iresult

    def _build_for_execute_multiple_gas(
        self,
        gas_budget: Union[str, SuiString],
        use_gas_objects: Optional[List[Union[str, SuiCoinObject]]] = None,
    ) -> Union[bcs.TransactionData, ValueError]:        
        # Get the transaction body
        tx_kind = self.raw_kind()
        # Get costs
        tx_kind_b64 = base64.b64encode(tx_kind.serialize()).decode()
        # Resolve sender address for inspect
        if self.signer_block.sender:
            for_sender: Union[
                SuiAddress, SigningMultiSig
            ] = self.signer_block.sender
            if not isinstance(for_sender, SuiAddress):
                for_sender = for_sender.multi_sig.as_sui_address
        else:
            for_sender = self.client.config.active_address
        try:
            # Do the inspection
            logger.debug(f"Inspecting {tx_kind_b64}")
            result = self.client.execute(
                _DebugInspectTransaction(
                    sender_address=for_sender, tx_bytes=tx_kind_b64
                )
            )
            if result.is_ok():
                result = SuiRpcResult(
                    True, "", TxInspectionResult.factory(result.result_data)
                )
            # Bad result
            else:
                logger.exception(
                    f"Inspecting transaction failed with {result.result_string}"
                )
                raise ValueError(
                    f"Inspecting transaction failed with {result.result_string}"
                )
        # Malformed result
        except KeyError as kexcp:
            logger.exception(
                f"Malformed inspection results {result.result_data}"
            )
            raise ValueError(
                f"Malformed inspection results {result.result_data}"
            )
        # if result.is_ok():
        ispec: TxInspectionResult = result.result_data
        gas_budget = (
            gas_budget if isinstance(gas_budget, str) else gas_budget.value
        )
        # Total = computation_cost + non_refundable_storage_fee + storage_cost
        gas_budget = max(ispec.effects.gas_used.total, int(gas_budget))

        # If user provided
        if use_gas_objects:
            gas_objects = []
            for use_coin in use_gas_objects:
                if isinstance(use_coin, str):
                    res = self.client.get_object(use_coin)
                    if res.is_ok():
                        object_read: ObjectRead = res.result_data
                        use_coin = SuiCoinObject.from_read_object(object_read)
                    else:
                        raise ValueError(
                            f"Failed to fetch use_gas_object {use_coin}"
                        )
                if use_coin.object_id in self.builder.objects_registry:                            
                    raise ValueError(
                        f"use_gas_object {use_coin.object_id} in use in transaction."
                    )                            
                gas_objects.append(
                    bcs.ObjectReference(
                        bcs.Address.from_str(use_coin.object_id),
                        int(use_coin.version),
                        bcs.Digest.from_str(use_coin.digest),
                    )
                )

            gas_object = bcs.GasData(gas_objects,
                bcs.Address.from_str(for_sender.owner),
                int(self._current_gas_price),
                int(gas_budget),
            )
        else:
            # Fetch the payment
            gas_object = self._sig_block.get_gas_object(
                client=self.client,
                budget=gas_budget,
                objects_in_use=self.builder.objects_registry,
                merge_coin=self._merge_gas,
                gas_price=self._current_gas_price,
            )
        if isinstance(self.signer_block.sender, SuiAddress):
            who_sends = self.signer_block.sender.address
        else:
            who_sends = self.signer_block.sender.signing_address
        return bcs.TransactionData(
            "V1",
            bcs.TransactionDataV1(
                tx_kind,
                bcs.Address.from_str(who_sends),
                gas_object,
                bcs.TransactionExpiration("None"),
            ),
        )


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

def fetch_coins(coin_queue, gas_object, owner, url, coin_type="0x2::sui::SUI", cursor=None, limit=250): # programmable transaction block limit is 512, undershoot a little    
    coins = []
    seen = set([gas_object])
    while True:
        response = get_coins(owner, url, coin_type, cursor, limit)
        if response:
            fetched_coins = response['result']['data']
            unique_coins = []            
            for coin in fetched_coins:
                if coin['coinObjectId'] not in seen:
                    unique_coins.append(SuiCoinObject.from_dict(coin))
                    seen.add(coin['coinObjectId'])
            coins.extend(unique_coins)

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
    while True:
        coins_to_merge: List[SuiCoinObject] = coin_queue.get()
        if coins_to_merge is None:
            break        
        txn = ModifiedSyncTransaction(client, initial_sender=SuiAddress(signer))        
        gas_objects = [gas_object]
        gas_objects.extend(coins_to_merge)
        result = txn.execute_with_multiple_gas(use_gas_objects=gas_objects)
        if not result.is_ok():
            print(result.result_string)
        else:
            print("ok")

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

    fetch_thread = threading.Thread(target=fetch_coins, args=(coin_queue, gas_object, signer, args.rpc_url))
    merge_thread = threading.Thread(target=merge_coins, args=(coin_queue, client, signer, gas_object))

    fetch_thread.start()
    merge_thread.start()

    fetch_thread.join()
    merge_thread.join()

if __name__ == "__main__":
    main()
    # invoke with python3 merge_coins_pubsub.py --prv-key "KEY" --signer "0xADDRESS" --gas-object "0xOBJECTID"