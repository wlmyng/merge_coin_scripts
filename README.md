# Summary
The largest bottleneck will be in reading coin objects off the rpc, especially if the limit per page is set to a small number like 50.
From one run, 69k objects were read in 592 seconds, which estimates to ~2 days to read the current number of coin objects (20m+).
The more coins we can fetch in one page, the faster the reading process will be.

Conversely, executing the merge coin transactions will be the fastest part of the process. 25000 coins can be smashed in 40 seconds with `merge_coins.py` at `num_workers=5`.

# Usage

## merge_coins_pubsub.py
A simple implementation that launches a queue for fetching pages of `0x2::sui:::SUI` for owner SuiAddress `--signer` into chunks of 500 coin object ids (PTB max input is 512, undershot a little), and another queue for executing sync transactions using `--prv-key`. Note that to minimize errors, the script does require a `--gas-object` input. At termination, you should be left with two objects, the gas object, and the final `merge_to` coin. From my tests, it takes about 3s on average to merge 500 coins on testnet, so you should be able to get through the 2m+ objects in just a little under 4 hours.

Example invocation:
```bash
python3 merge_coins_pubsub.py --prv-key "KEY" --signer "0xADDRESS" --gas-object "0xOBJECT"
```

## fetch_coins.py and merge_coins.py
As fetching coins is the most consuming bit, we can split the process into two parts, fetching and merging. 
`fetch_coins.py` will fetch all coins for a given owner address and write them to files in batches of 25000.
 `merge_coins.py` will read the file and merge the coins based on a combination of `num_workers`.

Example invocation:
```bash
python3 fetch_coins.py --owner "0xefa434f1441f5d61bfb5f3d9a26e494e8fcaef87a69cd9ce639d6b648cc8a512"
```

```bash
python3 merge_coins.py --prv-key "KEY" --signer "0xADDRESS" --gas-object "0xOBJECT" --gas-to-split "0xGAS" --num-workers 5
```