import pandas as pd
import threading
import numpy as np
import time
from Rebalancing.Etherscan_data_get import ERC20TransferTx


# address to detect
AddressToDetect = '0xdbf5e9c5206d0db70a90108bf936da60221dc080'
APIkey_list=['BJ3446K621AECIYKDPFU1HXJ629NUU6Z7N',
             '89MJ5EAXFJ1AMFP18MIGRD1TTCSEV2PU85',
             'ZGH8WV5B2EV1VG382BJ3CKFQFH4RFZR3SS',
             '1VI27F7S4FBH8QDK7U1SCP9QU9FCA4C5VP',
             'NXMJ9JTDX8I4JHKVNENM2E243JPKIM254H',
             '7ATHQS171N3KHIN1TYYSE9VHGPV8UA7D13',
             'DEGPT733HF5K4U14Q3QX2DSP7BGEAWXXHU',
             '1IENSM9MCTBTJGYR9KDIG1NSHHRNSNI849',
             'UHQ2A4PF2YDESZGJKBIYDZWM6AVKUQ94PA',
             'C3J9P9FTIZDJ92TQAYB3ID41UNRUTTH7GS',
             'V624UDGTXPRKJX4ZNGZ1T52IZVMSJE46RE',
             '3VCTVI8RU73STIAGEZPQ6QV5Y6TN66T23S']


# Sample DataFrame
df = pd.read_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_FromCEX.csv', index_col=0)
df = df.iloc[20000:,:]


max_calls_per_second = 1
# Minimum time interval between calls in seconds
interval_call = 1 / max_calls_per_second

# Shared variables
results = []
results_lock = threading.Lock()
progress_counter = 0
counter_lock = threading.Lock()

def my_function(df_chunk, APIkey):
    global progress_counter
    local_result = pd.DataFrame()

    for index, row in df_chunk.iterrows():
        start_time = time.time()  # count time
        APIkey = APIkey
        walletAddress = AddressToDetect
        contractAddress = row['contract_address']
        startblock = int(row['evt_block_number'])
        endblock = int(row['evt_block_number'])

        data_contract_i = ERC20TransferTx(APIkey,
                                          walletAddress,
                                          contractAddress,
                                          startblock,
                                          endblock,
                                          sort='desc',
                                          page=1,
                                          offset=500
                                          )

        result_row = pd.DataFrame(data_contract_i['result'])
        local_result = pd.concat([local_result, result_row], ignore_index=True)

        elapsed_time = time.time() - start_time
        sleep_time = max(0, interval_call - elapsed_time)
        time.sleep(sleep_time)

        with counter_lock:
            progress_counter += 1

    with results_lock:
        results.append(local_result)

def worker(df_chunk, APIkey, done_event):
    my_function(df_chunk,APIkey)
    done_event.set()  # Signal that this worker is done

def progress_reporter(stop_event, interval=1):
    while not stop_event.is_set():
        time.sleep(interval)
        with counter_lock:
            print(f"Progress: {progress_counter} rows processed")

# Split the DataFrame into chunks for each thread
num_threads = 9
df_chunks = np.array_split(df, num_threads)

# Create an event for each worker to signal when done
done_events = [threading.Event() for _ in range(num_threads)]

# Create and start worker threads
threads = []
for i in range(num_threads):
    thread = threading.Thread(target=worker, args=(df_chunks[i], APIkey_list[i], done_events[i]), name=f"Worker-{i+1}")
    thread.start()
    threads.append(thread)

# Create and start progress reporter thread
stop_event = threading.Event()
progress_thread = threading.Thread(target=progress_reporter, args=(stop_event,), name="ProgressReporter")
progress_thread.start()

# Wait for all worker threads to finish
for event in done_events:
    event.wait()

# Signal the progress reporter to stop
stop_event.set()
progress_thread.join()

print("Worker threads have been stopped")

# Combine all results
final_result = pd.concat(results, ignore_index=True)
final_result = final_result.sort_values(by='blockNumber', ascending=False).reset_index(drop=True)
final_result.to_csv('/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/Rebalancing/Tx_wintermute_FromCEX_detailed_20000_end.csv')
print(f"Final Progress: {progress_counter} rows processed")
print(f"Final Result:\n{final_result}")
