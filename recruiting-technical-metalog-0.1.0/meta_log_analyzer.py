import json
import pandas as pd
from datetime import date


def read_log(log_path):
    """
    Opens the log file and transforms json to pandas dataframe
    and persisting data in a csv
    
    Args:
        log_path (str): path of input file
        
    Returns
        df : pandas dataframe
    """
    
    with open(log_path, 'r') as f:
        Lines = f.readlines()
    
    pair_list = []

    for line in Lines:
        line_json = json.loads(line)
        pair_list.append(line_json)
    
    today = date.today()    
    df_pair_list = pd.DataFrame.from_records(pair_list)
    df_pair_list.to_csv(f'pair_list_{today.strftime("%Y%m%d")}.csv')
    
    return df_pair_list


def calculate_thread_runtime(df_pair_list):
    """
    Calculates the runtime for thread grouped by MetacacheTread number
    
    Args:
        df_pairs_list (Dataframe) of log file
        
    Returns
        df :  dataframe with original columns with calculated run_times
    """
    
    list_of_df = []

    for i in range(10):
        # for each thread in thread pool
        thread = f'MetacacheThread-{i}'
        a = df_pair_list[df_pair_list['thread']== thread]
        # convert to datetime 
        a['timestamp'] = pd.to_datetime(a['timestamp'], infer_datetime_format=True)
        # find difference from previous and current
        a['run_time']=a['timestamp'].diff()
        # shift the result of .diff to point to current and next
        a.loc[:, 'run_time'] = a.run_time.shift(-1)
        list_of_df.append(a)

    # concat the list of thread df into one big_df
    big_df = pd.concat(list_of_df)

    return big_df


def calculate_total_time(big_df):
    """
    Calculates the total wall-clock time of a run of threads
    
    Args: 
        big_df (Dataframe) - complete dataframe with calculated run_times
    
    Returns
        datetime - total_time run time from starting thread 
                    to no jobs left to thread terminating
    """
    
    start=big_df['timestamp'].min()
    stop=big_df['timestamp'].max()
    total_time = stop - start
    
    return total_time


# This is to silence the SettingWithCopyWarning to reduce vebose warnings
pd.set_option('mode.chained_assignment', None)

pair_list_df = read_log('input/metadata_update.log')
calculated_runtimes_df = calculate_thread_runtime(pair_list_df)

total_wall_clock = calculate_total_time(calculated_runtimes_df)
print(total_wall_clock)