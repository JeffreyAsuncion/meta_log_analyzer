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
    pair_list = []
    count = 0
    number_of_threads = 10
    list_job_run_df = []
    
    with open(log_path, 'r') as f:
        Lines = f.readlines()
    
    for line in Lines:
        line_json = json.loads(line)
        if "pair" in line_json:
            pair_list.append(line_json)
        else:
            count += 1
            pair_list.append(line_json)
            if count == 20:
                # covert line_json to df
                job_run_df = pd.DataFrame.from_records(pair_list)
                # add df to list_of_job_df
                list_job_run_df.append(job_run_df)
                # clear pair_list for next run
                pair_list = []
                count = 0
                
    # previous version df_pair_list
    return len(list_job_run_df)


def calculate_thread_runtime(df_pair_list):
    """
    Calculates the runtime for thread grouped by MetacacheTread number
    
    Args:
        df_pairs_list (Dataframe) - df_pairs_list per run
        
    Returns
        df :  dataframe with original columns with calculated run_times
    """
    
    list_of_df = []

    for i in range(10):
        thread = f'MetacacheThread-{i}'
        a = df_pair_list[df_pair_list['thread']== thread]
        a['timestamp'] = pd.to_datetime(a['timestamp'], infer_datetime_format=True)
        a['run_time']=a['timestamp'].diff()
        a.loc[:, 'run_time'] = a.run_time.shift(-1)
        list_of_df.append(a)

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


def find_longest_running_pair(big_df):
    """
    Sorts the Dataframe by runtime column in descending order 
    to find the longest running pair
    
    Args:
        big_df (Data) - complete dataframe with calculated run_times
    
    Return
        df - dataframe with one record with columns pair and run_time
    """
    
    Pair_longest = big_df.sort_values(by=['run_time'], ascending=False).head(1)
    
    return Pair_longest[["pair","run_time"]]


def find_gt_ten_mins(big_df):
    """
    Filters the Dataframe with run_times greater than 10 mins
    
    Args:
        big_df (Data) - complete dataframe with calculated run_times
    
    Return
        df - dataframe with columns pair and run_time with run_time less 10 mins
    """
    
    GT_ten_mins = big_df[big_df['run_time']>'0 days 00:10:00.000000']
    
    return GT_ten_mins[["pair","run_time"]]


def write_results(total_wall_clock, longest_running_pair, greater_than_ten, writePath):
    """
    Writes results for total_wall_clock, longest_running_pair, greater_than_ten
    to file to report_<today's date> in writePath path
    
    Args:
        total_wall_clock (datatime) - result of calculate_total_time()
        
        longest_running_pair (Dataframe) - results of find_longest_running_pair()
        
        greater_than_ten (Dataframe) - results of find_gt_ten_mins()
        
    Return
        None
    """
    
    with open(writePath, 'w') as f:
        longest_running_pair_String = longest_running_pair.to_string(header=False, index=False)
        greater_than_ten_String = greater_than_ten.to_string(header=False, index=False)
        f.write("\n\n\n")
        f.write("The total wall-clock running time\n"+"*"*60+"\n")
        f.write(str(total_wall_clock))
        f.write("\n\nThe pair that took the longest\n"+"*"*60+"\n")
        f.write(longest_running_pair_String)
        f.write("\n\nAll pairs that took longer than 10 minutes\n"+"*"*60+"\n")
        f.write(greater_than_ten_String)
    

# This is to silence the SettingWithCopyWarning to reduce vebose warnings
pd.set_option('mode.chained_assignment', None)


print(read_log('input/metadata_update-extension.log'))

# pair_list_df = read_log('input/metadata_update-extension.log')
# calculated_runtimes_df = calculate_thread_runtime(pair_list_df)

# total_wall_clock = calculate_total_time(calculated_runtimes_df)
# longest_running_pair = find_longest_running_pair(calculated_runtimes_df)
# greater_than_ten = find_gt_ten_mins(calculated_runtimes_df)

# today = date.today()    
# writePath = f'report_{today.strftime("%Y%m%d")}.txt'
# write_results(total_wall_clock, longest_running_pair, greater_than_ten, writePath)
