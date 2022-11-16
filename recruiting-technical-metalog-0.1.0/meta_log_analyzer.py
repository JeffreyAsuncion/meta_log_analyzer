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


# This is to silence the SettingWithCopyWarning to reduce vebose warnings
pd.set_option('mode.chained_assignment', None)

pair_list_df = read_log('input/metadata_update.log')


