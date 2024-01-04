import numpy as np
import pandas as pd
import os

dir_path = os.path.dirname(os.path.realpath("__file__"))

def save_local_tables(df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items, print_stuff):
    # Saves the transformed recharge tables to csv for manual inspection

    df_recharge_subs.to_csv(os.path.join(dir_path, "daily_report", "recharge_subscriptions.csv"))
    df_recharge_orders.to_csv(os.path.join(dir_path, "daily_report", "recharge_orders.csv"))
    df_recharge_customers.to_csv(os.path.join(dir_path, "daily_report", "recharge_customers.csv"))
    df_recharge_orders_line_items.to_csv(os.path.join(dir_path, "daily_report", "recharge_orders_line_items.csv"))
    if print_stuff: print("Saved local recharge tables")
    return

def save_daily_MRR(output_stats_full, report_date_str, print_stuff):
    # Updates or saves daily_MRR.csv

    # df to save
    df_daily_MRR_today = pd.DataFrame.from_records([output_stats_full], index=[report_date_str])
    df_daily_MRR_today.index.name = 'Report Date'

    daily_MRR_fname = os.path.join(dir_path, "daily_report", "daily_MRR.csv")
    df_daily_MRR = df_daily_MRR_today

    if not os.path.isfile(daily_MRR_fname):
        # First save
        df_daily_MRR = df_daily_MRR_today
        if print_stuff: print("Files does not exist: " + daily_MRR_fname)
    else:
        # Load the existing file
        df_daily_MRR = pd.read_csv(daily_MRR_fname, index_col = 'Report Date')
        if not (report_date_str in df_daily_MRR.index):
            # Add new record
            df_daily_MRR = pd.concat([df_daily_MRR, df_daily_MRR_today]).sort_index()
            if print_stuff: print("Added new record to: " + daily_MRR_fname)
        else:
            # Replace record
            df_daily_MRR.loc[report_date_str] = df_daily_MRR_today.loc[report_date_str]
            if print_stuff: print("Replaced daily record in: " + daily_MRR_fname)
    
    df_daily_MRR.to_csv(daily_MRR_fname)
    if print_stuff:
        print("Saved: " + daily_MRR_fname)
        print()
    return
