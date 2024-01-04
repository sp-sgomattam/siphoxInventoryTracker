
# from oauth2client.service_account import ServiceAccountCredentials
# import gspread

# # define the scope
# scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# # add credentials to the account
# creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

# # authorize the clientsheet 
# client = gspread.authorize(creds)

import gspread
import pandas as pd
# connect got Google Sheets
gc = gspread.service_account(filename="credentials.json")

# list all available spreadsheets
# spreadsheets = gc.openall()
# for s in spreadsheets:
#     print(s.title, s.url)

sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1bsnOcSBw-6lfB_c9CTaUJmCil_XrEVIshjS3YPOIPPM')
ws = sh.worksheet('Sheet1')
dff = pd.DataFrame(ws.get_all_records())

#####################################
############3 Inputs 3###############

top = r'D:\Users\sipho\SiPhox Dropbox\New Dropbox\Lab\Stations\All-Readers'
earliest_date = '2023/03/09' # YY/MM/DD
# reduced version for SiPhox Health
display_columns = [0, 41, 37] # panel name, result crp, and units

#####################################
#####################################

import os
import time
import csv
from datetime import datetime
import re

chip_ids = []
exp_dates = []
exp_times = []
valid_dirs = []
for root, dirs, files in os.walk(top, topdown=True):
    if 'tlv' in dirs:
        folder_name = root.split("\\")[-1]

        if "DR0" in folder_name:
            continue

        result_file = os.path.join(os.path.join(root, "result.log"))
        file_stat = os.stat(result_file)
        result_size = file_stat.st_size

        # If result file is not empt result file and more than 40 tlvs (200 sec), count 
        if result_size > 40 and len(os.listdir(os.path.join(root, "tlv"))) > 40:

            timestamp = folder_name[-18:]
            folder_name_split = re.split("-|_|h|m|s", folder_name)[:-1]
            chip_id = folder_name_split[1]
            chip_ids.append(chip_id)

            exp_date = datetime(*[int(x) for x in folder_name_split[-6:]])
            exp_date = "/".join(folder_name_split[-6:-3])
            exp_time = ":".join(folder_name_split[-3:])

            exp_dates.append(exp_date+"-"+exp_time)
            exp_times.append(exp_time)

            valid_dirs.append(root[67:])
columns = ['Chip ID', 'Experiment date', 'Experiment time', 'Directory path']
df = pd.DataFrame(list(zip(chip_ids, exp_dates, exp_times, valid_dirs)), columns=columns)
df['Experiment date'] = pd.to_datetime(df['Experiment date'], infer_datetime_format=True, format='%y/%m/%d-%H:%M:%S')
df = df.sort_values(by=['Experiment date'])
df2 = df[(df['Experiment date'] > earliest_date+"-"+"00:00:00")]
df2['Experiment date'] = df2['Experiment date'].apply(lambda x: datetime.strftime(x, '%y/%m/%d'))

df3 = df2.copy().reset_index()

# from pathlib import Path
# source_path = os.path.join(Path.home(), 'SiPhox Dropbox/New Dropbox/Lab/Stations')
# source_path = r"D:\Users\sipho\SiPhox Dropbox\New Dropbox\Lab\Stations\"

result_s = []
for row in df3["Directory path"]:
    experiment_filename = os.path.join(top, row)
    result_log = os.path.join(experiment_filename, "result.log")
    with open(result_log) as f:
        result_lines = f.readlines()

        k, v = [], []

        for i in range(len(result_lines)):
            line = result_lines[i].split(',')
            
            # Processing a Result File
            if len(result_lines) == 29: # New format with rmse of result.log
                if i == 0 or i == 6:
                    continue 
                elif i == 1:
                    k.append("Panel")
                    v.append(line[1])
                elif i == 2:
                    k.append("t start")
                    v.append(float(line[1]))
                    k.append("t end")
                    v.append(float(line[2]))
                elif i == 3 or i == 4  or i == 5:
                    k.append(line[0])
                    v.append(line[1])
                elif i in range(7,22):
                    k.append(line[0])
                    v.append(line[1])
                    k.append(line[0])
                    v.append(float(line[2]))
                elif i == 22 or i == 23 or i == 24 or i == 25:
                    k.append(line[0])
                    v.append(line[1])
                elif i == 27 or i == 28:
                    k.append(line[0])
                    try:
                        v.append(round(float(line[1]),3))
                    except:
                        v.append(line[1])

        result_s.append(pd.Series(v))

result_df = pd.concat(result_s, axis=1)[::-1].transpose()
result_df.columns = k[::-1]

result_df = pd.concat(result_s, axis=1).iloc[display_columns].transpose()
result_df.columns = [k[i] for i in display_columns]

print(result_df)

df_final = pd.concat([df3, result_df], axis=1)
del df_final["index"]
# Append missing rows
if len(df_final) != len(dff):
    result_diff = len(df_final) - len(dff)
    print("==== Adding %s rows to Siphox Home Demo v2 Sheet" % result_diff)
    # for row in df_final.iloc[-result_diff:].rows:
    #     ws.append_row([str(x) for x in list(df_final.iloc[row].values)])
    for irow in range(result_diff):
        ws.append_row([str(x) for x in list(df_final.iloc[-result_diff+irow].values)])

# filepath = r'C:/Users/sipho/SiPhox Dropbox/New Dropbox/Lab/Stations/Flowcell Stations/Demo Laptop/Demo results/Integration 8/I8_External_Demo_v0_results.xlsx'
# df_final.to_excel(filepath)
# split into common settings and results

test = 0