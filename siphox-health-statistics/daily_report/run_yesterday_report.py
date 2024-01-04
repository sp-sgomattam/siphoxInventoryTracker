import os
import json
import datetime

## Make sure the python notebook is set to look at the arguments.json file

dir_path = os.path.dirname(os.path.realpath("__file__"))
# dir_path = r"C:\\Users\\KylePreston\\SiPhox Dropbox\\Kyle Preston\\PC (2)\\Documents\\actiondesk\\daily report"
# dir_path = r"D:\\Users\\sipho\\Documents\\git\\siphox-mrr\\daily_report"
notebook_filename = r'"' + os.path.join(dir_path, "MRR-Python.ipynb") + '"'

# Todo: args should just be a dict. That will make it easier to update

args_file = os.path.join(dir_path, 'arguments.json')
def save_args(nb_args_dict):
    with open(args_file, 'w') as fid:
        json.dump(nb_args_dict, fid)
    return

yesterday_date = datetime.date.today() - datetime.timedelta(days = 1)

nb_args_dict = {
    "report_date_str": yesterday_date.strftime("%Y-%m-%d"), # "2023-01-07"
    # "report_date_str": "2023-01-09",
    "load_local_tables": False, # Pull from database if False, read from local csv if True
    "save_local_tables": True, # Save tables to csv

    "save_daily_MRR": True, 
    "post_to_slack": True, 
    "save_html": False, # Save html from inside the notebook. Instead we will do this from our nbconvert command

    "monthly_target_subscriptions": 200, 
    "monthly_target_yearly_kits": 200
    }
save_args(nb_args_dict)

output_filename = r'"' + os.path.join(dir_path, "html_reports", nb_args_dict['report_date_str'] + "_MRR_Python.html") + '"'


import os
cmd = 'jupyter nbconvert ' + notebook_filename + ' --execute --to html --no-input --no-prompt --output ' + output_filename
print(cmd)
os.system(cmd)



# Remove args file so it isn't accidentally used the next time the notebook is run
os.remove(args_file)