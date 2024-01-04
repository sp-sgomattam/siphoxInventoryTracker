import os
import json
import datetime
import pandas
from tqdm import tqdm
import github_action_script as gas

## Make sure the python notebook is set to look at the arguments.json file
## Also that the new post_to_slack flag is False
start_date = "2022-10-01"
end_date = datetime.date.today() - datetime.timedelta(days = 1) # Yesterday
date_list = pandas.date_range(start_date, end_date, freq='d').to_list()

# Subscription targets by month: (monthly, yearly)
sub_targets = {
    "2022-10": (48, 48),
    "2022-11": (100, 200),
    "2022-12": (200, 200),
    "2023-01": (200, 200),
    "2023-02": (200, 200),
    "2023-03": (200, 200),
    "2023-04": (200, 200)

}


dir_path = os.path.dirname(os.path.realpath("__file__"))
# dir_path = r"C:\Users\KylePreston\SiPhox Dropbox\Kyle Preston\PC (2)\Documents\git\siphox-mrr\daily_report"
# dir_path = r"D:\\Users\\sipho\\Documents\\git\\siphox-mrr\\daily_report"
# notebook_filename = r'"' + os.path.join(dir_path, "MRR-Python.ipynb") + '"'

args_file = os.path.join(dir_path, 'daily_report', 'arguments.json')
def save_args(nb_args_dict):
    with open(args_file, 'w') as fid:
        json.dump(nb_args_dict, fid)
    return


nb_args_dict = {
    # "report_date_str": yesterday_date.strftime("%Y-%m-%d"), # "2023-01-07"
    "report_date_str": start_date,
    "load_local_tables": False, # Pull from database if False, read from local csv if True
    "save_local_tables": True, # Save tables to csv

    "save_daily_MRR": True, 
    "post_to_slack": False, 
    "save_html": False, # Save html from inside the notebook. Instead we will do this from our nbconvert command

    "monthly_target_subscriptions": 200, 
    "monthly_target_yearly_kits": 200,
    "print_stuff": False
    }
save_args(nb_args_dict)

for i, report_date in enumerate(tqdm(date_list)):
    report_date_str = report_date.strftime("%Y-%m-%d")

    # Set report date
    nb_args_dict['report_date_str'] = report_date_str
    # Set subscription targets
    (t1, t2) = sub_targets[report_date.strftime("%Y-%m")]
    nb_args_dict['monthly_target_subscriptions'] = t1
    nb_args_dict['monthly_target_yearly_kits'] = t2
    # Set data source
    if i==0:
        # First time: Pull date and save to disk
        nb_args_dict['load_local_tables'] = False
        nb_args_dict['save_local_tables'] = True
    else:
        # Other times: Load from disk
        nb_args_dict['load_local_tables'] = True
        nb_args_dict['save_local_tables'] = False
    save_args(nb_args_dict)
    # print(nb_args_dict)

    ### Old notebook stuff
    # HTML output
    # output_filename = r'"' + os.path.join(dir_path, "html_reports", nb_args_dict['report_date_str'] + "_MRR_Python.html") + '"'
    # cmd = 'jupyter nbconvert ' + notebook_filename + ' --execute --to html --no-input --no-prompt --log-level WARN --output ' + output_filename
    # No HTML
    # cmd = 'jupyter nbconvert ' + notebook_filename + ' --execute --to notebook --no-input --no-prompt --log-level WARN'
    # # print(cmd)
    # os.system(cmd)

    # New version
    gas.main()

    # Remove args file so it isn't accidentally used the next time the notebook is run
    os.remove(args_file)