import numpy as np
from scipy.stats import linregress
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os.path
import json

import recharge_tables as recharge_tables
import compute_statistics
import csv_io as csv_io

from slack_sdk.webhook import WebhookClient
url = "https://hooks.slack.com/services/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" # siphox-health-daily-statistics
webhook = WebhookClient(url)

dir_path = os.path.dirname(os.path.realpath("__file__"))

def main():
    # import plotly.express as px
    # import plotly.graph_objects as go
    # import plotly.io as pio
    # pd.options.plotting.backend = "plotly"
    # pio.renderers.default = "notebook"

    # Load arguments from json file
    # The file must contain all expected arguments
    load_args_from_json = False


    if not load_args_from_json:
        arguments = {
            'load_local_tables': False, # Pull from database if False, read from local csv if True
            'save_local_tables': False, # Save csv

            'save_daily_MRR': True,
            'save_html': True,

            'monthly_target_subscriptions': 200,
            'monthly_target_yearly_kits': 200,
            
            'print_stuff': True,

            ######################
            ## Use this to report on yesterday
            'report_date_str': str(datetime.date.today() - datetime.timedelta(days = 1))
            ## Or use this for a temporary date:
            # 'report_date_str': "2022-10-01"
            }
    else:
        # load from json file
        args_fname = os.path.join(dir_path, 'daily_report', 'arguments.json')
        if os.path.isfile(args_fname):
            with open(args_fname, 'r') as fid:
                arguments = json.load(fid)
        else:
            print("Warning: arguments.json not found!")

    # Unpack from dict
    load_local_tables = arguments['load_local_tables']
    save_local_tables = arguments['save_local_tables']
    save_daily_MRR = arguments['save_daily_MRR']
    post_to_slack = os.environ['MODE'] == 'prod'
    save_html = arguments['save_html']
    monthly_target_subscriptions = arguments['monthly_target_subscriptions']
    monthly_target_yearly_kits = arguments['monthly_target_yearly_kits']
    print_stuff = arguments['print_stuff']
    report_date_str = arguments['report_date_str']


    report_date = datetime.datetime.strptime(report_date_str, "%Y-%m-%d").date()
    if print_stuff:
        print('Report date: ' + report_date_str)
        print()
        print(arguments)

    # Load data

    if not load_local_tables:
        # Pull Recharge data
        print("Pulling recharge data ...")
        df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items = recharge_tables.get_siphox_recharge_tables()
    else:
        # Load local tables
        df_recharge_subs = pd.read_csv(os.path.join(dir_path, "daily_report", "recharge_subscriptions.csv"), index_col = 0,
                                        parse_dates=['created_at', 'cancelled_at', 'next_charge_scheduled_at'])
        df_recharge_orders = pd.read_csv(os.path.join(dir_path, "daily_report", "recharge_orders.csv"), index_col = 0,
                                        parse_dates=['created_at', 'processed_at', 'scheduled_at', 'updated_at'])
        df_recharge_orders_line_items = pd.read_csv(os.path.join(dir_path, "daily_report", "recharge_orders_line_items.csv"), index_col = 0)

    if save_local_tables:
        csv_io.save_local_tables(df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items, print_stuff)

    if print_stuff: print("Crunching numbers ...")
    output_stats_full, output_stats_display, product_stats_with_sum_display = compute_statistics.compute_statistics(
        df_recharge_subs,
        df_recharge_orders,
        report_date=report_date,
        report_date_str=report_date_str,
        monthly_target_subscriptions=monthly_target_subscriptions,
        monthly_target_yearly_kits=monthly_target_yearly_kits
    )

    if print_stuff:
        print(product_stats_with_sum_display)
        print()

    if save_daily_MRR:
        csv_io.save_daily_MRR(output_stats_full, report_date_str, print_stuff)

    # Post to slack

    blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "MRR Daily Report, " + report_date_str
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + output_stats_display.to_markdown() + "```"
                    }
                },
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "Subscriptions remaining this month: {0:d}\nYearly kits remaining this month: {1:d}".format(int(output_stats_display.loc['subs_remaining_this_month']), int(output_stats_display.loc['yearly_remaining_this_month']))
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```" + product_stats_with_sum_display.to_markdown() + "```"
                    }
                },
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "MRR excluding cancellations: " + product_stats_with_sum_display.loc['SUM', 'MRR']
                    }
                },
            ]

    if post_to_slack:
        if print_stuff: print('Start post to slack')
        response = webhook.send(
            text="MRR Daily Report, " + report_date_str,
            blocks=blocks
        )
    else:
        block_obj = {
            'blocks': blocks
        }   
        blocks_json = json.dumps(block_obj, separators=(',', ':'))
        if print_stuff:
            print('Copy JSON below and paste it here to test: https://app.slack.com/block-kit-builder')
            print(blocks_json)
    return

if __name__ == "__main__":
    main()