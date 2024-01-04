import numpy as np
import pandas as pd
import requests
import json
import time
import datetime
import os

recharge_api_token = "sk_1x1_c4cd5a749005540354a86a53e11814100ce43ad744a5386a7b5539762a8e0e27"
headers = {
    "X-Recharge-Version": "2021-11",
    "Accept": "application/json",
    "X-Recharge-Access-Token": recharge_api_token}

dir_path = os.path.dirname(os.path.realpath(__file__))

# Get/Put/Post including retries, borrowed from https://github.com/BuluBox/recharge-api/blob/master/recharge/resources.py
def http_get(self, url):
    response = requests.get(url, headers=self.headers)
    self.log(url, response)
    if response.status_code == 429:
        time.sleep(1)
        return self.http_get(url)
    return response.json()

def http_put(self, url, data):
    response = requests.put(url, json=data, headers=self.headers)
    self.log(url, response)
    if response.status_code == 429:
        time.sleep(1)
        return self.http_put(url, data)
    return response.json()

def http_post(self, url, data):
    response = requests.post(url, json=data, headers=self.headers)
    self.log(url, response)
    if response.status_code == 429:
        time.sleep(1)
        return self.http_post(url, data)
    return response.json()

def get_next_url(response_data, base_url):
    # Get URL for the next 250 records
    next_url = None
    if response_data.get('next_cursor') is not None:
        next_cursor = response_data['next_cursor']
        next_url = f"{base_url}&page_info={next_cursor}"
    return next_url


def get_records(base_url, dict_string = 'subscriptions'):
    # Get all records, including multiple calls if there are more than 200
    # dict_string = 'subscriptions' or 'orders'
    all_records = []
    url = base_url
    while True:
        # Get the data for this page
        result = requests.get(url, headers=headers)
        data = json.loads(result.text)

        # Add the results to our result set
        for s in data.get(dict_string, []):
            all_records.append(s)

        if not data.get('next_cursor'):
            # Do not loop again, no more records available
            break
        else:
            # Update the URL for the next page
            url = get_next_url(data, base_url)
            if not url:
                # no next url
                break
            # Sleep to avoid hitting rate limits
            time.sleep(0.5)

    return all_records

def get_recharge_tables():
    # Get raw tables from Recharge
    base_url_subs = "https://api.rechargeapps.com/subscriptions?limit=250"    
    df_recharge_subs = pd.DataFrame.from_dict(get_records(base_url_subs, dict_string = 'subscriptions'))

    base_url_orders = "https://api.rechargeapps.com/orders?limit=250"
    df_recharge_orders = pd.DataFrame.from_dict(get_records(base_url_orders, dict_string = 'orders'))

    base_url_customers = "https://api.rechargeapps.com/customers?limit=250"
    df_recharge_customers = pd.DataFrame.from_dict(get_records(base_url_customers, dict_string = 'customers'))

    return df_recharge_subs, df_recharge_orders, df_recharge_customers

def get_COGS_vs_sku():
    fname = os.path.join(dir_path, "COGS_vs_sku.csv")
    df_COGS_vs_sku = pd.read_csv(fname)
    return df_COGS_vs_sku

def siphoxify_tables(df_recharge_subs, df_recharge_orders, df_recharge_customers, df_COGS_vs_sku):
    # Modify recharge tables to match how they look in ActionDesk
    
    #############################
    # Subscriptions
    recharge_subs_cols = [
                    'id', 
                    'cancelled_at', 
                    'created_at',	
                    'customer_id', 
                    'order_interval_frequency', 
                    'order_interval_unit', 
                    'price',
                    'product_title',
                    'status',
                    'address_id',
                    'next_charge_scheduled_at'
                    ]
    recharge_subs_date_cols = [
                    'cancelled_at', 
                    'created_at',
                    'next_charge_scheduled_at'
                    ]
    recharge_subs_numeric_cols = [
                    'id',
                    'price', 
                    'order_interval_frequency'
                    ]
    recharge_subs_str_cols = [
                    'order_interval_unit', 
                    'product_title',
                    'status'
                    ]
    df_recharge_subs = df_recharge_subs[recharge_subs_cols].copy(deep=True) # Needed the copy when this sits in a different function than get_records()
    df_recharge_subs[recharge_subs_date_cols] = df_recharge_subs[recharge_subs_date_cols].apply(pd.to_datetime)
    df_recharge_subs[recharge_subs_numeric_cols] = df_recharge_subs[recharge_subs_numeric_cols].apply(pd.to_numeric)
    df_recharge_subs.rename(columns = {'id': 'subscription_id'}, inplace=True)

    df_recharge_customers.rename(columns = {'id': 'customer_id'}, inplace=True)

    # Flag fake/test customers
    customer_blacklist = ["flow@getquantify.io","tests@getquantify.io","colin@siphox.com"]
    df_recharge_customers["is_test"] = df_recharge_customers.apply(lambda row: row['email'] in customer_blacklist, axis=1)
    # Flag fake/test subscriptions based on customer_id
    df_recharge_subs['is_test'] = df_recharge_subs['customer_id'].map(df_recharge_customers.set_index('customer_id')['is_test'])
    # Flag fake/test orders based on subscription_id
    df_recharge_orders['customer_id'] = df_recharge_orders['customer'].apply(lambda x: x['id'])
    df_recharge_orders['is_test'] = df_recharge_orders['customer_id'].map(df_recharge_customers.set_index('customer_id')['is_test'])
    df_recharge_orders = df_recharge_orders.astype({'is_test': 'bool'}) # not sure why it wasn't already bool
    # `is_test` is NaN when the customer_id does not exist in the customer table for some reason. Map those to False.
    # TODO Check why some customers don't exist. They seem to be from the earliest days.
    df_recharge_orders.loc[df_recharge_orders['is_test'].isnull(), 'is_test'] = False
    # Remove fake subscriptions and orders
    df_recharge_subs = df_recharge_subs[~df_recharge_subs['is_test']]
    df_recharge_orders = df_recharge_orders[~df_recharge_orders['is_test']]

    # Add SiPhox columns following the Actiondesk table

    # Simplified Product Title
    # =LOOKUP(#product_title,'Matching table Product Titles'!#'Product Title','Matching table Product Titles'!#'Simplified Product Title')
    product_match = {
        "Female Membership: Monthly": "Tier III Monthly",
        "Female Membership: Quarterly":	"Tier II Quarterly",
        "Female Membership: Tier I": "Tier I Yearly",
        "Female Membership: Yearly": "Tier I Yearly",
        "Male Membership: Monthly": "Tier III Monthly",
        "Male Membership: Quarterly": "Tier II Quarterly",
        "Male Membership: Tier I": "Tier I Yearly",
        "Male Membership: Tier II": "Tier II Quarterly",
        "Male Membership: Tier III": "Tier III Monthly",
        "Male Membership: Yearly": "Tier I Yearly",
        "Monthly Membership": "Tier III Monthly",
        "Quarterly Membership": "Tier II Quarterly",
        "Yearly Membership": "Tier I Yearly",
        "Yearly Subscription": "Tier I Yearly",
        "New Membership Test Product": "", # ignore
        "2 Kits Every Month Membership": "Tier III Monthly",
        "Unlimited Membership": "Unlimited Membership", # new category
        "Unlimited Membership I": "Unlimited Membership I", # new category
        "Unlimited Membership 2": "Unlimited Membership", # new category
        "Unlimited Membership v0": "Unlimited Membership", # new category
        "Unlimited Membership II": "Unlimited Membership II",
        "Unlimited Membership III": "Unlimited Membership III",
        "Unlimited Membership IV": "Unlimited Membership IV",
        "Blue Light Blockers": "" # ignore
    }
    df_recharge_subs['Simplified Product Title'] = df_recharge_subs['product_title'].apply(lambda x: product_match[x])

    # MRR
    # =IFS(
    #   AND(#order_interval_frequency=12,#order_interval_unit="month"),#price/12,
    #   AND(#order_interval_frequency=3,#order_interval_unit="month"),#price/3,
    #   AND(#order_interval_frequency=30,#order_interval_unit="day"),#price)
    def MRR(row):  
        if (row['order_interval_frequency'] == 12) & (row['order_interval_unit'] == "month"):
            if row['Simplified Product Title'] == "Unlimited Membership":
                # Special case, assume $95 refill every 3 months.
                return row['price']/12 + 95/3
            else:
                return row['price']/12
        elif row['Simplified Product Title'] == "Unlimited Membership I" or row['Simplified Product Title'] == "Unlimited Membership IV":
            # Special case, assume $95 refill every 3 months.
            return row['price']/6 + 95/3
        elif row['Simplified Product Title'] == "Unlimited Membership II":
            return row['price']
        elif row['Simplified Product Title'] == "Unlimited Membership III":
            return row['price']
        elif (row['order_interval_frequency'] == 3) & (row['order_interval_unit'] == "month"):
            return row['price']/3
        elif (row['order_interval_frequency'] == 30) & (row['order_interval_unit'] == "day"):
            return row['price']
        return
    df_recharge_subs['MRR'] = df_recharge_subs.apply(lambda row: MRR(row), axis=1)



    #############################
    # Order line items:

    # Make table with one line for each line_item. Keep the order_id as a column.
    df_recharge_orders.rename(columns={'id':'order_id'}, inplace=True)
    df_recharge_orders_line_items_dicts = pd.DataFrame(df_recharge_orders.line_items.tolist(), index=df_recharge_orders.order_id).stack().reset_index([0, 'order_id'])
    df_recharge_orders_line_items_dicts.columns = ['order_id', 'line_items']
    df_recharge_orders_line_items = pd.DataFrame.from_records(df_recharge_orders_line_items_dicts.line_items)
    df_recharge_orders_line_items.insert(0, 'order_id', df_recharge_orders_line_items_dicts.order_id)


    # Add a column 'COGS' to the line items table by looking up 'sku' in df_COGS_vs_sku, and multiply by the line item quantity
    df_recharge_orders_line_items['COGS'] = df_recharge_orders_line_items['sku'].map(df_COGS_vs_sku.set_index('sku')['COGS']) * df_recharge_orders_line_items['quantity']


    # For calculating revenue we need to use orders, not line items. 
    # This is because discounts are taken off at the order level, not at the line item.
    # This means we should extract the info we need from the line items, and add it to orders as new columns, and do our calculations there.
    # Need to extract the subscriber_id for the subscription products, and the Simplified Product Title.
    # That means both line items will get counted as one Simplified Product Title, they cannot be two different type.
    # For now assume we can use the first line item for orders that have multiple line items.

    ## Old way: Get the first line item for each order
    # df_recharge_orders_line_items_single = df_recharge_orders_line_items.drop_duplicates(subset='order_id', keep='first')
    ## Instead, get the line item with the highest price. This is the one that is most likely to be the subscription product.
    df_recharge_orders_line_items['total_price'] = df_recharge_orders_line_items['total_price'].astype(float)
    df_recharge_orders_line_items_single = df_recharge_orders_line_items.sort_values(by='total_price', ascending=False).drop_duplicates(subset='order_id', keep='first').sort_values(by='order_id', ascending=False)
    df_recharge_orders_line_items_single.set_index('order_id', inplace=True)

    # Pull new columns into orders table
    df_recharge_orders['subscription_id'] = df_recharge_orders['order_id'].apply(lambda x: df_recharge_orders_line_items_single.loc[x, 'purchase_item_id'])
    # Map to Simplified Product Title so we can compute revenue for each type
    product_match_line_items = {
            "quantify_mens_health_kit_tier_I": "Tier I Yearly",
            "quantify_womens_health_kit_tier_I": "Tier I Yearly",
            "quantify_mens_health_kit_tier_II": "Tier II Quarterly",
            "quantify_womens_health_kit_tier_II": "Tier II Quarterly",
            "quantify_mens_health_kit_tier_III": "Tier III Monthly",
            "quantify_womens_health_kit_tier_III": "Tier III Monthly",
            "test_product": "", # ignore
            "quantify-starter-kit": "", # ignore
            "quantify_health_kit_x2_monthly": "Tier III Monthly",
            "unlimited_membership": "Unlimited Membership", # new category
            "unlimited_membership-147-2-1": "Unlimited Membership I", # new category
            "unlimited_membership-16-12-1": "Unlimited Membership II",
            "quantify_starter_kit-2": "Unlimited Membership", # new category
            "unlimited_membership-2": "Unlimited Membership", # new category
            "unlimited_membership-16-12-0": "Unlimited Membership III",
            "unlimited_membership-147-2-0": "Unlimited Membership IV",
            "blue_light_blockers": "", # ignore
            "quantify_cgm": "", # ignore
            "quantify_health_hormone_kit": "" # ignore
            }
    df_recharge_orders['top_sku'] = df_recharge_orders['order_id'].map(df_recharge_orders_line_items_single['sku'])
    df_recharge_orders['Simplified Product Title'] = df_recharge_orders['top_sku'].apply(lambda x: product_match_line_items[x])
    # Make a new column "COGS" in df_recharge_orders by summing the COGS of the line items with the same order_id
    df_recharge_orders['COGS'] = df_recharge_orders['order_id'].map(df_recharge_orders_line_items.groupby('order_id')['COGS'].sum())

    # Set a new column 'discount_code' in df_recharge_orders. If df_recharge_orders['discounts'] == [] then set an empty string. Else, set df_recharge_orders['discounts'][0]['code']
    df_recharge_orders['discount_code'] = df_recharge_orders['discounts'].apply(lambda x: x[0]['code'] if len(x) > 0 else "")
    # Boolean for whether we are overriding the order COGS
    df_recharge_orders['override_order_COGS'] = df_recharge_orders['discount_code'] == df_recharge_orders['top_sku'].map(df_COGS_vs_sku.set_index('sku')['discount_code_override'])
    # If we are overriding the order COGS, then set the order COGS to the override value
    df_recharge_orders.loc[df_recharge_orders['override_order_COGS'] == True, 'COGS'] = df_recharge_orders[df_recharge_orders['override_order_COGS'] == True]['top_sku'].map(df_COGS_vs_sku.set_index('sku')['COGS_override'])

    # Change types
    recharge_order_date_cols = [
                    'created_at',
                    'processed_at',
                    'scheduled_at',
                    'updated_at'
                    ]
    recharge_order_numeric_cols = [
                    'subtotal_price',
                    'total_discounts',
                    'total_duties',
                    'total_line_items_price',
                    'total_price',
                    'total_refunds',
                    'total_tax',
                    'total_weight_grams'
                    ]
    df_recharge_orders[recharge_order_date_cols] = df_recharge_orders[recharge_order_date_cols].apply(pd.to_datetime)
    df_recharge_orders[recharge_order_numeric_cols] = df_recharge_orders[recharge_order_numeric_cols].apply(pd.to_numeric)

    # # of orders associated to subscriptions
    # =LOOKUP(#id,'Pivot: # of orders per subscription'!#subscription_id,'Pivot: # of orders per subscription'!#'COUNTUNIQUE of order_id')
    orders_per_subscription = df_recharge_orders.value_counts("subscription_id")
    df_recharge_subs["# of orders associated to subscriptions"] = df_recharge_subs["subscription_id"].apply(lambda x: orders_per_subscription[x] if x in orders_per_subscription else 0)


    # #######
    # ## Old approach
    # # Make line_items objects into their own table
    # df_recharge_orders_line_items = pd.DataFrame.from_records(df_recharge_orders['line_items'].explode())
    # # NOTE I can only grab things from orders if there is exactly one line item in every order, which seems to be the case so far
    # # TODO This is broken now with two line items on some orders. Need to look these things up, not just copy them in order
    # df_recharge_orders_line_items['order_id'] = df_recharge_orders['id']
    # df_recharge_orders_line_items['order_total_price'] = df_recharge_orders['total_price'] # This value has already subtracted any discount so it can be used for revenue
    # df_recharge_orders_line_items['order_created_at'] = df_recharge_orders['created_at'] # This value has already subtracted any discount so it can be used for revenue
    # # Map to Simplified Product Title so we can compute revenue for each type
    # product_match_line_items = {
    #     "quantify_mens_health_kit_tier_I": "Tier I Yearly",
    #     "quantify_womens_health_kit_tier_I": "Tier I Yearly",
    #     "quantify_mens_health_kit_tier_II": "Tier II Quarterly",
    #     "quantify_womens_health_kit_tier_II": "Tier II Quarterly",
    #     "quantify_mens_health_kit_tier_III": "Tier III Monthly",
    #     "quantify_womens_health_kit_tier_III": "Tier III Monthly",
    #     "test_product": "", # ignore
    #     "quantify-starter-kit": "", # ignore
    #     "quantify_health_kit_x2_monthly": "Tier III Monthly",
    #     "unlimited_membership": "Unlimited Membership", # new category
    #     "quantify_starter_kit-2": "Unlimited Membership" # new category
    #     }
    # df_recharge_orders_line_items['Simplified Product Title'] = df_recharge_orders_line_items['sku'].apply(lambda x: product_match_line_items[x])

    # Rename some columns to match ActionDesk
    # purchase_item_id: The Subscription or Onetime ID associated with the line_item.
    # 'price' in the ActionDesk table appears to be `unit_price` here. I will not rename it.
    # df_recharge_orders_line_items.rename(columns = {'purchase_item_id': 'subscription_id'}, inplace=True)
    # recharge_order_line_items_date_cols = [
    #                 'order_created_at'
    #                 ]
    # recharge_order_line_items_numeric_cols = [
    #                 'order_total_price'
    #                 ]
    # df_recharge_orders_line_items[recharge_order_line_items_date_cols] = df_recharge_orders_line_items[recharge_order_line_items_date_cols].apply(pd.to_datetime)
    # df_recharge_orders_line_items[recharge_order_line_items_numeric_cols] = df_recharge_orders_line_items[recharge_order_line_items_numeric_cols].apply(pd.to_numeric)

    #############################
    # Back to Subscriptions:

    # # of orders associated to subscriptions
    # =LOOKUP(#id,'Pivot: # of orders per subscription'!#subscription_id,'Pivot: # of orders per subscription'!#'COUNTUNIQUE of order_id')
    # orders_per_subscription = df_recharge_orders_line_items.value_counts("subscription_id")
    # df_recharge_subs["# of orders associated to subscriptions"] = df_recharge_subs["subscription_id"].apply(lambda x: orders_per_subscription[x] if x in orders_per_subscription else 0)



    # subscriptions "Is test order"
    # =LOOKUP(#id,'Recharge order line items'!#subscription_id,'Recharge order line items'!#'Is test order')
    # Comes from order_line_items "Is test order"
    # =LOOKUP(#order_id,'Recharge orders'!#id,'Recharge orders'!#'Is test order')
    # Comes from orders "Is test order"
    # =LOOKUP(#shopify_order_id,'Shopify orders - augmented'!#order_id,'Shopify orders - augmented'!#'Is test order')
    # Comes from shopify_orders_augmented "Is test order" (which we will need to grab)
    # =INCLUDE("test order",#tags)

    # punt on this one


    return df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items

def get_siphox_recharge_tables():
    df_recharge_subs, df_recharge_orders, df_recharge_customers = get_recharge_tables()
    df_COGS_vs_sku = get_COGS_vs_sku()
    df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items = siphoxify_tables(df_recharge_subs, df_recharge_orders, df_recharge_customers, df_COGS_vs_sku)

    return df_recharge_subs, df_recharge_orders, df_recharge_customers, df_recharge_orders_line_items
