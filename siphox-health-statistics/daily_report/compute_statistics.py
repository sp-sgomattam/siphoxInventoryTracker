import pandas as pd

def compute_statistics(df_recharge_subs, df_recharge_orders, report_date, report_date_str, monthly_target_yearly_kits, monthly_target_subscriptions):
    # Clean up data https://www.notion.so/siphox/Membership-Master-List-59a6d3a319d74132bc16e06d87fe2218
    valid_products = ['Tier I Yearly', 'Tier II Quarterly', 'Tier III Monthly', 'Unlimited Membership', 'Unlimited Membership I', 'Unlimited Membership II', 'Unlimited Membership III', 'Unlimited Membership IV']
    # Make a dataframe of valid subscriptions, which excludes test subscriptions
    # This must be used as the starting point for all further calculations
    df_recharge_subs_valid = df_recharge_subs[df_recharge_subs['Simplified Product Title'].isin(valid_products)]
    # Make a dataframe of valid orders, which excludes test orders
    # This must be used as the starting point for all further calculations
    df_recharge_orders_valid = df_recharge_orders[df_recharge_orders['Simplified Product Title'].isin(valid_products)]


    # Compute active subscriptions **as of the report date**
    # This assumes that once a Subscription has been cancelled, it cannot be uncancelled. Only matters when re-running past report dates.
    df_recharge_subs_active_on_report_date = df_recharge_subs_valid[(df_recharge_subs_valid['created_at'].dt.date.le(report_date)) & ((pd.isnull(df_recharge_subs_valid['cancelled_at'])) | (df_recharge_subs_valid['cancelled_at'].dt.date.gt(report_date)))]
    product_stats = df_recharge_subs_active_on_report_date.groupby('Simplified Product Title')['subscription_id'].count().rename('Active subscriptions')
    product_stats = product_stats.to_frame() # make it a dataframe again

    # Compute MRR (net of cancellations, aka excluding calculations)
    product_stats['MRR'] = df_recharge_subs_active_on_report_date.groupby('Simplified Product Title')['MRR'].sum()

    # Get the revenue for each product type
    # Each order has subtotal_price which has subtracted out any discounts.
    # Note that monthly figures ignore dates after the report date
    df_recharge_orders_today = df_recharge_orders_valid[df_recharge_orders_valid['created_at'].dt.date.eq(report_date)]
    df_recharge_orders_this_month = df_recharge_orders_valid[(df_recharge_orders_valid['created_at'].dt.month.eq(report_date.month)) & (df_recharge_orders_valid['created_at'].dt.year.eq(report_date.year)) & (df_recharge_orders_valid['created_at'].dt.day.le(report_date.day))]
    product_stats['Monthly revenue'] = df_recharge_orders_this_month.groupby('Simplified Product Title')['subtotal_price'].sum()

    # Compute the net revenue, assuming $80 COGS:
    product_stats['Monthly orders'] = df_recharge_orders_this_month.groupby('Simplified Product Title')['order_id'].count()
    # Monthly net revenue = 0.97 * Monthly revenue - ($80 * # orders this month)
    # product_stats['Monthly net revenue'] = 0.97 * product_stats['Monthly revenue'] - (80 * product_stats['Monthly orders'])
    # if 'Unlimited Membership' in product_stats.index:
    #     product_stats.loc['Unlimited Membership', 'Monthly net revenue'] = 0.97 * product_stats.loc['Unlimited Membership', 'Monthly revenue'] - (200 * product_stats.loc['Unlimited Membership', 'Monthly orders'])
    product_stats['Monthly net revenue'] = 0.97 * product_stats['Monthly revenue'] - df_recharge_orders_this_month.groupby('Simplified Product Title')['COGS'].sum()
    product_stats.fillna(0, inplace=True)

    # Handle newer product types before they exist
    if not 'Unlimited Membership' in product_stats.index:
        # print("Inserting 0 unlimited memberships")
        product_stats = pd.concat([product_stats, pd.DataFrame([[0, 0, 0, 0, 0]], columns=['Active subscriptions','MRR', 'Monthly revenue', 'Monthly orders', 'Monthly net revenue'], index=['Unlimited Membership'])])

    if not 'Unlimited Membership I' in product_stats.index:
            # print("Inserting 0 unlimited memberships")
            product_stats = pd.concat([product_stats, pd.DataFrame([[0, 0, 0, 0, 0]], columns=['Active subscriptions','MRR', 'Monthly revenue', 'Monthly orders', 'Monthly net revenue'], index=['Unlimited Membership I'])])

    if not 'Unlimited Membership II' in product_stats.index:
            # print("Inserting 0 unlimited memberships")
            product_stats = pd.concat([product_stats, pd.DataFrame([[0, 0, 0, 0, 0]], columns=['Active subscriptions','MRR', 'Monthly revenue', 'Monthly orders', 'Monthly net revenue'], index=['Unlimited Membership II'])])

    if not 'Unlimited Membership III' in product_stats.index:
            # print("Inserting 0 unlimited memberships")
            product_stats = pd.concat([product_stats, pd.DataFrame([[0, 0, 0, 0, 0]], columns=['Active subscriptions','MRR', 'Monthly revenue', 'Monthly orders', 'Monthly net revenue'], index=['Unlimited Membership III'])])
    if not 'Unlimited Membership IV' in product_stats.index:
            # print("Inserting 0 unlimited memberships")
            product_stats = pd.concat([product_stats, pd.DataFrame([[0, 0, 0, 0, 0]], columns=['Active subscriptions','MRR', 'Monthly revenue', 'Monthly orders', 'Monthly net revenue'], index=['Unlimited Membership IV'])])

    # Totals
    totals = product_stats.sum().rename('SUM').to_frame().transpose()

    # Make a copy with prettier formatting (numbers --> strings)
    product_stats_with_sum = pd.concat([product_stats, totals]).astype({'Active subscriptions': 'int32', 'Monthly orders': 'int32'})
    # product_stats_display.append(totals)
    product_stats_with_sum_display = product_stats_with_sum.copy()
    product_stats_with_sum_display['MRR'] = product_stats_with_sum_display['MRR'].map('${:,.2f}'.format)
    # product_stats_with_sum_display['Daily revenue'] = product_stats_with_sum_display['Daily revenue'].map('${:,.2f}'.format)
    product_stats_with_sum_display['Monthly revenue'] = product_stats_with_sum_display['Monthly revenue'].map('${:,.2f}'.format)
    product_stats_with_sum_display['Monthly net revenue'] = product_stats_with_sum_display['Monthly net revenue'].map('${:,.2f}'.format)

    unlimited_subscription_products = ['Unlimited Membership', 'Unlimited Membership I', 'Unlimited Membership II', 'Unlimited Membership III', 'Unlimited Membership IV']


    # Dict of the values for later use
    product_stats_dict = {}
    product_stats_dict['active_subscriptions_total'] = product_stats_with_sum.loc['SUM', 'Active subscriptions']
    product_stats_dict['active_subscriptions_yearly'] = product_stats_with_sum.loc['Tier I Yearly', 'Active subscriptions']
    product_stats_dict['active_subscriptions_quarterly'] = product_stats_with_sum.loc['Tier II Quarterly', 'Active subscriptions']
    product_stats_dict['active_subscriptions_monthly'] = product_stats_with_sum.loc['Tier III Monthly', 'Active subscriptions']
    product_stats_dict['active_subscriptions_unlimited'] = product_stats_with_sum.loc[unlimited_subscription_products, 'Active subscriptions']
    product_stats_dict['MRR_total'] = product_stats_with_sum.loc['SUM', 'MRR']
    product_stats_dict['MRR_yearly'] = product_stats_with_sum.loc['Tier I Yearly', 'MRR']
    product_stats_dict['MRR_quarterly'] = product_stats_with_sum.loc['Tier II Quarterly', 'MRR']
    product_stats_dict['MRR_monthly'] = product_stats_with_sum.loc['Tier III Monthly', 'MRR']
    product_stats_dict['MRR_unlimited'] = product_stats_with_sum.loc[unlimited_subscription_products, 'MRR']
    product_stats_dict['monthly_revenue_total'] = product_stats_with_sum.loc['SUM', 'Monthly revenue']
    product_stats_dict['monthly_revenue_yearly'] = product_stats_with_sum.loc['Tier I Yearly', 'Monthly revenue']
    product_stats_dict['monthly_revenue_quarterly'] = product_stats_with_sum.loc['Tier II Quarterly', 'Monthly revenue']
    product_stats_dict['monthly_revenue_monthly'] = product_stats_with_sum.loc['Tier III Monthly', 'Monthly revenue']
    product_stats_dict['monthly_revenue_unlimited'] = product_stats_with_sum.loc[unlimited_subscription_products, 'Monthly revenue']
    product_stats_dict['monthly_orders_total'] = product_stats_with_sum.loc['SUM', 'Monthly orders']
    product_stats_dict['monthly_net_revenue_total'] = product_stats_with_sum.loc['SUM', 'Monthly net revenue']

    # Compile daily and monthly stats of interest
    output_stats = {}

    all_subscription_products = ['Tier II Quarterly', 'Tier III Monthly', 'Unlimited Membership', 'Unlimited Membership I', 'Unlimited Membership II', 'Unlimited Membership III', 'Unlimited Membership IV']
    monthly_subscription_products = ['Tier III Monthly']
    quarterly_subscription_products = ['Tier II Quarterly']
    yearly_products = ['Tier I Yearly']
    # Filter to rows from exactly today or this month
    # Note that monthly figures ignore dates after the report date
    df_recharge_subs_today = df_recharge_subs_valid[df_recharge_subs_valid['created_at'].dt.date.eq(report_date)]
    df_recharge_subs_month = df_recharge_subs_valid[(df_recharge_subs_valid['created_at'].dt.month.eq(report_date.month)) & (df_recharge_subs_valid['created_at'].dt.year.eq(report_date.year)) & (df_recharge_subs_valid['created_at'].dt.day.le(report_date.day))]

    output_stats['all_subs_sold_today'] = df_recharge_subs_today[df_recharge_subs_today['Simplified Product Title'].isin(all_subscription_products)]['subscription_id'].count()
    output_stats['monthly_subs_sold_today'] = df_recharge_subs_today[df_recharge_subs_today['Simplified Product Title'].isin(monthly_subscription_products)]['subscription_id'].count()
    output_stats['unlimited_subs_sold_today'] = df_recharge_subs_today[df_recharge_subs_today['Simplified Product Title'].isin(unlimited_subscription_products)]['subscription_id'].count()
    output_stats['quarterly_subs_sold_today'] = df_recharge_subs_today[df_recharge_subs_today['Simplified Product Title'].isin(quarterly_subscription_products)]['subscription_id'].count()
    output_stats['yearly_sold_today'] = df_recharge_subs_today[df_recharge_subs_today['Simplified Product Title'].isin(yearly_products)]['subscription_id'].count()

    output_stats['all_subs_sold_this_month'] = df_recharge_subs_month[df_recharge_subs_month['Simplified Product Title'].isin(all_subscription_products)]['subscription_id'].count()
    output_stats['monthly_subs_sold_this_month'] = df_recharge_subs_month[df_recharge_subs_month['Simplified Product Title'].isin(monthly_subscription_products)]['subscription_id'].count()
    output_stats['unlimited_subs_sold_this_month'] = df_recharge_subs_month[df_recharge_subs_month['Simplified Product Title'].isin(unlimited_subscription_products)]['subscription_id'].count()
    output_stats['quarterly_subs_sold_this_month'] = df_recharge_subs_month[df_recharge_subs_month['Simplified Product Title'].isin(quarterly_subscription_products)]['subscription_id'].count()
    output_stats['yearly_sold_this_month'] = df_recharge_subs_month[df_recharge_subs_month['Simplified Product Title'].isin(yearly_products)]['subscription_id'].count()

    output_stats['total_revenue_today'] = df_recharge_orders_today['subtotal_price'].sum()
    df_recharge_orders_today_newsubs = df_recharge_orders_today[df_recharge_orders_today['subscription_id'].isin(df_recharge_subs_today['subscription_id'])]
    output_stats['revenue_new_subscriptions_today'] = df_recharge_orders_today_newsubs['subtotal_price'].sum()
    # Net revenue with assumption of $80 COGS: .97 * Actual revenue from new sales today - (X * number of new sales) = net revenue
    output_stats['net_revenue_new_subscriptions_today'] = "{0:.2f}".format(0.97 * output_stats['revenue_new_subscriptions_today'] - df_recharge_orders_today_newsubs['COGS'].sum() ) # all orders including yearly

    # Note that monthly figures ignore dates after the report date
    df_recharge_cancelled_month = df_recharge_subs_valid[(df_recharge_subs_valid['cancelled_at'].dt.month.eq(report_date.month)) & (df_recharge_subs_valid['cancelled_at'].dt.year.eq(report_date.year)) & (df_recharge_subs_valid['cancelled_at'].dt.day.le(report_date.day))]
    # Discount hackers
    output_stats['all_cancellations_after_1_order_this_month'] = df_recharge_cancelled_month[ (df_recharge_cancelled_month['# of orders associated to subscriptions'] == 1)]['subscription_id'].count()
    # Remainder of cancellations
    output_stats['all_cancellations_after_gt1_order_this_month'] = df_recharge_cancelled_month[ (df_recharge_cancelled_month['# of orders associated to subscriptions'] > 1)]['subscription_id'].count()
    # For churn: Cancellations of individual products
    output_stats['monthly_cancellations_after_gt1_order_this_month'] = df_recharge_cancelled_month[(df_recharge_cancelled_month['Simplified Product Title'].isin(monthly_subscription_products)) & (df_recharge_cancelled_month['# of orders associated to subscriptions'] > 1)]['subscription_id'].count()
    output_stats['unlimited_cancellations_after_gt1_order_this_month'] = df_recharge_cancelled_month[(df_recharge_cancelled_month['Simplified Product Title'].isin(unlimited_subscription_products)) & (df_recharge_cancelled_month['# of orders associated to subscriptions'] > 1)]['subscription_id'].count()
    output_stats['quarterly_cancellations_after_gt1_order_this_month'] = df_recharge_cancelled_month[(df_recharge_cancelled_month['Simplified Product Title'].isin(quarterly_subscription_products)) & (df_recharge_cancelled_month['# of orders associated to subscriptions'] > 1)]['subscription_id'].count()
    output_stats['yearly_cancellations_after_gt1_order_this_month'] = df_recharge_cancelled_month[(df_recharge_cancelled_month['Simplified Product Title'].isin(yearly_products)) & (df_recharge_cancelled_month['# of orders associated to subscriptions'] > 1)]['subscription_id'].count()

    output_stats['target_subs_this_month'] = monthly_target_subscriptions
    output_stats['target_yearly_this_month'] = monthly_target_yearly_kits

    output_stats['subs_remaining_this_month'] = output_stats['target_subs_this_month'] - output_stats['all_subs_sold_this_month']
    output_stats['yearly_remaining_this_month'] = output_stats['target_yearly_this_month'] - output_stats['yearly_sold_this_month']

    output_stats['subs_percent_of_target'] = "{0:.1f}%".format(100*output_stats['all_subs_sold_this_month'] / output_stats['target_subs_this_month'])
    output_stats['yearly_percent_of_target'] = "{0:.1f}%".format(100*output_stats['yearly_sold_this_month'] / output_stats['target_yearly_this_month'])

    output_stats_display = pd.DataFrame.from_dict(output_stats, orient='index', columns = [report_date_str])

    output_stats_full = {**product_stats_dict, **output_stats}

    return output_stats_full, output_stats_display, product_stats_with_sum_display
