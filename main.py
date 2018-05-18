import sys, os, base64, datetime, hashlib, hmac, requests, csv

from dotenv import load_dotenv
from urllib.parse import quote
from myawis import *
from lxml import etree

#Load Env Vars from .env
load_dotenv()
CSV_IN_NAME = os.getenv("CSV_IN_NAME")
CSV_OUT_NAME = os.getenv("CSV_OUT_NAME")
RESPONSE_GROUPS = os.getenv("RESPONSE_GROUPS")
access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

#Initialize the AWIS object to fetch URLS
awis_fetcher=CallAwis(access_key,secret_key)

with open('out.csv', 'w', newline='') as csv_out_file:
    with open(CSV_IN_NAME) as csv_in_file:
        writer = csv.writer(
            csv_out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        #write headers
        writer.writerow([
                "Company",
                "URL",
                "Links In",

                "Three Month Rank Value",
                "Three Month Rank Delta",
                "Three Month Reach Per Million Value",
                "Three Month Reach Per Million Delta",
                "Three Month Reach Rank Value",
                "Three Month Reach Rank Delta",
                "Three Month Page Views Per Million Value",
                "Three Month Page Views Per Million Delta",
                "Three Month Page Views Rank Value",
                "Three Month Page Views Rank Delta",
                "Three Month Page Views Per User Value",
                "Three Month Page Views Per User Delta",

                "One Month Rank Value",
                "One Month Rank Delta",
                "One Month Reach Per Million Value",
                "One Month Reach Per Million Delta",
                "One Month Reach Rank Value",
                "One Month Reach Rank Delta",
                "One Month Page Views Per Million Value",
                "One Month Page Views Per Million Delta",
                "One Month Page Views Rank Value",
                "One Month Page Views Rank Delta",
                "One Month Page Views Per User Value",
                "One Month Page Views Per User Delta",

                "Seven Day Rank Value",
                "Seven Day Rank Delta",
                "Seven Day Reach Per Million Value",
                "Seven Day Reach Per Million Delta",
                "Seven Day Reach Rank Value",
                "Seven Day Reach Rank Delta",
                "Seven Day Page Views Per Million Value",
                "Seven Day Page Views Per Million Delta",
                "Seven Day Page Views Rank Value",
                "Seven Day Page Views Rank Delta",
                "Seven Day Page Views Per User Value",
                "Seven Day Page Views Per User Delta",

                "One Day Rank Value",
                "One Day Rank Delta",
                "One Day Reach Per Million Value",
                "One Day Reach Per Million Delta",
                "One Day Reach Rank Value",
                "One Day Reach Rank Delta",
                "One Day Page Views Per Million Value",
                "One Day Page Views Per Million Delta",
                "One Day Page Views Rank Value",
                "One Day Page Views Rank Delta",
                "One Day Page Views Per User Value",
                "One Day Page Views Per User Delta",
            ])
        
        reader = csv.reader(csv_in_file, delimiter=',', quotechar='"')
        next(reader) #consume header row
        for i, row in enumerate(reader):
            try:
                company_name = row[0]
                company_url = row[1]
                print("({0}) Fetching {1}".format(i, company_name))

                #Query the awis API
                page_xml = awis_fetcher.urlinfo(company_url, RESPONSE_GROUPS)
                linksin = page_xml.find("ContentData").find("LinksInCount").text

                usage_stats = page_xml.find("TrafficData").findAll('UsageStatistic')
                output_data = {
                    'three_month': {},
                    'one_month': {},
                    'seven_days': {},
                    'one_day': {}
                }
                #Build info for each time range into object output data
                for usage_stat in usage_stats:
                    rank = usage_stat.find("Rank")
                    pv = usage_stat.find("PageViews")
                    reach = usage_stat.find("Reach")
                    
                    output_node = {
                        'rank_value': rank.find('Value').text,
                        'rank_delta': rank.find('Delta').text,

                        'reach_per_million_value': reach.find('PerMillion').find('Value').text,
                        'reach_per_million_delta': reach.find('PerMillion').find('Delta').text,
                        'reach_rank_value': reach.find('Rank').find('Value').text,
                        'reach_rank_delta': reach.find('Rank').find('Delta').text,

                        'page_views_per_million_value': pv.find('PerMillion').find('Value').text,
                        'page_views_per_million_delta': pv.find('PerMillion').find('Delta').text,
                        'page_views_rank_value': pv.find('PageViews').find('Value').text,
                        'page_views_rank_delta': pv.find('PageViews').find('Delta').text,
                        'page_views_user_value': pv.find('PerUser').find('Value').text,
                        'page_views_user_delta': pv.find('PerUser').find('Delta').text
                    }

                    time_range = usage_stat.find("TimeRange")
                    if time_range.find("Months") and time_range.find("Months").text == "3":
                        output_data['three_month'] = output_node
                    if time_range.find("Months") and time_range.find("Months").text == "1":
                        output_data['one_month'] = output_node
                    if time_range.find("Days") and time_range.find("Days").text == "7":
                        output_data['seven_days'] = output_node
                    if time_range.find("Days") and time_range.find("Days").text == "1":
                        output_data['one_day'] = output_node

                #Write out data
                writer.writerow([
                    company_name,
                    company_url,
                    linksin,

                    output_data.get('three_month').get('rank_value', ''),
                    output_data.get('three_month').get('rank_delta', ''),

                    output_data.get('three_month').get('reach_per_million_value', ''),
                    output_data.get('three_month').get('reach_per_million_delta', ''),
                    output_data.get('three_month').get('reach_rank_value', ''),
                    output_data.get('three_month').get('reach_rank_delta', ''),

                    output_data.get('three_month').get('page_views_per_million_value', ''),
                    output_data.get('three_month').get('page_views_per_million_delta', ''),
                    output_data.get('three_month').get('page_views_rank_value', ''),
                    output_data.get('three_month').get('page_views_rank_delta', ''),
                    output_data.get('three_month').get('page_views_user_value', ''),
                    output_data.get('three_month').get('page_views_user_delta', ''),

                    output_data.get('one_month').get('rank_value', ''),
                    output_data.get('one_month').get('rank_delta', ''),

                    output_data.get('one_month').get('reach_per_million_value', ''),
                    output_data.get('one_month').get('reach_per_million_delta', ''),
                    output_data.get('one_month').get('reach_rank_value', ''),
                    output_data.get('one_month').get('reach_rank_delta', ''),

                    output_data.get('one_month').get('page_views_per_million_value', ''),
                    output_data.get('one_month').get('page_views_per_million_delta', ''),
                    output_data.get('one_month').get('page_views_rank_value', ''),
                    output_data.get('one_month').get('page_views_rank_delta', ''),
                    output_data.get('one_month').get('page_views_user_value', ''),
                    output_data.get('one_month').get('page_views_user_delta', ''),

                    output_data.get('seven_days').get('rank_value', ''),
                    output_data.get('seven_days').get('rank_delta', ''),

                    output_data.get('seven_days').get('reach_per_million_value', ''),
                    output_data.get('seven_days').get('reach_per_million_delta', ''),
                    output_data.get('seven_days').get('reach_rank_value', ''),
                    output_data.get('seven_days').get('reach_rank_delta', ''),

                    output_data.get('seven_days').get('page_views_per_million_value', ''),
                    output_data.get('seven_days').get('page_views_per_million_delta', ''),
                    output_data.get('seven_days').get('page_views_rank_value', ''),
                    output_data.get('seven_days').get('page_views_rank_delta', ''),
                    output_data.get('seven_days').get('page_views_user_value', ''),
                    output_data.get('seven_days').get('page_views_user_delta', ''),

                    output_data.get('one_day').get('rank_value', ''),
                    output_data.get('one_day').get('rank_delta', ''),

                    output_data.get('one_day').get('reach_per_million_value', ''),
                    output_data.get('one_day').get('reach_per_million_delta', ''),
                    output_data.get('one_day').get('reach_rank_value', ''),
                    output_data.get('one_day').get('reach_rank_delta', ''),

                    output_data.get('one_day').get('page_views_per_million_value', ''),
                    output_data.get('one_day').get('page_views_per_million_delta', ''),
                    output_data.get('one_day').get('page_views_rank_value', ''),
                    output_data.get('one_day').get('page_views_rank_delta', ''),
                    output_data.get('one_day').get('page_views_user_value', ''),
                    output_data.get('one_day').get('page_views_user_delta', ''),
                ])

            except Exception as e: 
                print(e)