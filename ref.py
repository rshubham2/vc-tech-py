# def sync_data():
#     # Fetch data from Zoho
#     sales_orders = fetch_all_zoho_sales_orders()
#     print('Total records present -', len(sales_orders))
#     # print('Data fetched from Zoho:', sales_orders[5])
#     if sales_orders:
#         excel_data = read_excel_data(excel_file_path)
#         combined_data = merge_data(sales_orders, excel_data, key_column_name)
#         mapped_data = [map_fields(order, field_mapping) for order in combined_data]
#         # print("mapped data-",mapped_data[0])
#         upsert_data_to_mongodb(mapped_data)
#     else:
#         print("Failed to fetch sales orders data")

import requests
from pymongo import MongoClient
from datetime import datetime, timezone
import pandas as pd
import time


REFRESH_TOKEN = '1000.94fccafc2fd7f57ea21eee0f8cdd7955.fd557182781a6dc4059361c7bd66e041'
CLIENT_ID = '1000.KXTGP1GAGIDX12Q294C6OIMVR60VMX'
CLIENT_SECRET = 'bb44b083c2b29eb4eefd1a605266a866fcd5f491fb'
REDIRECT_URI = 'https://www.google.com/'

auth_token = None
token_expiry = 0

def refresh_token():
    global auth_token, token_expiry
    response = requests.post('https://accounts.zoho.in/oauth/v2/token', data={
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'refresh_token'
    })
    response.raise_for_status()
    data = response.json()
    auth_token = data['access_token']
    token_expiry = time.time() + data['expires_in']

def get_valid_token():
    global auth_token, token_expiry
    if not auth_token or time.time() > token_expiry - 300:  # 5 minutes buffer
        refresh_token()
    return auth_token

def fetch_all_zoho_sales_orders():
    base_url = 'https://www.zohoapis.in/books/v3/salesorders'
    params = {
        'organization_id': '60005679410',
        'page': 1,
        'per_page': 200
    }
    
    all_sales_orders = []
    
    while True:
        try:
            headers = {
                'Authorization': f'Zoho-oauthtoken {get_valid_token()}'
            }
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            page_sales_orders = data.get('salesorders', [])
            all_sales_orders.extend(page_sales_orders)
            
            if data.get('page_context', {}).get('has_more_page', False):
                params['page'] += 1
            else:
                break
        
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            break
    
    return all_sales_orders

def read_excel_data(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)
    return df


def map_fields(sales_order, field_mapping):
    mapped_data = {}
    for mongo_field, zoho_field in field_mapping.items():
        value = sales_order
        for key in zoho_field.split('.'):
            value = value.get(key, {})
        if value != {}:
            mapped_data[mongo_field] = value
        else:
            # Provide default values for missing fields
            if mongo_field in ['SOId', 'SOCategory', 'PMName']:
                mapped_data[mongo_field] = 'HELLO'
            elif mongo_field in ['SubTotal', 'Total']:
                mapped_data[mongo_field] = 0.0
            elif mongo_field == 'isDropped':
                mapped_data[mongo_field] = False
            # elif mongo_field == 'currentStage':
            #     mapped_data[mongo_field] = 0
            elif mongo_field in ['createdAt', 'updatedAt']:
                mapped_data[mongo_field] = datetime.strptime('01-01-2001', '%d-%m-%Y')
                # mapped_data[mongo_field] = date_obj.replace(tzinfo=timezone.utc).isoformat()
    return mapped_data


def merge_data(zoho_data, excel_data, key):
    # Convert Excel data to a dictionary for easy look-up
    excel_dict = excel_data.set_index(key).to_dict(orient='index')
    default_date = datetime.strptime('01-01-2001', '%d-%m-%Y')

    for order in zoho_data:
        order_key = order.get('salesorder_number')
        if order_key in excel_dict:
            # Merge Excel data into the sales order
            for field, value in excel_dict[order_key].items():
                if value is not None:
                    order[field] = value
        
        # Provide default values for missing Excel data only if not already set
        if 'currentStage' not in order:
            order['currentStage'] = 0
        if 'isDropped' not in order:
            order['isDropped'] = False
    return zoho_data

def upsert_data_to_mongodb(data):
    client = MongoClient('mongodb+srv://admin:admin123@cluster0.idwldf8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
    db = client['Database']
    collection = db['Stage0']

    if data:
        for item in data:
            # Remove None values to avoid MongoDB errors
            item = {k: v for k, v in item.items() if v is not None}

            # Set SONumber as the _id field
            item['_id'] = item['SONumber']

            # Fetch the existing document
            existing_doc = collection.find_one({'_id': item['_id']})

            # If the document exists, retain the currentStage field
            if existing_doc and 'currentStage' in existing_doc:
                item['currentStage'] = existing_doc['currentStage']

            # Upsert the document, preserving the existing currentStage if present
            collection.update_one(
                {'_id': item['_id']},
                {'$set': item},
                upsert=True
            )
        print(f"Upserted {len(data)} documents")
    else:
        print("No data to upsert")

    client.close()



# Define your field mapping
field_mapping = {
    'SONumber': 'salesorder_number',
    'SOId' : 'salesorder_id',
    'clientName': 'customer_name',
    'SubTotal' : 'sub_total',
    'Total': 'total',
    'SOCategory' : 'cf_so_cat',
    'PMName' : 'cf_project_manager_name',
    'isDropped' : 'isDropped',
    'currentStage'  : 'currentStage',
    'clientExpectedDate': 'clientExpectedDate',
    'orderStatus' : 'order_status',
    'createdAt' : 'createdAt',
    'updatedAt' : 'updatedAt'
    # Add more fields as needed
}

# Paths to your Excel file and other required details
excel_file_path = 'C:/Users/priya/OneDrive/Documents/GitHub/CurrentStageSOdata.xlsx'  # Update this path
key_column_name = 'SONumber'  # Update this with the actual key column name in your Excel file

def sync_data(first_run=False):
    sales_orders = fetch_all_zoho_sales_orders()
    print('Total records present -', len(sales_orders))
    
    # On first run, process all records; otherwise, filter for 'open' orderStatus
    if not first_run:
        sales_orders = [order for order in sales_orders if order.get('order_status') == 'open']
        print('Open records present -', len(sales_orders))
    
    if sales_orders:
        excel_data = read_excel_data(excel_file_path)
        combined_data = merge_data(sales_orders, excel_data, key_column_name)
        mapped_data = [map_fields(order, field_mapping) for order in combined_data]
        upsert_data_to_mongodb(mapped_data)
    else:
        print("No relevant sales orders to process")

# Run the initial sync without filtering
sync_data(first_run=True)

# Periodic sync loop, filtering for 'open' orders
while True:
    sync_data()
    time.sleep(60)
