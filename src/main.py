from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId
from datetime import datetime
from mangum import Mangum
from typing import List
from io import StringIO
import numpy as np
import boto3, io
import pysftp
import csv

#uvicorn main:app --reload

app = FastAPI()

class Item(BaseModel):
    storeid: str
    MerchandiseCode: List[str]
    FTP_Server: str
    FTP_User: str
    FTP_Password: str

@app.get("/")
async def root():
    return {"message": "Welcome to Scan Data"}

@app.post("/scan_data")
async def scan_data(item: Item):
    try:
        storeid = item.storeid
        merchandise_codes = item.MerchandiseCode
        sftp_server = item.FTP_Server
        sftp_user = item.FTP_User
        sftp_password = item.FTP_Password

        # Create a connection to the MongoDB instance
        client = MongoClient('mongodb+srv://verifonejb:AP7RygDsKyckD01M@cluster0.yr64gis.mongodb.net/')
        db = client['verifone']
        collection = db['weekly']

        # Find the latest document with the specified storeid
        latest_doc = collection.find({"storeid": storeid}).sort('_id', -1).limit(1)[0]

        # Access the new collection where you want to dump the data
        #data_collection = db['scan_data']
        promotion = db['gbPricePromotion']
        stores = db['stores']
        upc_collection = db['upc']

        # Define the current month
        current_month = datetime.now().strftime('%B%Y')

        # Fetch the store data from the gbPricePromotion collection based on storeid and month
        store_data = promotion.find_one(
            {"storeid": storeid, "month": current_month},
            {"Stores": 1}
        )

        # Check if store data is found
        if store_data and 'Stores' in store_data:
            # Extract the required fields from the first store in the Stores array
            store = store_data['Stores'][0]
            rcn = store['RCN']
            address = store['Address']
            city = store['City']
            state = store['State']
        else:
            rcn = address = city = state = None

        # Create a list to store the extracted data
        data = []

        # Get the ReceiptDate from the last SaleEvent
        last_event = latest_doc['SaleEvent'][-1]
        week_ending_date = last_event['ReceiptDate']

        # Extract TransactionID, ReceiptDate, and ReceiptTime from SaleEvent and dump it to the new collection
        for event in latest_doc['SaleEvent']:
            transaction_id = event['TransactionID']
            receipt_date = event['ReceiptDate']
            receipt_time = event['ReceiptTime']

            # Check if TransactionLine exists in the event
            if 'TransactionLine' in event:
                for transaction in event['TransactionLine']:
                    # Check if ItemLine exists in the transaction
                    if 'ItemLine' in transaction:
                        item = transaction['ItemLine']
                        merchandise_code = item.get('MerchandiseCode', None)

                        # Check if MerchandiseCode is in the list for the current store
                        if merchandise_code in merchandise_codes:
                            poscode = item.get('POSCode', None)
                            description = item.get('Description', None)
                            salesquantity = int(float(item.get('SalesQuantity', 0)))
                            salesamount = "{:.2f}".format(float(item.get('SalesAmount', None)))

                            # Fetch the store document based on storeid
                            store_doc = stores.find_one({'_id': ObjectId(storeid)})
                            store_name = store_doc['store_name'] if store_doc else None
                            zip_code = store_doc['zip_code'] if store_doc else None

                            # Fetch the document from MongoDB based on 'cyclecode' and 'month'
                            doc = upc_collection.find_one({"cycleCode": "Promotion", "month": current_month})

                            multi_pack_indicator = 'N'
                            multi_pack_required_quantity = 0
                            multi_pack_discount_amount = 0
                            if doc:
                                # Check if 'SKU Code' matches any 'UPCCodes' (ignoring first and last digit) and 'Quantity Sold' is greater than or equal to 2
                                for upc in doc['UPCCodes']:
                                    if len(poscode) == 14:
                                        poscode_slice = poscode[2:-1]
                                        upc_to_compare = upc[:-1]
                                    else:
                                        poscode_slice = poscode
                                        upc_to_compare = upc[:-1]

                                    if str(poscode_slice) == upc_to_compare and salesquantity >= 2:
                                        # Change 'Multi-Pack Indicator' value from 'N' to 'Y'
                                        multi_pack_indicator = 'Y'
                                        # Set 'Multi-Pack Required Quantity' to the value in 'Quantity Sold'
                                        multi_pack_required_quantity = int(float(salesquantity))
                                        # Calculate Multi-Pack Discount Amount only if Multi-Pack Indicator is 'Y'
                                        multi_pack_discount_amount = multi_pack_required_quantity * 0.25
                                        break

                            # Create a dictionary with the extracted data and add it to the list
                            row = {
                                'Retail Control Number': rcn,
                                'WeekEndingDate': week_ending_date,
                                'TransactionDate': receipt_date,
                                'TransactionTime': receipt_time,
                                'TransactionID': transaction_id,
                                'Store Number': 1,
                                'Store Name': store_name,
                                'Store Address': address,
                                'Store City': city,
                                'Store State': state,
                                'Store Zip + 4 Code': zip_code,
                                'Category': 'CIG',
                                'Manufacturer Name': 'PM USA',
                                'SKU Code': poscode,
                                'UPC Code': poscode,
                                'UPC Description': description,
                                'Unit of Measure': 'PACK',
                                'Quantity Sold': salesquantity,
                                'Consumer Units': 1,
                                'Multi-Pack Indicator': multi_pack_indicator,
                                'Multi-Pack Required Quantity': multi_pack_required_quantity,
                                'Multi-Pack Discount Amount': multi_pack_discount_amount,
                                'Retailer-Funded Discount Name': np.nan,
                                'Retailer-Funded Discount Amount': np.nan,
                                'MFG Deal Name ONE': np.nan,
                                'MFG Deal Discount Amount ONE': np.nan,
                                'MFG Deal Name TWO': np.nan,
                                'MFG Deal Discount Amount TWO': np.nan,
                                'MFG Deal Name THREE': np.nan,
                                'MFG Deal Discount Amount THREE': np.nan,
                                'Final Sales Price': salesamount,
                                'R1': np.nan,
                                'R2': np.nan,
                                'R3': np.nan,
                                'R4': np.nan,
                                'R5': np.nan,
                                'R6': np.nan,
                                'R7': np.nan,
                                'R8': np.nan,
                                'R9': np.nan,
                                'R10': np.nan,
                                'R11': np.nan,
                                'R12': np.nan,
                                'R13': np.nan,
                                'R14': np.nan
                            }

                            data.append(row)

        # Calculate the sum of 'Quantity Sold' and 'Final Sales Price'
        sum_quantity_sold = sum(int(float(row['Quantity Sold'])) for row in data if row['Quantity Sold'] != '')
        sum_fsp = "{:.2f}".format(sum(float(row['Final Sales Price']) for row in data if row['Final Sales Price'] != ''))

        # Get the unique 'Store Name' and remove spaces
        store_name = next((row['Store Name'] for row in data if row['Store Name'] != ''), '')
        store_title = next((row['Store Name'] for row in data if row['Store Name'] != ''), '').replace(' ', '')

        # Get the unique 'WeekEndingDate' and remove hyphens
        week_end_date = datetime.strptime(last_event['ReceiptDate'], '%Y-%m-%d').strftime('%Y%m%d')

        # Create a session using your AWS credentials
        s3 = boto3.client('s3', aws_access_key_id='AKIASYKHH2OQSUJI2VHN', aws_secret_access_key='QHPYn5dywQBoIRUtS4y7aUlsKc+PZK4Q3rfvFX4x')

        # Convert your data to a CSV string
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=row.keys(), delimiter='|')
        for row in data:
            # Replace nan values with an empty string
            cleaned_row = {k: ('' if str(v) == 'nan' else v) for k, v in row.items()}
            # Convert date to the required format
            cleaned_row['WeekEndingDate'] = cleaned_row['WeekEndingDate'].replace('-', '')
            cleaned_row['TransactionDate'] = cleaned_row['TransactionDate'].replace('-', '')
            writer.writerow(cleaned_row)

        # Add the number of rows, sum of 'Quantity Sold', sum of 'Final Sales Price', and 'Store Name' at the beginning
        csv_buffer.seek(0, 0)
        csv_content = csv_buffer.getvalue()
        csv_content = f"{len(data)}|{sum_quantity_sold}|{sum_fsp}|{store_name}\n" + csv_content

        key = f"{store_title}_{week_end_date}.txt"

        # Write the CSV string to an S3 bucket
        s3.put_object(Bucket='ilearnbackend', Key=key, Body=csv_content)

        # Construct the URL of the uploaded file
        file_url = f"https://ilearnbackend.s3.amazonaws.com/{key}"

        # Print success message
        print(f"File {key} uploaded successfully to S3 bucket.")

        # Print the URL of the uploaded file
        print(f"URL of the uploaded file: {file_url}")

        # Create a CnOpts object and set hostkeys to None
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        # Define the name of the file on the SFTP server
        remote_filename = f"/incoming/{store_title}_{week_end_date}.txt"

        # Create an SFTP object and login
        with pysftp.Connection(sftp_server, username=sftp_user, password=sftp_password, cnopts=cnopts) as sftp:
            # Create a BytesIO object
            with io.BytesIO() as file:
                # Download the file from S3 to the BytesIO object
                s3.download_fileobj('ilearnbackend', key, file)
                # Seek back to the start of the file
                file.seek(0)
                # Use SFTP's putfo command to upload the file from the BytesIO object
                sftp.putfo(file, remote_filename)

        return {"message": f"File {key} uploaded successfully to {sftp_server}."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

handler = Mangum(app)
