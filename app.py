from flask import Flask
import psycopg2
import json
import schedule
import time
from twilio.rest import Client
from datetime import datetime, timedelta


app = Flask(__name__)

@app.route('/')
def home():
# PostgreSQL database configuration
    DATABASE_CONFIG = {
        'host': "postgresql-152351-0.cloudclusters.net",
        'port': "19991",
        'database': "finflash",
        'user': "suresh",
        'password': "s1u2r3e4"
    }

    def fetch_data_from_postgresql():
        conn = None
        local_product_dict = {}
        try:
            # Connect to your postgres DB
            conn = psycopg2.connect(**DATABASE_CONFIG)

            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a query
            cur.execute("SELECT phone, product_info FROM customer")

            # Retrieve query results
            records = cur.fetchall()

            # Process data
            local_product_dict = process_data(records)

        except Exception as err:
            print(err)
        finally:
            if conn is not None:
                conn.close()

        return local_product_dict

    def process_data(data):
        local_product_dict = {}
        for row in data:
            phone = row[0]
            wa_number = '+91' + phone.strip()  # Assuming the phone number doesn't already start with '+91'
            product_infos = row[1]  # This is already a Python list or dict, not a JSON string.

            if product_infos:
                # No need for json.loads() as product_infos is already a list or dict.
                if isinstance(product_infos, list):  # If product_infos is a list
                    for product in product_infos:
                        product_name = product  # If product_infos is a list of product names
                        if product_name not in local_product_dict:
                            local_product_dict[product_name] = []
                        local_product_dict[product_name].append(wa_number)
                elif isinstance(product_infos, dict):  # If product_infos is a dict with product details
                    for product_name, product_details in product_infos.items():
                        if product_name not in local_product_dict:
                            local_product_dict[product_name] = []
                        local_product_dict[product_name].append(wa_number)

        return local_product_dict

    local_product_dict_ = fetch_data_from_postgresql()
    #i have dictionary from customer table in the variable called local_product_dict_

    def fetch_data_for_day(target_date):
        conn = None
        research_data = []

        try:
            with psycopg2.connect(**DATABASE_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT * FROM research WHERE "TimeStamp_str" = %s', (target_date,))
                    research_data = cur.fetchall()

        except Exception as err:
            print(f"Error: {err}")
        finally:
            if conn is not None:
                conn.close()

        return research_data
    target_date = '27-Nov-2023 15:53'
    research_data_for_day = fetch_data_for_day(target_date)

    #i have a list of products from research table for the recent day in research_data_for_day

    def fetch_all_products_from_research():
        conn = None
        all_products = []

        try:
            with psycopg2.connect(**DATABASE_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Fetch all columns from the research table
                    cur.execute("SELECT * FROM research")
                    research_records = cur.fetchall()

                    # Assuming the first column is the product column
                    all_products = [row[0] for row in research_records]

        except Exception as err:
            # Log or handle the error appropriately
            print(f"Error: {err}")
        finally:
            if conn is not None:
                conn.close()

        return all_products

    a =[]

    def unique(list1):
    
        # insert the list to the set
        list_set = set(list1)
        # convert the set to the list
        unique_list = (list(list_set))
        for x in unique_list:
            a=a.append(x)

    # If you need to execute and check the output in this script, you can:
    if __name__ == '__main__':
        all_products = fetch_all_products_from_research()

    def filter_phone_numbers(product_list, product_phone_dict):
        selected_numbers = set()

        for product, phone_numbers in product_phone_dict.items():
            # Check if the product is in the list
            if product in product_list:
                # Add all associated phone numbers to the selected numbers set
                selected_numbers.update(phone_numbers)

        return selected_numbers

    final_number = []

    if __name__ == "__main__":
        # Example data

        product_phone_dict = local_product_dict_

        unique_products_list = research_data_for_day[0]


        selected_numbers = filter_phone_numbers(unique_products_list, product_phone_dict)
        
        print("Selected phone numbers:")
        for number in selected_numbers:
            final_number.append(number)

            
    # Your Twilio account SID and Auth Token
    account_sid = 'ACd2d46eea8cb50e84bf59bd0a8e8683ca'
    auth_token = 'b2f40b59d69a650f96337020ddc9bbcb'

    # Create a Twilio client
    client = Client(account_sid, auth_token)

    # Your Twilio WhatsApp-enabled phone number
    from_whatsapp_number = '+12562729417'

    # Update the list by adding "whatsapp:" to each phone number
    for i in range(len(final_number)):
        final_number[i] = 'whatsapp:' + final_number[i]

    # Print the updated list


    print(final_number)

    # List of phone numbers to send WhatsApp messages to
    whatsapp_numbers = final_number

    # Message to be sent
    message_body = 'To view disclaimer and understand more about our recommendations, please click here'

    # Function to send WhatsApp message
    def send_whatsapp_message():
        for whatsapp_number in whatsapp_numbers:
            try:
                message = client.messages.create(
                    body=message_body,
                    from_='whatsapp:' + from_whatsapp_number,
                    to=whatsapp_number
                )
                print(f'WhatsApp message sent to {whatsapp_number}. SID: {message.sid}')
            except Exception as e:
                print(f'Error sending WhatsApp message to {whatsapp_number}: {str(e)}')

    # Schedule the task to run every day at 16:00
    schedule.every().day.at("16:00").do(send_whatsapp_message)

    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)
        
if __name__ == '__main__':
    app.run(debug=True)

    
