import psycopg2
import requests
import json
import datetime

# Database connection parameters
dbname = 'postgres'
user = 'postgres'
password = 'naukee'
host = 'localhost'
port = '5432'

try:
    # Establish a connection to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print("Connected to Database")

    # Create a cursor
    cur = conn.cursor()

    # API endpoint
    api_url = 'https://data.cityofchicago.org/resource/ijzp-q8t2.json'
    limit = 50000  # Limit for each request
    offset = 5000    # Initialize offset value

    # Define start and end dates for the desired date range
    start_date = datetime.date(2019, 1, 1)
    end_date = datetime.date.today()  # Now

    total_records = 0

    # Fetch total number of records in the dataset
    response = requests.get(api_url, params={'$select': 'COUNT(*)', '$where': f"date >= '{start_date}' AND date <= '{end_date}'"})
    if response.status_code == 200:
        total_records = int(response.json()[0]['COUNT'])
        print("Total records in dataset:", total_records)
    else:
        print("Failed to fetch total number of records:", response.status_code)

    # Fetch data in batches until all records are retrieved
    while offset < total_records:
        response = requests.get(api_url, params={'$limit': limit, '$offset': offset, '$where': f"date >= '{start_date}' AND date <= '{end_date}'"})
        if response.status_code == 200:
            api_data = response.json()
            
            # Inserting data into PostgreSQL
            for item in api_data:
                if 'location' in item:
                    location = item['location']
                    latitude = location.get('latitude')
                    longitude = location.get('longitude')
                    item['location'] = json.dumps(location)
                    
                    sql_query = """
                        INSERT INTO chicago_crime (id, case_number, date, block, iucr, primary_type, description, location_description, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, year, updated_on, latitude, longitude, location)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;  -- Skip insertion if ID already exists
                    """
                    cur.execute(sql_query, (
                        item['id'], item['case_number'], item.get('date'), item.get('block'), item.get('iucr'), item.get('primary_type'),
                        item.get('description'), item.get('location_description'), item.get('arrest'), item.get('domestic'), item.get('beat'),
                        item.get('district'), item.get('ward'), item.get('community_area'), item.get('fbi_code'), item.get('x_coordinate'),
                        item.get('y_coordinate'), item.get('year'), item.get('updated_on'), latitude, longitude, item['location']
                    ))
                else:
                    print("Location data incomplete or missing in item:", item)
            
            offset += limit
        else:
            print("Failed to fetch data from API:", response.status_code)
            break
    
    # Commit the transaction
    conn.commit()
    print("Data successfully inserted")

except psycopg2.Error as e:
    print("Error executing SQL:", e)
    conn.rollback()  # Rollback in case of an error 

finally:
    # Close the cursor and connection
    if cur:
        cur.close()
    if conn:
        conn.close()
