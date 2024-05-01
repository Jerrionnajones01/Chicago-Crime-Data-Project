import psycopg2
import csv

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='naukee',
    host='localhost',
    port='5432'
)

# Create a cursor
cur = conn.cursor()

try:
    # Define the Common Table Expressions (CTEs)
    # Join chicagocrime and community_data
    cur.execute("""
        WITH weather_avg AS (
    SELECT
        DATE_TRUNC('day', date) AS truncated_date,
        AVG(temperature_2m_max) AS avg_max_temperature,
        AVG(temperature_2m_min) AS avg_min_temperature,
        AVG(rain_sum) AS avg_rain,
        AVG(snowfall_sum) AS avg_snow
    FROM
        weather_data 
    GROUP BY
        truncated_date
)

SELECT
    w.truncated_date,
    w.avg_max_temperature,
    w.avg_min_temperature,
    w.avg_rain,
    w.avg_snow,
    c.*,
    cd.*, 
	CASE 
		WHEN 
			w.avg_rain > 0 THEN 1 
			ELSE 0 
	END AS is_raining,
	CASE
		WHEN
			w.avg_snow > 0 THEN 1
			ELSE 0
	END AS is_snowing
FROM
    weather_avg w
JOIN
    chicagocrime c ON DATE_TRUNC('day', c.date) = w.truncated_date
JOIN
    community_data cd ON c.community_area = cd.communityareanumber
ORDER BY 
    w.truncated_date DESC;

    """)
    
    # Fetch all rows along with column names
    joined_data = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    # Write the data into a CSV file
    with open('joined_ChiData.csv', 'w' ,newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names) # writes the headers
        writer. writerows(joined_data) # writes rows
    
    print('joined data sucesfully fetched and written into file')


except psycopg2.Error as e:
    print("Error:", e)
    conn.rollback()  # Rollback in case of an error

finally:
    # Close the cursor and connection
    if cur:
        cur.close()
    if conn:
        conn.close()
