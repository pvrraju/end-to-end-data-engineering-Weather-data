




import pandas as pd
import gcsfs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import json
import requests
from google.cloud import bigquery


def get_average_from_bigquery(column_name):
    client = bigquery.Client(project='data-225-group-project')
    query = f'SELECT AVG({column_name}) as avg FROM `data-225-group-project.climate_dwh.climate_fact`'
    query_job = client.query(query)
    result = query_job.result()
    return result.to_dataframe()['avg'][0]


def get_lat_lon(api_key, city_name):
    geocoding_url = "http://api.openweathermap.org/data/2.5/weather"
    geocoding_params = {
        "q": city_name,
        "appid": api_key,
    }

    geocoding_response = requests.get(geocoding_url, params=geocoding_params)
    geocoding_data = geocoding_response.json()

    if geocoding_response.status_code == 200:
        lat = geocoding_data["coord"]["lat"]
        lon = geocoding_data["coord"]["lon"]
        return lat, lon
    else:
        print(f"Error: {geocoding_data['message']}")
        return None
    # ... same as before ...

# Function to get weather data from the OpenWeatherMap API
def get_weather_data(api_key, lat, lon):
    weather_url = "http://api.openweathermap.org/data/2.5/forecast/daily"
    weather_params = {
        "lat": lat,
        "lon": lon,
        "cnt": 1,  # Number of days for forecast
        "appid": api_key,
        "units": "metric",  # Use Celsius for temperature
    }

    weather_response = requests.get(weather_url, params=weather_params)
    weather_data = weather_response.json()

    return weather_data
    # ... same as before ...

# def pullingdata(**context):
#     api_key = "d27e47c320fcf6b75478eeaae6c3ef74"
#     city_name = "San Jose"
#     coordinates = get_lat_lon(api_key, city_name)
#     weather_data = get_weather_data(api_key, coordinates[0], coordinates[1])
#     context['ti'].xcom_push(key='weather_data', value=weather_data)


def pullingdata(**context):
    # api_key = "your_api_key_here"   Replace with your actual API key

    api_key = "d27e47c320fcf6b75478eeaae6c3ef74"

    city_names = [
    "Helena", "Montpelier", "Bismarck", "Saint Paul", "Cheyenne", "Madison", "Denver", "Des Moines", "Indianapolis",
    "Lincoln", "Boise", "Albany", "Topeka", "Columbus", "Springfield", "Jefferson City", "Frankfort", "Concord",
    "Hartford", "Lansing", "Juneau", "Augusta", "Nashville", "Carson City", "Jackson", "Charleston", "Salt Lake City",
    "Boston", "Santa Fe", "Dover", "Providence", "Annapolis", "Harrisburg", "Trenton", "Oklahoma City", "Honolulu",
    "Columbia", "Atlanta", "Austin", "Salem", "Olympia", "Washington", "Tallahassee", "Phoenix", "Sacramento",
    "Little Rock", "Richmond", "Raleigh", "Montgomery", "Phoenix", "Hartford", "Bismarck", "Little Rock", "Topeka",
    "Oklahoma City", "Springfield", "Salem", "Austin", "Montgomery", "Lincoln", "Nashville", "Boise", "Jefferson City",
    "Indianapolis", "Sacramento", "Washington", "Columbia", "Des Moines", "Trenton", "Frankfort", "Saint Paul",
    "Salt Lake City", "Richmond", "Dover", "Annapolis", "Atlanta", "Boston", "Tallahassee", "Santa Fe", "Jackson",
    "Madison", "Raleigh", "Concord", "Harrisburg", "Columbus", "Carson City", "Providence", "Augusta", "Olympia",
    "Charleston", "Albany", "Lansing", "Denver", "Montpelier", "Helena", "Honolulu", "Cheyenne", "Juneau",
    "San Jose", "New York", "Los Angeles"
]   
      # Add more city names as needed
    weather_data_all = []
    
    for city_name in city_names:
        coordinates = get_lat_lon(api_key, city_name)
        if coordinates:
            weather_data = get_weather_data(api_key, *coordinates)
            weather_data['city'] = city_name  # Add city name to the weather data
            weather_data_all.append(weather_data)
    
    context['ti'].xcom_push(key='weather_data_all', value=weather_data_all)

# def transformdata(**context):
#     weather_data = context['ti'].xcom_pull(key='weather_data')
#     weather_data_list = []
#     for item in weather_data['list']:
#         date = pd.to_datetime(item['dt'], unit='s').date()
#         weather_data_list.append({
#             'city_name': weather_data['city']['name'],
#             'date': str(date),
#             'season': date.month if date.month in [1, 2, 3] else
#                       'Spring' if date.month in [4, 5, 6] else
#                       'Summer' if date.month in [7, 8, 9] else
#                       'Autumn' if date.month in [10, 11, 12] else None,
#             'avg_temp_c': item['temp']['day'],
#             'min_temp_c': item['temp']['min'],
#             'max_temp_c': item['temp']['max'],
#             'precipitation_mm': item['rain'] if 'rain' in item else 0,
#             'avg_wind_dir_deg': item['deg'],
#             'avg_wind_speed_kmh': item['speed'] * 3.6,
#             'peak_wind_gust_kmh': item['gust'] * 3.6,
#             'avg_sea_level_pres_hpa': item['pressure'],
#             'sunshine_total_min': (item['sunset'] - item['sunrise']) / 60,
#         })

#     weather_df = pd.DataFrame(weather_data_list)
#     weather_dict = weather_df.astype(str).to_dict()
#     context['ti'].xcom_push(key='weather_df', value=weather_dict)

def transformdata(**context):
    weather_data_all = context['ti'].xcom_pull(key='weather_data_all')
    weather_data_list = []

    # Iterate over each city's weather data
    for weather_data in weather_data_all:
        city_name = weather_data['city']
        # Iterate over each day's weather data
        for item in weather_data['list']:
            date = pd.to_datetime(item['dt'], unit='s').date()
            weather_data_list.append({
                'city_name': city_name,
                'date': str(date),
                'season': 'Winter' if date.month in [1, 2, 12] else
                          'Spring' if date.month in [3, 4, 5] else
                          'Summer' if date.month in [6, 7, 8] else
                          'Autumn' if date.month in [9, 10, 11] else None,
                'avg_temp_c': item['temp']['day'],
                'min_temp_c': item['temp']['min'],
                'max_temp_c': item['temp']['max'],
                'precipitation_mm': item['rain'] if 'rain' in item else 0,
                'avg_wind_dir_deg': item['deg'],
                'avg_wind_speed_kmh': item['speed'] * 3.6,
                'peak_wind_gust_kmh': item.get('gust', 0) * 3.6 if 'gust' in item else 0,
                'avg_sea_level_pres_hpa': item['pressure'],
                'sunshine_total_min': (item['sunset'] - item['sunrise']) / 60,
            })

    # Convert the list of dictionaries to a DataFrame
    weather_df = pd.DataFrame(weather_data_list)
    # Convert the DataFrame to a dictionary for XCom push
    weather_dict = weather_df.astype(str).to_dict()
    # Push the dictionary to XCom for the next task
    context['ti'].xcom_push(key='weather_df', value=weather_dict)




# def loaddata(**context):
#     fs = gcsfs.GCSFileSystem(project='data-225-project-406523')
#     weather_dict = context['ti'].xcom_pull(key='weather_df')
#     weather_df = pd.DataFrame.from_dict(weather_dict)


#     csv_filename = 'gs://data225/Preprocessed-stage1/realtimedata.csv'
    
#     if fs.exists(csv_filename):
#         with fs.open(csv_filename, 'r') as f:
#             existing_df = pd.read_csv(f)
#         combined_df = pd.concat([existing_df, weather_df])
#         with fs.open(csv_filename, 'w') as f:
#             combined_df.to_csv(f, index=False)
#     else:
#         with fs.open(csv_filename, 'w') as f:
#             weather_df.to_csv(f, index=False)





# def loaddata(**context):
#     fs = gcsfs.GCSFileSystem(project='data-225-project-406523')
#     weather_dict = context['ti'].xcom_pull(key='weather_df')
#     weather_df = pd.DataFrame.from_dict(weather_dict)
#     csv_filename = 'gs://data225-datawarehouse-input/Processed-realtimedata/realtimedata.csv'
    
#     # Check if the file exists and is not empty
#     if fs.exists(csv_filename):
#         with fs.open(csv_filename, 'r') as f:
#             try:
#                 existing_df = pd.read_csv(f)
#                 # If the file is not empty, concatenate the new data
#                 combined_df = pd.concat([existing_df, weather_df])
#             except pd.errors.EmptyDataError:
#                 # If the file is empty, just use the new data
#                 combined_df = weather_df
#         with fs.open(csv_filename, 'w') as f:
#             combined_df.to_csv(f, index=False)
#     else:
#         # If the file does not exist, create a new one with the new data
#         with fs.open(csv_filename, 'w') as f:
#             weather_df.to_csv(f, index=False)


# from google.cloud import bigquery
# import pandas as pd

# def loaddata(**context):
#     client = bigquery.Client(project='data-225-group-project')
#     table_id = 'data-225-group-project.climate_dwh.real_time_data'
#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
#     )

#     weather_dict = context['ti'].xcom_pull(key='weather_df')
#     weather_df = pd.DataFrame.from_dict(weather_dict)

#     # Convert data types
#     weather_df['date'] = pd.to_datetime(weather_df['date']).dt.date
#     weather_df['city_name'] = weather_df['city_name'].astype(str)
#     weather_df['season'] = weather_df['season'].astype(str)
#     weather_df['avg_temp_c'] = weather_df['avg_temp_c'].astype(float)
#     weather_df['min_temp_c'] = weather_df['min_temp_c'].astype(float)
#     weather_df['max_temp_c'] = weather_df['max_temp_c'].astype(float)
#     weather_df['precipitation_mm'] = weather_df['precipitation_mm'].astype(float)
#     weather_df['avg_wind_dir_deg'] = weather_df['avg_wind_dir_deg'].astype(float)
#     weather_df['avg_wind_speed_kmh'] = weather_df['avg_wind_speed_kmh'].astype(float)
#     weather_df['peak_wind_gust_kmh'] = weather_df['peak_wind_gust_kmh'].astype(float)
#     weather_df['avg_sea_level_pres_hpa'] = weather_df['avg_sea_level_pres_hpa'].astype(float)
#     weather_df['sunshine_total_min'] = weather_df['sunshine_total_min'].astype(float)

#     # Load the DataFrame into BigQuery
#     job = client.load_table_from_dataframe(weather_df, table_id, job_config=job_config)
#     job.result()  # Wait for the job to complete
         





def validate_data(**context):
    weather_data_all = context['ti'].xcom_pull(key='weather_data_all')
    missing_records = 0
    recorded = 0
    
    for weather_data in weather_data_all:
        if 'message' in weather_data:  # Skip if there's an error message
            continue
        city_name = weather_data['city']['name']
        for item in weather_data['list']:
            # If date is missing, use the current date
            item['dt'] = item.get('dt', datetime.datetime.now().timestamp())
            
            # If temperatures are below 0 Kelvin, set them to 0
            item['temp']['day'] = max(item['temp']['day'], 0)
            item['temp']['min'] = max(item['temp']['min'], 0)
            item['temp']['max'] = max(item['temp']['max'], 0)
            if 'deg' not in item:
                item['deg'] = get_average_from_bigquery(f'avg_wind_dir_deg WHERE city_name = "{city_name}"')  # CHANGE: Use city_name in the query
            if 'speed' not in item:
                item['speed'] = get_average_from_bigquery(f'avg_wind_speed_kmh WHERE city_name = "{city_name}"') / 3.6  # CHANGE: Use city_name in the query
            if 'pressure' not in item:
                item['pressure'] = get_average_from_bigquery(f'avg_sea_level_pres_hpa WHERE city_name = "{city_name}"')  # CHANGE: Use city_name in the query
            if 'sunset' not in item or 'sunrise' not in item:
                item['sunset'] = item.get('sunset', get_average_from_bigquery(f'sunshine_total_min WHERE city_name = "{city_name}"') * 60 + item['sunrise'])  # CHANGE: Use city_name in the query
                item['sunrise'] = item.get('sunrise', item['sunset'] - get_average_from_bigquery(f'sunshine_total_min WHERE city_name = "{city_name}"') * 60)  # CHANGE: Use city_name in the query
            recorded += 1
    missing_records = 101 - recorded  # CHANGE: Calculate missing_records as 101 - recorded
    client = bigquery.Client(project='data-225-group-project')
    query = f"""
INSERT `data-225-group-project.climate_dwh.data_tracking` (Date, Recorded, Missing_records)
VALUES (CURRENT_DATETIME(), {missing_records}, {recorded})
    """
    query_job = client.query(query)
    query_job.result()
    context['ti'].xcom_push(key='weather_data_all', value=weather_data_all)






from google.cloud import bigquery
import pandas as pd




# def loaddata(**context):
#     client = bigquery.Client(project='data-225-group-project')
#     table_id = 'data-225-group-project.climate_realtimedata_staging.real_time_data'
#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
#     )

#     weather_dict = context['ti'].xcom_pull(key='weather_df')
#     weather_df = pd.DataFrame.from_dict(weather_dict)

#     # Convert data types
#     weather_df['date'] = pd.to_datetime(weather_df['date']).dt.date
#     weather_df['city_name'] = weather_df['city_name'].astype(str)
#     weather_df['season'] = weather_df['season'].astype(str)
#     weather_df['avg_temp_c'] = weather_df['avg_temp_c'].astype(float)
#     weather_df['min_temp_c'] = weather_df['min_temp_c'].astype(float)
#     weather_df['max_temp_c'] = weather_df['max_temp_c'].astype(float)
#     weather_df['precipitation_mm'] = weather_df['precipitation_mm'].astype(float)
#     weather_df['avg_wind_dir_deg'] = weather_df['avg_wind_dir_deg'].astype(float)
#     weather_df['avg_wind_speed_kmh'] = weather_df['avg_wind_speed_kmh'].astype(float)
#     weather_df['peak_wind_gust_kmh'] = weather_df['peak_wind_gust_kmh'].astype(float)
#     weather_df['avg_sea_level_pres_hpa'] = weather_df['avg_sea_level_pres_hpa'].astype(float)
#     weather_df['sunshine_total_min'] = weather_df['sunshine_total_min'].astype(float)

#     # Convert DataFrame to string to avoid ArrowTypeError
#     weather_df = weather_df.astype(str)

#     # Load the DataFrame into BigQuery
#     job = client.load_table_from_dataframe(weather_df, table_id, job_config=job_config)
#     job.result()







def loaddata(**context):
    client = bigquery.Client(project='data-225-group-project')
    table_id = 'data-225-group-project.climate_realtimedata_staging.real_time_data'
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    weather_dict = context['ti'].xcom_pull(key='weather_df')
    weather_df = pd.DataFrame.from_dict(weather_dict)

    # Convert data types
    weather_df['date'] = pd.to_datetime(weather_df['date']).dt.date
    weather_df['city_name'] = weather_df['city_name'].astype(str)
    weather_df['season'] = weather_df['season'].astype(str)
    weather_df['avg_temp_c'] = weather_df['avg_temp_c'].astype(float)
    weather_df['min_temp_c'] = weather_df['min_temp_c'].astype(float)
    weather_df['max_temp_c'] = weather_df['max_temp_c'].astype(float)
    weather_df['precipitation_mm'] = weather_df['precipitation_mm'].astype(float)
    weather_df['avg_wind_dir_deg'] = weather_df['avg_wind_dir_deg'].astype(float)
    weather_df['avg_wind_speed_kmh'] = weather_df['avg_wind_speed_kmh'].astype(float)
    weather_df['peak_wind_gust_kmh'] = weather_df['peak_wind_gust_kmh'].astype(float)
    weather_df['avg_sea_level_pres_hpa'] = weather_df['avg_sea_level_pres_hpa'].astype(float)
    weather_df['sunshine_total_min'] = weather_df['sunshine_total_min'].astype(float)

    # Load the DataFrame into BigQuery
    job = client.load_table_from_dataframe(weather_df, table_id, job_config=job_config)
    job.result()







def copy_data_remove_duplicates(**context):
    client = bigquery.Client(project='data-225-group-project')
    
    # Define your source and destination tables
    source_table = '`data-225-group-project.climate_realtimedata_staging.real_time_data`'
    destination_table = '`data-225-group-project.climate_dwh.real_time_data`'
    
    # Write a SQL query to copy data from the source table to the destination table, removing duplicates
    query = f"""
    CREATE OR REPLACE TABLE {destination_table} AS
    SELECT DISTINCT *
    FROM {source_table}
    """
    
    # Run the query
    query_job = client.query(query)
    query_job.result()


















dag = DAG('realtime_api_2_csv_v255', description='Process CSV file with timestamp',
          schedule_interval='@daily',
          start_date=datetime(2023, 12, 1), catchup=True)




# Add this task to your DAG
copy_data_task = PythonOperator(task_id='copy_data', python_callable=copy_data_remove_duplicates, provide_context=True, dag=dag)

pullingdata_task = PythonOperator(task_id='pullingdata', python_callable=pullingdata, provide_context=True, dag=dag)
validate_data_task = PythonOperator(task_id='validate_data', python_callable=validate_data, provide_context=True, dag=dag)
transformdata_task = PythonOperator(task_id='transformdata', python_callable=transformdata, provide_context=True, dag=dag)
loaddata_task = PythonOperator(task_id='loaddata', python_callable=loaddata, provide_context=True, dag=dag)



# Define the order of tasks in your DAG
pullingdata_task >> validate_data_task >> transformdata_task >> loaddata_task >> copy_data_task








