from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
    weather_description = data['weather'][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp'])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise Time (Local Time)": sunrise_time,
        "Sunset Time (Local Time)": sunset_time
    }
    #convert to df
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    #aws_credentaials= {"key":"ASIAZJH4JGFKQQLX5EMB", "secret":"ifQvQbNbf8/aveVBJn+OLt0voTgJGIqtn3PtETk1", "token":"FwoGZXIvYXdzEOr//////////wEaDD0QNDcjEuTPmOvaFyJqGlwhMyvMKKh7kKkD5ecZW1t0H24LdK+0cKKVVNC9rpel5AeTy1b6StG3Z278ZRkyHrXHZ+VqE/DL5NtrnGv0SSTnY1PZFhUJJPdVoBInvDIBXPBN2hIXTc37Vsdk4qVlYG23fL7eSeDg9Cix1MbDBjIoyKh1n6b7UA+Hg0yOrIQ85xZUkDMl47DUhiUdKmRIONakQhGGPXlWYA=="}
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    dt_string = 'current_weather_data_portland_'+ dt_string
    s3_hook = S3Hook(aws_conn_id='aws_default')
    df_data.to_csv(f'/tmp/{dt_string}.csv', index=False)
    s3_hook.load_file(
        filename=f'/tmp/{dt_string}.csv',
        key=f'{dt_string}.csv',
        bucket_name='weatherapiforawsproject',
        replace=True
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weathermap_api',
    default_args=default_args,
    description='Run API',
    schedule='@daily',  
    catchup=False)
    
is_weather_api_ready = HttpSensor(
    task_id='is_weather_api_ready',
    http_conn_id='weathermap_api',
    endpoint='/data/2.5/weather?q=Portland&APPID=30135dbfb5e9413c344c5efc42a81e3b',
    dag=dag)

extract_weather_data = HttpOperator(
    task_id='extract_weather_data',
    http_conn_id='weathermap_api',
    endpoint='data/2.5/weather?q=Portland&APPID=30135dbfb5e9413c344c5efc42a81e3b',
    method='GET',
    response_filter=lambda r: json.loads(r.text),
    log_response=True,
    dag=dag
)

transform_load_weather_data = PythonOperator(
    task_id='transform_load_weather_data',
    python_callable=transform_load_data,
    dag=dag,
)

is_weather_api_ready >> extract_weather_data >> transform_load_weather_data