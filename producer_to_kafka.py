import time
import json
from kafka import KafkaProducer
import requests


kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'new-topic'

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None

def get_weather_detail(openweathermap_api_endpoint):
    api_response = requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    city_name = json_data['name']
    humidity = json_data['main']['humidity']
    temperature = json_data['main']['temp']
    json_message = {"CityName": city_name, "Temperature": temperature, "Humidity": humidity, "CreationTime": time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message

while True:
    city_name = "Ho Chi Minh City"
    appid = "d1f255a1251258e9884cf848113b4ab0"
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid="+appid+"&q="+city_name
    json_message= get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic, json_message)
    print("Published message 1: "+ json.dumps(json_message))
    print("Wait for 2 seconds ... ")
    time.sleep(2)

    city_name = "Hanoi"
    appid = "d1f255a1251258e9884cf848113b4ab0"
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic, json_message)
    print("Published message 2: " + json.dumps(json_message))
    print("Wait for 2 seconds ... ")
    time.sleep(2)