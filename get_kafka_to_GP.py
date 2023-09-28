import json
from kafka import KafkaConsumer
import psycopg2

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'  # replace with your Kafka broker(s) address
topic_name = 'nebogin_iptv'  # replace with your topic name

# Подключение к GP
connection = psycopg2.connect(
    host='localhost',
    port='5432',
    database='pg',
    user='pg',
    password='1234'
)

TABLE_NAME = 'iptv_tv'
attribute_names = ['mrf_id', 'location_name', 'san', 'mac', 'content_id', 'content_name', 'session_content_time_start',
                   'session_content_time_end', 'session_content_on_period', 'channel_name', 'channel_number',
                   'is_favourite_channel', 'favourite_channel_date', 'content_type', 'watching_type', 'period',
                   'accn_id', 'load_dttm', 'src_id', 'eff_dttm', 'exp_dttm']

# Создаем Kafka Consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda value: json.loads(value.decode('utf-8')))

cursor = connection.cursor()

# Читаем данные из Kafka
for message in consumer:
    data = message.value

    # Извлекаем данные JSON
    attribute_values = [data.get(attr_name) for attr_name in attribute_names]
    insert_query = f"INSERT INTO {TABLE_NAME} VALUES ({','.join(['%s'] * len(attribute_names))})"

    cursor.execute(insert_query, attribute_values)
    connection.commit()

# Закрываем Kafka Consumer
consumer.close()