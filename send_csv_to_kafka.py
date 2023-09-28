from kafka import KafkaProducer
import pandas as pd
import json

# Создаем Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Читаем файл CSV с разделителем "табуляция"
df = pd.read_csv('data.csv', sep='\t')

# Преобразуем каждую строку данных в формат JSON
for index, row in df.iterrows():
    data = {
        'mrf_id': str(row['mrf_id']),
        'location_name': str(row['location_name']),
        'san': str(row['san']),
        'mac': str(row['mac']),
        'content_id': str(row['content_id']),
        'content_name': str(row['content_name']),
        'session_content_time_start': str(row['session_content_time_start']),
        'session_content_time_end': str(row['session_content_time_end']),
        'session_content_on_period': str(row['session_content_on_period']),
        'channel_name': str(row['channel_name']),
        'channel_number': str(row['channel_number']),
        'is_favourite_channel': str(row['is_favourite_channel']),
        'favourite_channel_date': str(row['favourite_channel_date']),
        'content_type': str(row['content_type']),
        'watching_type': str(row['watching_type']),
        'period': str(row['period']),
        'accn_id': str(row['accn_id']),
        'load_dttm': str(row['load_dttm']),
        'src_id': str(row['src_id']),
        'eff_dttm': str(row['eff_dttm']),
        'exp_dttm': str(row['exp_dttm'])
    }

    # Преобразуем данные в строку JSON
    json_data = json.dumps(data)

    # Отправляем данные в Kafka
    producer.send('nebogin_iptv', key=str(index), value=json_data.encode('utf-8'))

# Закрываем Kafka Producer
producer.close()