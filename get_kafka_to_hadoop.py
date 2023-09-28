from kafka import KafkaConsumer
import pandas as pd
import json
import pyarrow.parquet as pq
import pyarrow as pa

# Создаем Kafka Consumer
consumer = KafkaConsumer(
    'topic_name',  # Название топика Kafka
    bootstrap_servers='localhost:9092',  # Адрес и порт вашего Kafka брокера
    group_id='my_group',  # Группа потребителей Kafka
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Функция преобразования значения в JSON
)

# Создаем пустой список для хранения данных
data_list = []

# Читаем данные из Kafka и сохраняем в список
for message in consumer:
    data = message.value
    data_list.append(data)

    # Завершаем чтение после получения достаточного количества сообщений
    if len(data_list) >= 1000:
        break

# Создаем DataFrame из списка данных
df = pd.DataFrame(data_list)

# Сохраняем DataFrame в формате Parquet в HDFS
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')

# Закрываем Kafka Consumer
consumer.close()