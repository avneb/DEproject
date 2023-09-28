# загрузка данных из csv в greenplum
import csv
import psycopg2


TABLE_NAME = 'iptv_tv'
FILE_NAME = 'iptv_tv.csv'
# Подключение к PostgreSQL
connection = psycopg2.connect(
    host='localhost',
    port='5432',
    database='pg',
    user='pg',
    password='1234'
)

# Чтение файла CSV и получение названий атрибутов
with open(FILE_NAME, 'r', encoding='utf-8') as file:
    csv_reader = csv.reader(file, delimiter='\t')
    attribute_names = next(csv_reader)  # Прочитать первую строку с названиями атрибутов
    print(attribute_names)

#'mrf_id', 'location_name', 'san', 'mac', 'content_id', 'content_name', 'session_content_time_start', 'session_content_time_end', 'session_content_on_period', 'channel_name', 'channel_number', 'is_favourite_channel', 'favourite_channel_date', 'content_type', 'watching_type', 'period', 'accn_id', 'load_dttm', 'src_id', 'eff_dttm', 'exp_dttm']



# Создание таблицы в PostgreSQL
create_table_query = f'''
CREATE TABLE {TABLE_NAME} (
    {', '.join([f'{name} VARCHAR' for name in attribute_names])}
);
'''

with connection.cursor() as cursor:
    cursor.execute(create_table_query)
    connection.commit()

# Чтение файла CSV и вставка данных в PostgreSQL
with open(FILE_NAME, 'r', encoding='utf-8') as file:
    csv_reader = csv.reader(file, delimiter='\t')
    next(csv_reader)  # Пропустить первую строку с названиями атрибутов

    attribute_placeholders = ', '.join(['%s' for _ in attribute_names])
    insert_query = f'''
    INSERT INTO {TABLE_NAME} ({', '.join(attribute_names)})
    VALUES ({attribute_placeholders});
    '''

    for row in csv_reader:
        with connection.cursor() as cursor:
            cursor.execute(insert_query, row)
            connection.commit()