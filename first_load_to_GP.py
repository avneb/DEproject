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

# Создание таблицы в PostgreSQL
create_table_query = f'''
CREATE TABLE {TABLE_NAME} (
    {', '.join([f'{name} VARCHAR' for name in attribute_names])}
);
'''

with connection.cursor() as cursor:
    cursor.execute(create_table_query)
    connection.commit()

# Чтение файла CSV и вставка данных в greenplum
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