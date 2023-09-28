from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Подключение к Greenplum
def connect_greenplum():
    conn = psycopg2.connect(
        host='localhost',
        port='5432',
        database='pg',
        user='pg',
        password='1234'
    )
    return conn

# Загрузка данных в материализованное представление в Greenplum
def load_data_to_materialized_view(mv_name):
    connection = connect_greenplum()

    with connection.cursor() as cursor:
        # Обновление материализованного представления
        update_query = f"REFRESH MATERIALIZED VIEW {mv_name}"
        cursor.execute(update_query)

        # Применение изменений
        connection.commit()

    connection.close()

# Создание DAG
dag = DAG(
    'refresh_materialized_view',
    description='Обновление материализованного представления в Greenplum раз в сутки в 1:00',
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 9, 25),  # Установить дату начала выполнения DAG
    catchup=False
)

# Задачи для обновления материализованного представления
refresh_mv_nebogin_iptv = PythonOperator(
    task_id='refresh_materialized_view_task',
    python_callable=load_data_to_materialized_view(mv_name='mv_nebogin_iptv'),
    dag=dag
)
refresh_mv_nebogin_loc_rate = PythonOperator(
    task_id='refresh_materialized_view_task',
    python_callable=load_data_to_materialized_view(mv_name='mv_nebogin_loc_rate'),
    dag=dag
)
refresh_mv_nebogin_channel_rate = PythonOperator(
    task_id='refresh_materialized_view_task',
    python_callable=load_data_to_materialized_view(mv_name='mv_nebogin_channel_rate'),
    dag=dag
)
refresh_mv_nebogin_day_rate = PythonOperator(
    task_id='refresh_materialized_view_task',
    python_callable=load_data_to_materialized_view(mv_name='mv_nebogin_day_rate'),
    dag=dag
)
refresh_mv_nebogin_day_h_rate = PythonOperator(
    task_id='refresh_materialized_view_task',
    python_callable=load_data_to_materialized_view(mv_name='mv_nebogin_day_h_rate'),
    dag=dag
)

# Установка зависимостей между задачами
refresh_mv_nebogin_iptv >> refresh_mv_nebogin_loc_rate >> refresh_mv_nebogin_channel_rate  >> refresh_mv_nebogin_day_rate >> refresh_mv_nebogin_day_h_rate

