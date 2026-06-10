import pandahouse as ph
import pandas as pd

from airflow.decorators import dag, task
from datetime import datetime, timedelta

# подключение к схеме откуда выгружаем данные
connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'database': 'simulator_20260420',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# подключение к схеме куда загружаем данные
connection_test = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}

# дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd.aladina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 5, 28),
}

# интервал запуска DAG
schedule_interval = '0 20 * * *'


# задаём DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, dag_id='dag_etl_action_slice_report_2')
def dag_etl_action_slice_report_2():
    @task()
    # выгрузка данных по активности юзеров в ленте и мессенджере
    def extract_actions_info():
        query = '''
        WITH 
        -- сколько юзер отсылает сообщений и скольким людям пишет (исходящие)
        sent_stats AS (
        SELECT 
            user_id AS users_id,
            count() AS messages_sent,
            countDistinct(receiver_id) AS unique_recipients
        FROM simulator_20260420.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY user_id
        ),
        -- сколько юзер получает сообщений и сколько людей пишет ему (входящие)
        received_stats AS (
         SELECT 
            receiver_id AS users_id,
            count() AS messages_received,
            countDistinct(user_id) AS unique_senders
        FROM simulator_20260420.message_actions
        WHERE toDate(time) = today() - 1
        GROUP BY receiver_id
        ),
        -- сколько лайков и просмотров в ленте
        feed_stats AS (
        SELECT 
            user_id AS users_id,
            countIf(action = 'view') AS views_count,
            countIf(action = 'like') AS likes_count
        FROM simulator_20260420.feed_actions
        WHERE toDate(time) = today() - 1
        GROUP BY user_id
        )

        -- объединили информацию по мессенджеру и ленте
        SELECT 
            today() - 1 AS event_date,
            s.users_id AS users_id,
            s.messages_sent AS messages_sent,
            s.unique_recipients AS users_sent,
            r.messages_received AS messages_received,
            r.unique_senders AS users_received,
            f.views_count AS views,
            f.likes_count AS likes
        FROM sent_stats s
        FULL JOIN received_stats r USING (users_id)
        FULL JOIN feed_stats f USING (users_id)
        ORDER BY users_id
        '''

        df_actions_info = ph.read_clickhouse(query, connection=connection)
        return df_actions_info

    @task()
    # выгрузка данных с демографическими признаками юзеров
    def extract_users_info():
        query = '''
        -- демографические данные
        SELECT 
            user_id AS users_id,
            gender,
            age,
            os
        FROM simulator_20260420.feed_actions
        UNION DISTINCT
        SELECT 
            user_id AS users_id,
            gender,
            age,
            os
        FROM simulator_20260420.message_actions
        '''

        df_users_info = ph.read_clickhouse(query, connection=connection)
        return df_users_info

    @task()
    # обработка данных по гендеру
    def transform_gender(df_actions_info, df_users_info):
        df_gender_slice = pd.merge(df_actions_info, df_users_info[['users_id', 'gender']], how='left', on='users_id')
        df_gender_slice = df_gender_slice.groupby(by=['event_date', 'gender'], as_index=False).agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_received=('messages_received', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            users_received=('users_received', 'sum'),
            users_sent=('users_sent', 'sum'),
        )
        df_gender_slice.insert(1, 'dimension', 'gender')
        df_gender_slice.rename(columns={'gender': 'dimension_value'}, inplace=True)
        df_gender_slice['dimension_value'] = df_gender_slice['dimension_value'].astype(str)
        return df_gender_slice

    @task()
    # обработка данных по возрасту
    def transform_age(df_actions_info, df_users_info):
        df_age_slice = pd.merge(df_actions_info, df_users_info[['users_id', 'age']], how='left', on='users_id')
        df_age_slice = df_age_slice.groupby(by=['event_date', 'age'], as_index=False).agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_received=('messages_received', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            users_received=('users_received', 'sum'),
            users_sent=('users_sent', 'sum'),
        )
        df_age_slice.insert(1, 'dimension', 'age')
        df_age_slice.rename(columns={'age': 'dimension_value'}, inplace=True)
        df_age_slice['dimension_value'] = df_age_slice['dimension_value'].astype(str)
        return df_age_slice

    @task()
    # обработка данных по операционке
    def transform_os(df_actions_info, df_users_info):
        df_os_slice = pd.merge(df_actions_info, df_users_info[['users_id', 'os']], how='left', on='users_id')
        df_os_slice = df_os_slice.groupby(by=['event_date', 'os'], as_index=False).agg(
            views=('views', 'sum'),
            likes=('likes', 'sum'),
            messages_received=('messages_received', 'sum'),
            messages_sent=('messages_sent', 'sum'),
            users_received=('users_received', 'sum'),
            users_sent=('users_sent', 'sum'),
        )
        df_os_slice.insert(1, 'dimension', 'os')
        df_os_slice.rename(columns={'os': 'dimension_value'}, inplace=True)
        df_os_slice['dimension_value'] = df_os_slice['dimension_value'].astype(str)
        return df_os_slice

    @task()
    # загрузка данных в таблицу отчёта (или создание таблицы отчёта)
    def load(df_gender_slice, df_age_slice, df_os_slice):
        df_general = pd.concat([df_gender_slice, df_age_slice, df_os_slice], ignore_index=True)

        int_cols = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']
        df_general[int_cols] = df_general[int_cols].astype(int)

        create_query = """
                CREATE TABLE IF NOT EXISTS test.aladina_etl_result_2
                (
                    event_date        Date,
                    dimension         String,
                    dimension_value   String,
                    views             Int64,
                    likes             Int64,
                    messages_received Int64,
                    messages_sent     Int64,
                    users_received    Int64,
                    users_sent        Int64
                )
                ENGINE = MergeTree()
                ORDER BY (event_date, dimension, dimension_value)
            """
        ph.execute(create_query, connection=connection_test)
        ph.to_clickhouse(df=df_general, table='aladina_etl_result_2', index=False, connection=connection_test)

    # исполнение тасков
    df_actions = extract_actions_info()
    df_users = extract_users_info()

    df_cube_gender = transform_gender(df_actions, df_users)
    df_cube_age = transform_age(df_actions, df_users)
    df_cube_os = transform_os(df_actions, df_users)

    load(df_cube_gender, df_cube_age, df_cube_os)

dag_etl_action_slice_report_run = dag_etl_action_slice_report_2()