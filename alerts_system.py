import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
import io
import sys
import os

default_args = {
    'owner': 'd.aladina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 6, 7)
}

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, dag_id='alerts_system_feed_message')
def alerts_system_feed_message():

    def check_anomaly(df, metric, a=4, n=5):
        # функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        # shift(1) сдвигает на 1 шаг границу пятнадцатиминуток, чтобы в том случае,
        # если наша пятнадцатиминутка окажется аномальной, она не повлияла 
        # на границы доверительного интервала и не сдвинула их относительно нормы
        
        df['iqr'] = df['q75'] - df['q25'] # межквартильный размах
        df['up'] = df['q75'] + a*df['iqr'] # верхняя граница
        df['low'] = df['q25'] - a*df['iqr'] # нижняя граница

        # сглаживаем границы доверительных интервалов, чтобы они были не такие рваные
        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        # интерпретация результатов межквартильного размаха
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df
    
    @task()
    def run_alerts(chat=None):
        #система алертов
        #chat_id = chat or -1002614297220
        #my_token = '8706906706:AAG_S7YVq3gFFhzK2-FjGjXbSoEVlRnbxsQ' # токен моего созданного бота
        #bot = telegram.Bot(token=my_token) # получаю доступ к боту

        connection = {'host': 'http://clickhouse.lab.karpov.courses:8123',
        'database':'simulator_20260420',
        'user':'student',
        'password':'dpo_python_2020'
        }

        q_feed = '''
            SELECT 
            toStartOfFifteenMinutes(time) as ts, toDate(time) as dt,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_feed,
            countIf(user_id, action='like') as likes,
            countIf(user_id, action='view') as views,
            likes/views as ctr
            FROM simulator_20260420.feed_actions
            WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, dt, hm
            ORDER BY ts'''
        feed_df = pandahouse.read_clickhouse(q_feed, connection=connection)

        q_message = '''
            SELECT 
            toStartOfFifteenMinutes(time) as ts, toDate(time) as dt,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_message,
            count(user_id) as messages
            FROM simulator_20260420.message_actions
            WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, dt, hm
            ORDER BY ts'''
        message_df = pandahouse.read_clickhouse(q_message, connection=connection)

        general_data = [
            (feed_df, ['users_feed', 'likes', 'views', 'ctr']),
            (message_df, ['users_message', 'messages'])
        ]

        for data, metrics_list in general_data:
            for metric in metrics_list:
                df = data[['ts', 'dt', 'hm', metric]].copy()
                is_alert, df = check_anomaly(df, metric)

                if is_alert == 1:
                    msg = ''' Метрика {metric}:\n
                              текущее значение {current_val:.2f}\n
                              отклонение от предыдущего значения {last_val_diff:.2%}\n
                              https://superset.lab.karpov.courses/superset/dashboard/8739/
                              '''.format(metric=metric, 
                                         current_val=df[metric].iloc[-1],
                                         last_val_diff=1-(df[metric].iloc[-1]/df[metric].iloc[-2]))

                    sns.set(rc={'figure.figsize': (16, 10)})
                    plt.tight_layout()

                    ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                    ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                    ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

                    for ind, label in enumerate(ax.get_xticklabels()):
                        if ind % 2 == 0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)

                    ax.set(xlabel='time')
                    ax.set(ylabel=metric)

                    ax.set_title(metric)
                    ax.set(ylim=(0, None))

                    plot_object = io.BytesIO()
                    ax.figure.savefig(plot_object)
                    plot_object.seek(0)
                    plot_object.name = '{0}.png'.format(metric)
                    plt.close()

                    #bot.sendMessage(chat_id=chat_id, text=msg)
                    #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                    print(msg)

    run_alerts() 
alerts_system_feed_message_run = alerts_system_feed_message()
