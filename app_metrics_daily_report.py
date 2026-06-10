from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
import telegram
import matplotlib.pyplot as plt
import io
import numpy as np
import seaborn as sns
import os

connection = {'host': 'http://clickhouse.lab.karpov.courses:8123',
'database':'simulator_20260420',
'user':'student',
'password':'dpo_python_2020'
}

default_args = {
    'owner': 'd.aladina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 6, 7),
}

# ежедневно 11:00 утра 
schedule_interval = '0 11 * * *'

sns.set()

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, dag_id='app_metrics_daily_report')
def app_metrics_daily_report():
    
    def get_plot(data_feed, data_msg, data_dau_all, data_new_users):
        # объединяем все данные в один датафрейм, чтобы удобнее было строить графики
        data = pd.merge(data_feed, data_msg, on='date')
        data = pd.merge(data, data_dau_all, on='date')
        data = pd.merge(data, data_new_users, on='date')
        # общее количество событий в приложении
        data['events_app'] = data['events'] + data['msgs']
    
        # одним графиком мы не обойдёмся, поэтому будем работать со списком графиков
        plot_objects = []
    
        # ВСЁ ПРИЛОЖЕНИЕ
        # задаём параметры для дашборда, где будут расположены графики
        fig, axes = plt.subplots(3, figsize=(10,14))
        fig.suptitle('Статистика по всему приложению за 7 дней')
        app_dict = {0 : {'y' : ['events_app'], 'title':'Events'},
                    1 : {'y' : ['users','users_ios','users_android'], 'title':'DAU'},
                    2 : {'y' : ['new_users','new_users_ads','new_users_organic'], 'title':'New users'}}
    
        # строим наш башборд с графиками
        for i in range(3):
            for y in app_dict[i]['y']:
                sns.lineplot(ax=axes[i], data=data, x='date', y=y)
            axes[i].set_title(app_dict[(i)]['title'])
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            axes[i].legend(app_dict[i]['y'])
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                
        # сохраняем дашборд как файловый объект
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'app_stat.png'
        plot_object.seek(0)
        plt.close()
    
        # добавляем дашборд в наш финальный список
        plot_objects.append(plot_object)
    
        # ЛЕНТА
        fig, axes = plt.subplots(2, 2, figsize=(14,14))
        fig.suptitle('Статистика по ленте за 7 дней')
        plot_dict = {(0, 0) : {'y' : 'users_feed', 'title':'DAU'},
                    (0, 1) : {'y' : 'likes', 'title':'Likes'},
                    (1, 0) : {'y' : 'views', 'title':'Views'},
                    (1, 1) : {'y' : 'ctr', 'title':'CTR'}}
    
        for i in range(2):
            for j in range(2):
                sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'feed_stat.png'
        plot_object.seek(0)
        plt.close()
    
        plot_objects.append(plot_object)
    
        # МЕССЕНДЖЕР
        fig, axes = plt.subplots(3, figsize=(10,14))
        fig.suptitle('Статистика по мессенджеру за 7 дней')
        msg_dict = {0 : {'y' : 'users_msg', 'title':'DAU'},
                    1 : {'y' : 'msgs', 'title':'Messages'},
                    2 : {'y' : 'mpu', 'title':'Messages per users'}}
    
        for i in range(3):
            sns.lineplot(ax=axes[i], data=data, x='date', y=msg_dict[i]['y'])
            axes[i].set_title(msg_dict[(i)]['title'])
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'msgs_stat.png'
        plot_object.seek(0)
        plt.close()
    
        plot_objects.append(plot_object)
        
        return plot_objects

    @task()
    def app_report(chat=None):
        # задаём инфу по боту и чату
        # chat_id = chat or -1002614297220
        # my_token = '8706906706:AAG_S7YVq3gFFhzK2-FjGjXbSoEVlRnbxsQ' # токен моего созданного бота
        # bot = telegram.Bot(token=my_token) 
    
        # создание текстового шаблона отчёта
        msg = '''Отчёт по всему приложению за {date}
    
        Всего событий: {events}
            👤DAU: {users} ({to_users_day_ago:+.2%} к дню назад, {to_users_week_ago:+.2%} к неделе назад)
            👤DAU by platform:
                🍏iOS: {users_ios} ({to_users_ios_day_ago:+.2%} к дню назад, {to_users_ios_week_ago:+.2%} к неделе назад)
                🤖Android: {users_android} ({to_users_android_day_ago:+.2%} к дню назад, {to_users_android_week_ago:+.2%} к неделе назад)
            👥New users: {new_users} ({to_new_users_day_ago:+.2%} к дню назад, {to_new_users_week_ago:+.2%} к неделе назад)
            👥New users by source:
                🧲ads: {new_users_ads} ({to_new_users_ads_day_ago:+.2%} к дню назад, {to_new_users_ads_week_ago:+.2%} к неделе назад)
                🌱organic: {new_users_organic} ({to_new_users_organic_day_ago:+.2%} к дню назад, {to_new_users_organic_week_ago:+.2%} к неделе назад)

        ЛЕНТА:
            👤DAU: {users_feed} ({to_users_feed_day_ago:+.2%} к дню назад, {to_users_feed_week_ago:+.2%} к неделе назад)
            ❤️Likes: {likes} ({to_likes_day_ago:+.2%} к дню назад, {to_likes_week_ago:+.2%} к неделе назад)
            👀Views: {views} ({to_views_day_ago:+.2%} к дню назад, {to_views_week_ago:+.2%} к неделе назад)
            🎯CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%} к дню назад, {to_ctr_week_ago:+.2%} к неделе назад)
            📝Posts: {posts} ({to_posts_day_ago:+.2%} к дню назад, {to_posts_week_ago:+.2%} к неделе назад)
            📊Likes per user: {lpu:.2f} ({to_lpu_day_ago:+.2%} к дню назад, {to_lpu_week_ago:+.2%} к неделе назад)
    
        МЕССЕНДЖЕР:
            👤DAU: {users_msg} ({to_users_msg_day_ago:+.2%} к дню назад, {to_users_msg_week_ago:+.2%} к неделе назад)
            💭Messages: {msgs} ({to_msgs_day_ago:+.2%} к дню назад, {to_msgs_week_ago:+.2%} к неделе назад)
            📊Messages per user: {mpu:.2f} ({to_mpu_day_ago:+.2%} к дню назад, {to_mpu_week_ago:+.2%} к неделе назад)
        '''
    
        # данные ленты для отчёта
        query = '''
            SELECT 
               toDate(time) as date,
               uniqExact(user_id) as users_feed,
               countIf(user_id, action = 'like') as likes,
               countIf(user_id, action = 'view') as views,
               100 * likes / views as ctr,
               likes + views as events,
               uniqExact(post_id) as posts,
               likes / users_feed as lpu
           
            FROM simulator_20260420.feed_actions
            WHERE toDate(time) BETWEEN today() - 8 and today() - 1
            GROUP BY date
            ORDER BY date'''
        data_feed = ph.read_clickhouse(query, connection=connection)
    
        # данные мессенджера для отчёта
        query = '''
            SELECT 
               toDate(time) as date,
               uniqExact(user_id) as users_msg,
               count(user_id) as msgs,
               msgs / users_msg as mpu
           
            FROM simulator_20260420.message_actions
            WHERE toDate(time) BETWEEN today() - 8 and today() - 1
            GROUP BY date
            ORDER BY date'''
        data_msg = ph.read_clickhouse(query, connection=connection)
        
        # данные по DAU всего приложения для отчёта
        query = '''
            SELECT 
                date,
                uniqExact(user_id) as users,
                uniqExactIf(user_id, os='iOS') as users_ios,
                uniqExactIf(user_id, os='Android') as users_android
            FROM (SELECT DISTINCT
                    toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20260420.feed_actions
                WHERE toDate(time) BETWEEN today() - 8 and today() - 1
                UNION ALL
                SELECT DISTINCT
                    toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20260420.message_actions
                WHERE toDate(time) BETWEEN today() - 8 and today() - 1) as t
            GROUP BY date
            ORDER BY date'''
        data_dau_all = ph.read_clickhouse(query, connection=connection)
    
        # данные по новым пользователям всего приложения для отчёта
        query = '''
            SELECT
                date,
                uniqExact(user_id) as new_users,
                uniqExactIf(user_id, source = 'ads') as new_users_ads,
                uniqExactIf(user_id, source = 'organic') as new_users_organic
            FROM (SELECT
                    user_id,
                    source,
                    min(min_dt) as date
                FROM (SELECT
                        user_id,
                        min(toDate(time)) as min_dt,
                        source
                    FROM simulator_20260420.feed_actions
                    WHERE toDate(time) BETWEEN today() - 90 and today() - 1
                    GROUP BY user_id, source
                    UNION ALL
                    SELECT
                        user_id,
                        min(toDate(time)) as min_dt,
                        source
                    FROM simulator_20260420.message_actions
                    WHERE toDate(time) BETWEEN today() - 90 and today() - 1
                    GROUP BY user_id, source) as t
                GROUP BY user_id, source) as tab
            WHERE date BETWEEN today() - 8 and today() - 1
            GROUP BY date'''
        data_new_users = ph.read_clickhouse(query, connection=connection)
    
        # задаём переменные дат для отчёта
        today = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = today - pd.DateOffset(days=1)
        week_ago = today - pd.DateOffset(days=7)
    
        # поправим проблемы с типами данных
        data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
        data_msg['date'] = pd.to_datetime(data_msg['date']).dt.date
        data_dau_all['date'] = pd.to_datetime(data_dau_all['date']).dt.date
        data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
    
        data_feed = data_feed.astype({'users_feed': int, 'likes': int, 'views': int, 'events': int, 'posts': int})
        data_msg = data_msg.astype({'users_msg': int, 'msgs': int})
        data_dau_all = data_dau_all.astype({'users': int, 'users_ios': int, 'users_android': int})
        data_new_users = data_new_users.astype({'new_users': int, 'new_users_ads': int, 'new_users_organic': int})
    
        # заполняем текстовый шаблон данными
        report = msg.format(date = today.date(),
                            events = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0] 
                            + data_feed[data_feed['date'] == today.date()]['events'].iloc[0],
                            # общий DAU приложения
                            users = data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0],
                            to_users_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == day_ago.date()]['users'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == day_ago.date()]['users'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == day_ago.date()]['users'].iloc[0] != 0 else 0,
                            to_users_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == week_ago.date()]['users'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == week_ago.date()]['users'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == week_ago.date()]['users'].iloc[0] != 0 else 0,
                            # DAU приложения по платформе IOS
                            users_ios = data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0],
                            to_users_ios_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == day_ago.date()]['users_ios'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == day_ago.date()]['users_ios'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == day_ago.date()]['users_ios'].iloc[0] != 0 else 0,
                            to_users_ios_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == week_ago.date()]['users_ios'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == week_ago.date()]['users_ios'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == week_ago.date()]['users_ios'].iloc[0] != 0 else 0,
                            # DAU приложения по платформе Android
                            users_android = data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0],
                            to_users_android_day_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == day_ago.date()]['users_android'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == day_ago.date()]['users_android'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == day_ago.date()]['users_android'].iloc[0] != 0 else 0,
                            to_users_android_week_ago = (data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0]
                                              - data_dau_all[data_dau_all['date'] == week_ago.date()]['users_android'].iloc[0])
                                              / data_dau_all[data_dau_all['date'] == week_ago.date()]['users_android'].iloc[0]
                                                if data_dau_all[data_dau_all['date'] == week_ago.date()]['users_android'].iloc[0] != 0 else 0,
                            # данные по новым пользователям приложения
                            new_users = data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
                            to_new_users_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                              - data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0])
                                              / data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0]
                                                if data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0] != 0 else 0,
                            to_new_users_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                              - data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0])
                                              / data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0]
                                                if data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0] != 0 else 0,
                            # данные по новым пользователям по источнику ads
                            new_users_ads = data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0],
                            to_new_users_ads_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                              - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0])
                                              / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0]
                                                if data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0] != 0 else 0,
                            to_new_users_ads_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                              - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0])
                                              / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0]
                                                if data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0] != 0 else 0,
                            # данные по новым пользователям по источнику organic
                            new_users_organic = data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0],
                            to_new_users_organic_day_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0]
                                              - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0])
                                              / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0] 
                                                if data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0] != 0 else 0,
                            to_new_users_organic_week_ago = (data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0]
                                              - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0])
                                              / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0]
                                                if data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0] != 0 else 0,
                            # данные ленты
                            # DAU ленты
                            users_feed = data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0],
                            to_users_feed_day_ago = (data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0] != 0 else 0,
                            to_users_feed_week_ago = (data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0] != 0 else 0,
                            # лайки
                            likes = data_feed[data_feed['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago = (data_feed[data_dau_all['date'] == today.date()]['likes'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0] != 0 else 0,
                            to_likes_week_ago = (data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0] != 0 else 0,
                            # просмотры
                            views = data_feed[data_feed['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago = (data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0] != 0 else 0,
                            to_views_week_ago = (data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0] != 0 else 0,
                            # CTR
                            ctr = data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0],
                            to_ctr_day_ago = (data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['ctr'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['ctr'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['ctr'].iloc[0] != 0 else 0,
                            to_ctr_week_ago = (data_feed[data_feed['date'] == today.date()]['ctr'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['ctr'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['ctr'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['ctr'].iloc[0] != 0 else 0,
                            # посты
                            posts = data_feed[data_feed['date'] == today.date()]['posts'].iloc[0],
                            to_posts_day_ago = (data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0] != 0 else 0,
                            to_posts_week_ago = (data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0] != 0 else 0,
                            # LPU (лайки на пользователя)
                            lpu = data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0],
                            to_lpu_day_ago = (data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0]
                                              - data_feed[data_feed['date'] == day_ago.date()]['lpu'].iloc[0])
                                              / data_feed[data_feed['date'] == day_ago.date()]['lpu'].iloc[0]
                                                if data_feed[data_feed['date'] == day_ago.date()]['lpu'].iloc[0] != 0 else 0,
                            to_lpu_week_ago = (data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0]
                                              - data_feed[data_feed['date'] == week_ago.date()]['lpu'].iloc[0])
                                              / data_feed[data_feed['date'] == week_ago.date()]['lpu'].iloc[0]
                                                if data_feed[data_feed['date'] == week_ago.date()]['lpu'].iloc[0] != 0 else 0,
                            # данные мессенджера
                            # DAU мессенджера
                            users_msg = data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0],
                            to_users_msg_day_ago = (data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0]
                                              - data_msg[data_msg['date'] == day_ago.date()]['users_msg'].iloc[0])
                                              / data_msg[data_msg['date'] == day_ago.date()]['users_msg'].iloc[0]
                                                if data_msg[data_msg['date'] == day_ago.date()]['users_msg'].iloc[0] != 0 else 0,
                            to_users_msg_week_ago = (data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0]
                                              - data_msg[data_msg['date'] == week_ago.date()]['users_msg'].iloc[0])
                                              / data_msg[data_msg['date'] == week_ago.date()]['users_msg'].iloc[0]
                                                if data_msg[data_msg['date'] == week_ago.date()]['users_msg'].iloc[0] != 0 else 0,
                            # сообщения
                            msgs = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0],
                            to_msgs_day_ago = (data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0]
                                              - data_msg[data_msg['date'] == day_ago.date()]['msgs'].iloc[0])
                                              / data_msg[data_msg['date'] == day_ago.date()]['msgs'].iloc[0]
                                                if data_msg[data_msg['date'] == day_ago.date()]['msgs'].iloc[0] != 0 else 0,
                            to_msgs_week_ago = (data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0]
                                              - data_msg[data_msg['date'] == week_ago.date()]['msgs'].iloc[0])
                                              / data_msg[data_msg['date'] == week_ago.date()]['msgs'].iloc[0]
                                                if data_msg[data_msg['date'] == week_ago.date()]['msgs'].iloc[0] != 0 else 0,
                            # MPU (сообщения на пользователя)
                            mpu = data_msg[data_msg['date'] == today.date()]['mpu'].iloc[0],
                            to_mpu_day_ago = (data_msg[data_msg['date'] == today.date()]['mpu'].iloc[0]
                                              - data_msg[data_msg['date'] == day_ago.date()]['mpu'].iloc[0])
                                              / data_msg[data_msg['date'] == day_ago.date()]['mpu'].iloc[0]
                                                if data_msg[data_msg['date'] == day_ago.date()]['mpu'].iloc[0] != 0 else 0,
                            to_mpu_week_ago = (data_msg[data_msg['date'] == today.date()]['mpu'].iloc[0]
                                              - data_msg[data_msg['date'] == week_ago.date()]['mpu'].iloc[0])
                                              / data_msg[data_msg['date'] == week_ago.date()]['mpu'].iloc[0]
                                                if data_msg[data_msg['date'] == week_ago.date()]['mpu'].iloc[0] != 0 else 0,
        )
    
        # сохраняем изображения в переменную
        plot_objects = get_plot(data_feed, data_msg, data_dau_all, data_new_users)
        # печать отчёта в логах
        print(report)
        
        # bot.sendMessage(chat_id=chat_id, text=report, parse_mode='Markdown')
        # for plot_object in plot_objects:
            # print(plot_object)
            #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
    # исполнение таска    
    app_report()
    
app_metrics_daily_report_run = app_metrics_daily_report()
