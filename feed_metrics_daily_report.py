from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
import telegram
import matplotlib.pyplot as plt
import io
import numpy as np
import seaborn as sns

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
    'start_date': datetime(2026, 6, 1)
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, dag_id='feed_metrics_daily_report')
def feed_metrics_daily_report():
    
    def create_dashboard(df):
        # создаём фигуру с несколькими субплотами
        fig = plt.figure(figsize=(15, 12))
        fig.suptitle('Дашборд метрик ленты за последние 7 дней', fontsize=20, fontweight='bold', y=0.98)
    
        # цветовая схема
        colors = {
        'dau': '#a8a8a7',
        'likes': '#828281',
        'views': '#a8a8a7',
        'ctr': '#a8a8a7'
        }
    
        # график DAU (активные пользователи)
        ax1 = plt.subplot(2, 2, 1)
        ax1.plot(df['prev_report_day'], df['dau'], 
        color=colors['dau'], label='DAU')
        ax1.set_title('DAU', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Дата', fontsize=10)
        ax1.set_ylabel('Количество пользователей', fontsize=10)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        # максимальное и минимальное значение
        max_dau = df['dau'].max()
        min_dau = df['dau'].min()
        max_date = df.loc[df['dau'].idxmax(), 'prev_report_day']
        min_date = df.loc[df['dau'].idxmin(), 'prev_report_day']
        # отмечаем максимум красной точкой с подписью
        ax1.text(max_date, max_dau, f'{max_dau/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
        # отмечаем минимум зелёной точкой с подписью
        ax1.text(min_date, min_dau, f'{min_dau/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
    
        # график CTR
        ax2 = plt.subplot(2, 2, 2)
        bars = ax2.bar(df['prev_report_day'], df['ctr'], width=0.6, color=colors['ctr'], alpha=0.8)
        ax2.set_title('CTR', fontsize=14, fontweight='bold', pad=15)
        ax2.set_xlabel('Дата', fontsize=10, labelpad=10)
        ax2.set_ylabel('CTR (%)', fontsize=10, labelpad=10)
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.tick_params(axis='x', rotation=45)
        ax2.set_yticklabels([f'{i}%' for i in range(0, 100, 5)])
        # подписи над столбцами
        for bar, val in zip(bars, df['ctr']):
            height = bar.get_height()
            ax2.annotate(f'{val:.2f}%', 
            xy=(bar.get_x() + bar.get_width()/2, height),
            xytext=(0, 5), 
            textcoords="offset points", 
            ha='center', 
            va='bottom', 
            fontsize=9)

        # график лайков
        ax3 = plt.subplot(2, 2, 3)
        ax3.plot(df['prev_report_day'], df['likes'], marker='^', linewidth=2, markersize=8,
        color=colors['likes'], label='Лайки')
        ax3.set_title('Лайки', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Дата', fontsize=10)
        ax3.set_ylabel('Количество лайков', fontsize=10)
        ax3.legend(loc='upper left')
        ax3.grid(True, alpha=0.3)
        ax3.tick_params(axis='x', rotation=45)
        # максимальное и минимальное значение
        max_likes = df['likes'].max()
        min_likes = df['likes'].min()
        max_date = df.loc[df['likes'].idxmax(), 'prev_report_day']
        min_date = df.loc[df['likes'].idxmin(), 'prev_report_day']
        # отмечаем максимум красной точкой с подписью
        ax3.text(max_date, max_likes, f'{max_likes/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
        # отмечаем минимум зелёной точкой с подписью
        ax3.text(min_date, min_likes, f'{min_likes/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
    
        # график просмотров
        ax4 = plt.subplot(2, 2, 4)
        ax4.plot(df['prev_report_day'], df['views'], marker='s', linewidth=2, markersize=8,
        color=colors['views'], label='Просмотры')
        ax4.set_title('Просмотры', fontsize=14, fontweight='bold')
        ax4.set_xlabel('Дата', fontsize=10)
        ax4.set_ylabel('Количество просмотров', fontsize=10)
        ax4.legend(loc='upper left')
        ax4.grid(True, alpha=0.3)
        ax4.tick_params(axis='x', rotation=45)
        # максимальное и минимальное значение
        max_views = df['views'].max()
        min_views = df['views'].min()
        max_date = df.loc[df['views'].idxmax(), 'prev_report_day']
        min_date = df.loc[df['views'].idxmin(), 'prev_report_day']
        # отмечаем максимум красной точкой с подписью
        ax4.text(max_date, max_views, f'{max_views/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
        # отмечаем минимум зелёной точкой с подписью
        ax4.text(min_date, min_views, f'{min_views/1000:.1f}K', 
        fontsize=9, fontweight='bold', color='#363636', va='bottom')
        
        plt.tight_layout()
        return fig


    @task()
    def feed_report (chat=None):
        #chat_id = chat or -1002614297220

        #my_token = '8706906706:AAG_S7YVq3gFFhzK2-FjGjXbSoEVlRnbxsQ' # токен моего созданного бота
        #bot = telegram.Bot(token=my_token) # получаю доступ к боту

        # значения ключевых метрик ленты за предыдущий день
        query = '''
                SELECT 
        
                toDate(time) as prev_report_day,
                count(DISTINCT user_id) as dau,
                sum(action='like') as likes,
                sum(action='view') as views,
                likes / views as ctr
        
                FROM simulator_20260420.feed_actions
                WHERE toDate(time) = today() - 1
                GROUP BY prev_report_day
                '''

        prev_day_metrics = ph.read_clickhouse(query, connection=connection)

        # Извлекаем значения метрик

        report_date = prev_day_metrics['prev_report_day'].dt.date.iloc[0]
        dau = int(prev_day_metrics['dau'].iloc[0])
        likes = int(prev_day_metrics['likes'].iloc[0])
        views = int(prev_day_metrics['views'].iloc[0])
        ctr = round(prev_day_metrics['ctr'].iloc[0] * 100, 2)  # в процентах
    
        # Формируем текст отчёта
        report_text = f"""
            📊 **Отчёт по метрикам ленты за {report_date}**

            DAU: {dau:,} 
            Лайки: {likes:,}
            Просмотры: {views:,}
            CTR: {ctr}%
        """            
        msg = report_text
        #bot.sendMessage(chat_id=chat_id, text=report_text, parse_mode='Markdown')
        print(msg)

         # значения ключевых метрик ленты за предыдущие 7 дней
        query = '''
                SELECT 
        
                toDate(time) as prev_report_day,
                count(DISTINCT user_id) as dau,
                sum(action='like') as likes,
                sum(action='view') as views,
                likes / views as ctr
        
                FROM simulator_20260420.feed_actions
                WHERE toDate(time) BETWEEN today() - 7 AND today() - 1
                GROUP BY prev_report_day
                '''

        prev_week_metrics = ph.read_clickhouse(query, connection=connection)
        # преобразуем датафрейм для работы над дашбордом
        prev_week_metrics['prev_report_day'] = pd.to_datetime(prev_week_metrics['prev_report_day'])
        prev_week_metrics['ctr'] = round(prev_week_metrics['ctr'] * 100, 2)

        # создаём изображение и сохраняем в переменную
        fig = create_dashboard(prev_week_metrics)

        # отправка изображения
        plot_object = io.BytesIO()
        fig.savefig(plot_object, format='png', dpi=150, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'dashbord_report.png'
        plt.close()
        #bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # исполнение таска
    feed_report()
    
feed_metrics_daily_report_run = feed_metrics_daily_report()