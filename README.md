# Apache-Airflow-projects

<h3>Построение <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">ETL-пайплайна</a><h3>

Задача: Создать DAG в Airflow с ETL-пайплайном, который будет считаться каждый день за вчера. Требования к пайплайну:
  1. Необходимо параллельно обрабатывать две таблицы. В feed_actions для каждого юзера считать: число просмотров и лайков контента. В message_actions для каждого юзера считать: количество отправленных сообщений, количество отправленных   сообщений, скольким людям он пишет, сколько людей пишет ему. Каждую выгрузку необходимо делать отдельным таском.
  2. Объединить две полученные таблицы в одну.
  3. Для получившейся общей таблицы необходимо посчитать все эти метрики в разрезе по полу, возрасту и ос. Рассчёт метрик по каждому срезу должен быть в отдельном таске.
  4. Финальные данные со всеми метриками записывать в отдельную таблицу в ClickHouse.
  5. Каждый день таблица должна наполняться новыми данными


<h3>Автоматизированный <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">отчёт по ленте</a> 



<h3>Автоматизированный <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">отчёт по всему приложению</a> 



<h3>Релизация <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/alerts_system.py" target="_blank">системы алертов</a> 
