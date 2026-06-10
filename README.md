# Apache-Airflow-projects

<h3>Построение <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">ETL-пайплайна</a></h3>

<p dir="auto"><bold>Задача:</bold> Создать DAG в Airflow с ETL-пайплайном, который будет считаться каждый день за вчера. Требования к пайплайну:</p>
  <ol>
    <li>Необходимо параллельно обрабатывать две таблицы. В feed_actions для каждого юзера считать: число просмотров и лайков контента. В message_actions для каждого юзера считать: количество отправленных сообщений, количество                   отправленных   сообщений, скольким людям он пишет, сколько людей пишет ему. Каждую выгрузку необходимо делать отдельным таском.</li>
    <li>Объединить две полученные таблицы в одну.</li>
    <li>Для получившейся общей таблицы необходимо посчитать все эти метрики в разрезе по полу, возрасту и ос. Рассчёт метрик по каждому срезу должен быть в отдельном таске.</li>
    <li>Финальные данные со всеми метриками записывать в отдельную таблицу в ClickHouse.</li>
    <li>Каждый день таблица должна наполняться новыми данными</li>
  </ol>
<p dir="auto"><bold>Стек:</bold> ClickHouse, Python (pandas, pandahouse, airflow.decorators), Redash, Apache Airflow, Git</p>
<a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">Код решения задачи</a>
<br>
<br>
<details open>
 <summary>
   <b>
     Демонстрация результата работы DAG с ETL-пайплайном (Airflow + Redash)
   </b>
 </summary>
  <p dir="auto">
    <a target="_blank" rel="noopener noreferrer" href="/pictures/dags_list.png"><img src="/pictures/dags_list.png" align="absmiddle" width="400" style="max-width: 100%;"></a> <code>Список запущенных дагов</code> <br>
  </p>
</details>

<h3>Автоматизированный <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">отчёт по ленте</a></h3>



<h3>Автоматизированный <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/etl_task_report.py" target="_blank">отчёт по всему приложению</a></h3>



<h3>Релизация <a href="https://github.com/daria-aladina/Apache-Airflow-projects/blob/main/alerts_system.py" target="_blank">системы алертов</a></h3>
