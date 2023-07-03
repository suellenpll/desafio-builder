from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import config
import database_connect
import bronze_etl
import silver_etl
import gold_etl


with DAG(
    'minha_dag',
    start_date=datetime(2023, 6, 30),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='read_from_mysql_builders',
        python_callable=bronze_etl.read_from_mysql_builders
    )

    task2 = PythonOperator(
        task_id='write_to_bronze_mysql_covid',
        python_callable=bronze_etl.write_to_bronze,
        op_kwargs={'df': task1.output, 'table_name': 'bronze_mysql_covid'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task3 = PythonOperator(
        task_id='read_from_mongodb_builders',
        python_callable=bronze_etl.read_from_mongodb_builders
    )

    task4 = PythonOperator(
        task_id='write_to_bronze_mongodb_multas',
        python_callable=bronze_etl.write_to_bronze,
        op_kwargs={'df': task3.output, 'table_name': 'bronze_mongodb_multas'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task5 = PythonOperator(
        task_id='read_from_mysql_bronze_multas',
        python_callable=silver_etl.read_from_mysql_bronze,
        op_kwargs={'arquivo': './queries/silver_multas.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task6 = PythonOperator(
        task_id='write_to_silver_multas',
        python_callable=silver_etl.write_to_silver,
        op_kwargs={'df': task5.output, 'table_name': 'silver_multas'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task7 = PythonOperator(
        task_id='read_from_mysql_bronze_covid_state',
        python_callable=silver_etl.read_from_mysql_bronze,
        op_kwargs={'arquivo': './queries/silver_covid_state.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task8 = PythonOperator(
        task_id='write_to_silver_covid_state',
        python_callable=silver_etl.write_to_silver,
        op_kwargs={'df': task7.output, 'table_name': 'silver_covid_state'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task9 = PythonOperator(
        task_id='read_from_mysql_bronze_covid_city',
        python_callable=silver_etl.read_from_mysql_bronze,
        op_kwargs={'arquivo': './queries/silver_covid_city.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task10 = PythonOperator(
        task_id='write_to_silver_covid_city',
        python_callable=silver_etl.write_to_silver,
        op_kwargs={'df': task9.output, 'table_name': 'silver_covid_city'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task11 = PythonOperator(
        task_id='read_from_silver_to_dim_state',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_state.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task12 = PythonOperator(
        task_id='write_to_gold_dim_state',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task11.output, 'table_name': 'gold_dim_state'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task13 = PythonOperator(
        task_id='read_from_silver_to_dim_city',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_city.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task14 = PythonOperator(
        task_id='write_to_gold_dim_city',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task13.output, 'table_name': 'gold_dim_city'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task15 = PythonOperator(
        task_id='read_from_silver_to_dim_date',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_date.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task16 = PythonOperator(
        task_id='write_to_gold_dim_date',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task15.output, 'table_name': 'gold_dim_date'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task17 = PythonOperator(
        task_id='read_from_silver_to_dim_amparo_legal',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_amparo_legal.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task18 = PythonOperator(
        task_id='write_to_gold_dim_amparo_legal',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task17.output, 'table_name': 'gold_dim_amparo_legal'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task19 = PythonOperator(
        task_id='read_from_silver_to_dim_infracao',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_infracao.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task20 = PythonOperator(
        task_id='write_to_gold_dim_infracao',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task19.output, 'table_name': 'gold_dim_infracao'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task21 = PythonOperator(
        task_id='read_from_silver_to_dim_escopo_autuacao',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_escopo_autuacao.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task22 = PythonOperator(
        task_id='write_to_gold_dim_escopo_autuacao',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task21.output, 'table_name': 'gold_dim_escopo_autuacao'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task23 = PythonOperator(
        task_id='read_from_silver_to_fato_multas',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_fato_multas.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task24 = PythonOperator(
        task_id='write_to_gold_fato_multas',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task23.output, 'table_name': 'gold_fato_multas'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task25 = PythonOperator(
        task_id='read_from_silver_to_dim_city_ibge',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_dim_city_ibge.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task26 = PythonOperator(
        task_id='write_to_gold_dim_city_ibge',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task25.output, 'table_name': 'gold_dim_city_ibge'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task27 = PythonOperator(
        task_id='read_from_silver_to_fato_covid',
        python_callable=gold_etl.read_from_silver,
        op_kwargs={'arquivo': './queries/gold_fato_covid.sql'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task28 = PythonOperator(
        task_id='write_to_gold_fato_covid',
        python_callable=gold_etl.write_to_gold,
        op_kwargs={'df': task27.output, 'table_name': 'gold_fato_covid'},
        provide_context=True,
        trigger_rule='all_done'
    )

    task1 >> task2
    task3 >> task4
    task5 >> task6
    task7 >> task8
    task9 >> task10
    task11 >> task12
    task13 >> task14 
    task15 >> task16
    task17 >> task18
    task19 >> task20
    task21 >> task22
    task23 >> task24
    task25 >> task26
    task27 >> task28