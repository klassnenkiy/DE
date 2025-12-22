# hw_2
как поднять: cd simple-etl && docker compose up -d

как прогнать dbt: docker exec -it airflow-bank-simple bash -lc "cd /opt/airflow/dbt && dbt run --select cdm_fraud_cases dm_fraud_daily"

ссылка: http://localhost:3000

SQL для вопроса

настройки графика: Bar / X=day / Y=fraud_case_cnt / Breakout=fraud_type / Stacked=ON