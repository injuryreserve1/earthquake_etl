## Проект: Пайплайн обработки данных о землетрясениях (Airflow + DuckDB + PostgreSQL)

## Данный проект представляет собой ETL/ELT-пайплайн для регулярного сбора, сохранения и агрегации данных о землетрясениях с использованием Apache Airflow, DuckDB, S3 (MinIO) и PostgreSQL (DWH).

## Архитектура данных

Пайплайн состоит из 4 связанных между собой DAG, которые последовательно проводят данные через слои RAW, ODS и DM (Data Marts).

[API USGS]
│
▼ (DAG: raw_from_api_to_s3)
[MinIO S3 (слой RAW)]
│
▼ (DAG: raw_from_s3_to_pg)
[PostgreSQL: ods.fct_earthquake (слой ODS)]
│
├──► (DAG: fct_avg_day_earthquake) ──► [PostgreSQL: dm.fct_avg_day_earthquake (слой DM)]
│
└──► (DAG: fct_count_day_earthquake) ─► [PostgreSQL: dm.fct_count_day_earthquake (слой DM)]

## Описание слоев:

1.  RAW (S3 / MinIO): Сырые данные в формате Parquet, выгруженные из внешнего API по дням.
2.  ODS (Operational Data Store): Детализированные данные, загруженные в PostgreSQL с приведением типов и названий колонок к snake_case.
3.  DM (Data Marts): Агрегированные витрины данных для аналитики (расчет среднего уровня магнитуды и количества землетрясений за день).

---

## Описание DAG

Все DAG настроены на ежедневное выполнение (schedule_interval="0 5 \* \* \*" в 05:00 по Москве) с включенным режимом catchup=True для исторической загрузки данных.

## 1. raw_from_api_to_s3

- Назначение: Сбор данных из API сейсмологической службы USGS и сохранение в S3.
- Технологии: PythonOperator, DuckDB (модуль httpfs).
- Логика: DuckDB скачивает CSV напрямую по ссылке за интервал data_interval_start — data_interval_end, конвертирует в Parquet со сжатием и загружает в бакет s3://prod/raw/earthquake/....

## 2. raw_from_s3_to_pg

- Назначение: Перенос данных из S3 в реляционную БД.
- Технологии: ExternalTaskSensor, PythonOperator, DuckDB (модули httpfs и postgres).
- Логика: Ждет успешного завершения raw_from_api_to_s3. С помощью DuckDB подключается к PostgreSQL, читает Parquet из S3, маппит колонки (например, magType -> mag_type) и выполняет INSERT INTO ods.fct_earthquake.

## 3. fct_avg_day_earthquake

- Назначение: Расчет средней магнитуды землетрясений за день.
- Технологии: ExternalTaskSensor, SQLExecuteQueryOperator.
- Логика: Ждет завершения слоя ODS. Использует идемпотентную схему загрузки через stg-таблицу:

1. Удаляет старую tmp-таблицу за текущую дату, если она осталась. 2. Создает tmp-таблицу и рассчитывает avg(mag::float) из ods.fct_earthquake за конкретный день. 3. Удаляет из целевой витрины dm.fct_avg_day_earthquake данные за этот день (защита от дублей). 4. Вставляет рассчитанные данные из tmp в целевую витрину. 5. Очищает за собой tmp-таблицу.

## 4. fct_count_day_earthquake

- Назначение: Расчет общего количества землетрясений за день.
- Технологии: ExternalTaskSensor, SQLExecuteQueryOperator.
- Логика: Полностью идентична DAG fct_avg_day_earthquake, но в качестве агрегации использует функцию count(\*) и сохраняет результат в витрину dm.fct_count_day_earthquake.

---

## Конфигурация и переменные Airflow

Для работы пайплайнов в Airflow должны быть настроены следующие сущности:

## Variables (Переменные)

- access_key — Ключ доступа к S3 (MinIO).
- secret_key — Секретный ключ к S3 (MinIO).
- pg_password — Пароль пользователя postgres для подключения DuckDB к PostgreSQL.

## Connections (Соединения)

- postgres_dwh — Подключение типа Postgres к базе данных DWH. Используется операторами SQLExecuteQueryOperator.

---

## Требования к инфраструктуре и окружению

Для успешного выполнения тасок на воркерах Airflow должны быть установлены следующие зависимости:

- Python-библиотеки: pendulum, duckdb, apache-airflow, apache-airflow-providers-common-sql.
- Доступность сервисов:
- Сетевой доступ к https://earthquake.usgs.gov
  - Доступ к S3-хранилищу по адресу minio:9000 (бакет prod)
  - Доступ к PostgreSQL по адресу postgres_dwh:5432
