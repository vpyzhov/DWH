README: Создание датового склада (DWH) с использованием Apache Spark и PostgreSQL

Общее описание

Этот проект предназначен для реализации датового склада (DWH) для маркетплейса товаров ручного производства. Вводные данные из источников импортируются в PostgreSQL и обрабатываются с использованием Apache Spark для формирования витрины данных со статистикой по продажам.

Выполненные шаги

1. Запуск Docker контейнеров

PostgreSQL был запущен в Docker с помощью команды:

docker run --name my_postgres -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e POSTGRES_DB=dwh -p 5432:5432 -d postgres

Apache Spark Notebook запущен с помощью:

docker run -p 8888:8888 --name spark-notebook --network spark-postgres-network jupyter/all-spark-notebook

2. Создание структуры DWH

Скрипты для создания таблиц измерений (мастеров, продуктов) и фактов (заказов) запущены через DBeaver.

3. Импорт данных

Данные из источников (файлы CSV) загружены в таблицы источников (схема src).

4. Настройка сетевого взаимодействия

PostgreSQL и Spark взаимодействуют в сети spark-postgres-network.

5. Обработка в Apache Spark

5.1 Чтение данных из исходных таблиц

craftsmens_src_df = spark.read.jdbc(url=url, table="src.craftsmens", properties=properties)
products_src_df = spark.read.jdbc(url=url, table="src.products", properties=properties)
orders_src_df = spark.read.jdbc(url=url, table="src.orders", properties=properties)

5.2 Заполнение DWH (таблицы измерений и фактов)

Данные из исходных таблиц загружены в таблицы d_craftsmans, d_products и f_orders.

5.3 Формирование витрины данных

from pyspark.sql.functions import col, sum, count, max as spark_max, current_timestamp

vitryna_df = (
    f_orders_df
    .join(d_craftsmans_df, "craftsman_id")
    .join(d_products_df, "product_id")
    .groupBy(
        "craftsman_id",
        "craftsman_name",
        "product_type"
    )
    .agg(
        count("*").alias("total_orders"),
        sum("product_price").alias("total_sales"),
        spark_max("order_created_date").alias("last_sale_date")
    )
    .withColumnRenamed("product_type", "product_category")
    .withColumn("load_dttm", current_timestamp())
)

vitryna_df.write.jdbc(url=url, table="dwh.craftsman_report_datamart", mode="append", properties=properties)

5.4 Проверка витрины данных

result_df = spark.read.jdbc(url=url, table="dwh.craftsman_report_datamart", properties=properties)
result_df.show()

Используемые библиотеки

Apache Spark

PostgreSQL

Docker

Jupyter Notebook

Как запустить

Запустите Docker контейнеры с PostgreSQL и Spark.

Запустите Jupyter Notebook для обработки данных в Spark.

Запустите пошаговые ячейки кода из блокнота.
