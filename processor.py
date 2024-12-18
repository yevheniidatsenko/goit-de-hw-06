import datetime
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
import os

# Налаштування для роботи з Kafka через PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Конфігурація для підключення до Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Створення сесії Spark для обробки даних
spark = (
    SparkSession.builder.appName("KafkaStreaming")
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "200")
    .config("spark.sql.columnNameLengthThreshold", "200")
    .getOrCreate()
)

# Завантаження CSV-файлу з умовами для алертів (передумови для порогів температури та вологості)
alerts_df = spark.read.csv("./data/alerts_conditions.csv", header=True)

# Параметри вікна для обчислень за часом
window_duration = "1 minute"
sliding_interval = "30 seconds"

# Підключення до Kafka та читання потоку даних
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "building_sensors_greenmoon")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "300")
    .load()
)

# Опис структури JSON для декодування вхідних даних
json_schema = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
    ]
)

# Обробка потоку: декодування JSON та обчислення середніх значень температури та вологості
avg_stats = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"),
    )
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), window_duration, sliding_interval))
    .agg(
        avg("value_json.temperature").alias("t_avg"),
        avg("value_json.humidity").alias("h_avg"),
    )
    .drop("topic")
)


all_alerts = avg_stats.crossJoin(alerts_df)

# Фільтрація для обробки тільки валідних алертів за температурою та вологістю
valid_alerts = (
    all_alerts.where("t_avg > temperature_min AND t_avg < temperature_max")
    .unionAll(all_alerts.where("h_avg > humidity_min AND h_avg < humidity_max"))
    .withColumn("timestamp", lit(str(datetime.datetime.now())))
    .drop("id", "humidity_min", "humidity_max", "temperature_min", "temperature_max")
)

# Створення унікального ключа для кожного запису
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Підготовка до відправки даних в Kafka з перетворенням у формат JSON
prepare_to_kafka_df = valid_alerts.withColumn("key", uuid_udf()).select(
    col("key"),
    to_json(
        struct(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("timestamp"),
        )
    ).alias(
        "value"
    ),  # Оформлення значення у формат JSON
)

# Оновлений метод виведення в консоль:
displaying_df = (
    valid_alerts.writeStream.trigger(processingTime="10 seconds")
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 10)
    .start()
    .awaitTermination()
)