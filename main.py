from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Створюємо сесію Spark
spark = SparkSession.builder.appName("goit-de-hw-03").getOrCreate()

# Завантажуємо датасет
users_df = spark.read.csv('./users.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)

# Виводимо перші 10 записів
users_df.show(10)
purchases_df.show(10)
products_df.show(10)

# Очищення данних

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Обчислення загальної суми покупок за кожною категорією

purchases_products_df = purchases_df.join(products_df, on="product_id", how="inner")

purchases_products_df = purchases_products_df.withColumn(
    "total_price", col("quantity") * col("price")
)

category_totals_df = purchases_products_df.groupBy("category").agg(
    spark_sum("total_price").alias("total_sales")
)

category_totals_df.show()

