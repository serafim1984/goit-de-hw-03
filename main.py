from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

# Створюємо сесію Spark
spark = SparkSession.builder.appName("goit-de-hw-03").getOrCreate()

# 1. Завантажуємо датасет
users_df = spark.read.csv('./users.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)

# Виводимо перші 10 записів
users_df.show(10)
purchases_df.show(10)
products_df.show(10)

# 2. Очищення данних

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# 3. Обчислення загальної суми покупок за кожною категорією

purchases_products_df = purchases_df.join(products_df, on="product_id", how="inner")

purchases_products_df = purchases_products_df.withColumn(
    "total_price", col("quantity") * col("price")
)

category_totals_df = purchases_products_df.groupBy("category").agg(
    spark_sum("total_price").alias("total_sales")
)

category_totals_df.show()

# 4. Фільтрація користувачів за віковою категорією (від 18 до 25 включно)
filtered_users_df = users_df.filter((col("age") >= 18) & (col("age") <= 25))

# Об'єднання таблиць
# З'єднуємо purchases_df з filtered_users_df за user_id
filtered_purchases_df = purchases_df.join(filtered_users_df, on="user_id", how="inner")

# З'єднуємо результат з products_df за product_id
filtered_purchases_products_df = filtered_purchases_df.join(products_df, on="product_id", how="inner")

# Обчислення загальної суми покупок для вікової групи 18–25
filtered_purchases_products_df = filtered_purchases_products_df.withColumn(
    "total_price", col("quantity") * col("price")
)

# Групування за категорією продуктів та обчислення загальної суми
category_totals_age_group_df = filtered_purchases_products_df.groupBy("category").agg(
    spark_sum("total_price").alias("total_sales")
)

# Виведення результатів
category_totals_age_group_df.show()

# 5. Обчислення загальної суми витрат у віковій категорії
total_sales_age_group = category_totals_age_group_df.agg(
    spark_sum("total_sales").alias("grand_total")
).collect()[0]["grand_total"]

# Додавання частки покупок для кожної категорії
category_with_percentage_df = category_totals_age_group_df.withColumn(
    "percentage_of_total",
    spark_round((col("total_sales") / total_sales_age_group) * 100, 2)
)

# Виведення результатів
category_with_percentage_df.show()

# 6. Вибір 3 категорій із найвищим відсотком витрат
top_3_categories_df = category_with_percentage_df.orderBy(
    col("percentage_of_total").desc()
).limit(3)

# Виведення результатів
top_3_categories_df.show()