import os
os.environ['SPARK_HOME'] = '/opt/homebrew/Cellar/apache-spark/3.5.4/libexec'
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@17/17.0.13/libexec/openjdk.jdk/Contents/Home"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

users = spark.read.csv('data/users.csv', header=True).dropna()
purchases = spark.read.csv('data/purchases.csv', header=True).dropna()
products = spark.read.csv('data/products.csv', header=True).dropna()

joined_table = users.join(purchases, users.user_id == purchases.user_id, 'inner') \
    .drop(purchases.user_id) \
    .join(products, purchases.product_id == products.product_id, 'inner') \
    .drop(purchases.product_id) \
    .withColumn("total_price", col("price") * col("quantity"))

joined_table.show(5)

joined_table.groupby("category") \
    .sum("total_price") \
    .withColumnRenamed("sum(total_price)", "total_price_per_category").show()

joined_table.filter((joined_table.age >= 18) & (joined_table.age <= 25)) \
    .groupby("category").sum("total_price") \
    .withColumnRenamed("sum(total_price)", "total_price_per_category_for_age_18-25_inclusively").show()

total_expenses_per_user = joined_table.filter((joined_table.age >= 18) & (joined_table.age <= 25)) \
    .groupby("user_id") \
    .sum("total_price") \
    .withColumnRenamed("sum(total_price)", "total_expenses")

total_expenses_per_user_per_category = joined_table.filter((joined_table.age >= 18) & (joined_table.age <= 25)) \
    .groupby("user_id", "category") \
    .sum("total_price") \
    .withColumnRenamed("sum(total_price)", "total_expenses_per_category") \
    .sort("user_id", "category")

expenses_structure = total_expenses_per_user_per_category.join(total_expenses_per_user, "user_id", "inner") \
    .withColumn("percentage_of_expenses", col("total_expenses_per_category") / col("total_expenses") * 100)

expenses_structure.show()

expenses_structure.groupby("category").avg("percentage_of_expenses") \
    .withColumnRenamed("avg(percentage_of_expenses)", "avg_percentage_of_expenses") \
    .sort("avg_percentage_of_expenses", ascending=False).show(3)
