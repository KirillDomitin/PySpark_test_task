from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

DATA_PATH = "data_frames/"

# датафреймы
products_df = spark.read.option("header", "true").csv(DATA_PATH + "products.csv")
categories_df = spark.read.option("header", "true").csv(DATA_PATH + "categories.csv")
product_category_df = spark.read.option("header", "true").csv(DATA_PATH + "product_category.csv")

products_df = products_df.withColumn("product_id", col("product_id").cast("int"))
categories_df = categories_df.withColumn("category_id", col("category_id").cast("int"))
product_category_df = product_category_df.withColumn("product_id", col("product_id").cast("int")) \
    .withColumn("category_id", col("category_id").cast("int"))


def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame,
                               product_category_df: DataFrame) -> DataFrame:
    """
    Напишите метод на PySpark, который в одном датафрейме вернет все пары
    «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.
    """

    # Соединяем продукты со связями
    product_with_category = products_df.join(
        product_category_df,
        on="product_id",
        how="left"
    )

    # Соединяем с категориями
    full_info = product_with_category.join(
        categories_df,
        on="category_id",
        how="left"
    )

    # Оставляем только имя продукта и имя категории
    result = full_info.select("product_name", "category_name")

    return result


if __name__ == '__main__':
    result_df = get_product_category_pairs(products_df, categories_df, product_category_df)
