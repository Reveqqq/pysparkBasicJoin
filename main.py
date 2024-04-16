import os

os.system("chcp 65001 > nul")  # change console encoding to UTF-8
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(products: DataFrame, categories: DataFrame,
                               product_categories: DataFrame) -> (DataFrame, DataFrame):
    products_with_categories = products.join(product_categories, ["product_id"], how="left") \
        .join(categories, ["category_id"], how="left") \
        .select(col("product_name"), col("category_name"))

    products_without_categories = products_with_categories.filter(col("category_id").isNull()) \
        .select(col("product_name"))

    products_with_categories = products_with_categories.filter(col("category_id").isNotNull())

    return products_with_categories, products_without_categories


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductsAndCategories") \
        .getOrCreate()

    products_data = [
        (1, 'product1'),
        (2, 'product2'),
        (3, 'product3'),
        (4, 'product4'),
        (5, 'product5')
    ]

    product_schema = StructType([
        StructField('product_id', IntegerType(), False),
        StructField('product_name', StringType(), True)
    ])

    categories_data = [
        (1, 'category1'),
        (2, 'category2')
    ]

    category_schema = StructType([
        StructField('category_id', IntegerType(), False),
        StructField('category_name', StringType(), True)
    ])

    product_category_data = [
        (1, 1),
        (1, 2),
        (2, 2),
        (3, 1),
        (5, 1)
    ]

    product_category_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True)
    ])

    products_df = spark.createDataFrame(products_data, schema=product_schema)
    categories_df = spark.createDataFrame(categories_data, schema=category_schema)
    product_category_df = spark.createDataFrame(product_category_data, schema=product_category_schema)

    prod_categories, prod_without_categories = \
        get_product_category_pairs(products_df, categories_df, product_category_df)

    print('Products with Categories:')
    prod_categories.show()

    print('Products with null category:')
    prod_without_categories.show()

    spark.stop()
