from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import SparkConf
import argparse
import datetime 

class DataPipeline:
	
	def __init__(self):
		self.spark = SparkSession.builder.master("local[1]").appName("SearchProduct").getOrCreate()
		self.column_list = ["Product", "Keyword"]
		 
	def read_data(self):
		df = self.spark.read.csv(file, sep=r"\t", header=True).select("product_list", "referrer")
		return df

	def process(self, dataframe):
		
		df_split_product_list = dataframe.withColumn("Product_list", explode(split("product_list",",")))
		df_normal = df_split_product_list.withColumn("PricePerUnit", coalesce(split(df_split_product_list["product_list"], ";").getItem(3).cast("integer")/
			split(df_split_product_list["product_list"], ";").getItem(2).cast("integer"),lit(0)))\
		.withColumn("Item", concat(split(df_split_product_list["product_list"], ";").getItem(0),split(df_split_product_list["product_list"], ";").getItem(1)))\
		.withColumn("Product", regexp_extract("referrer", "(?<=[.]).*(?=[.])",0))\
		.withColumn("Units", split(df_split_product_list["product_list"], ";").getItem(2))\
		.withColumn("Keyword", regexp_extract("referrer", "(?<=[pq]=)(.*?)(?=[& ])",0))

		window_spec  = Window.partitionBy("Item")
		df_normal = df_normal.withColumn("PricePerUnitUpdate",max("PricePerUnit").over(window_spec)) # if revenue is updated based on same Item


		# df_revenue = df_normal.withColumn("Revenue", df_normal["PricePerUnit"].cast("double")*df_normal["Units"].cast("double"))

		df_revenue = df_normal.withColumn("Revenue", when(df_normal["PricePerUnit"] == "0.0", df_normal["PricePerUnitUpdate"].cast("double")*df_normal["Units"].cast("double"))\
			.otherwise(df_normal["PricePerUnit"].cast("double")*df_normal["Units"].cast("double")))


		window_product_keyword  = Window.partitionBy([col(x) for x in self.column_list])
		window_spec_product_keyword  = window_product_keyword.orderBy(desc("Revenue"))
		window_spec_product  = Window.partitionBy(self.column_list[0])
		window_spec_final_product_desc  = Window.partitionBy(self.column_list[0]).orderBy(desc("Revenue"))
		df_final = df_revenue.withColumn("RankProductKeyword",rank().over(window_spec_product_keyword)) \
							.withColumn("RankProduct",rank().over(window_spec_final_product_desc)) \
							.withColumn("RevenueProduct",sum("Revenue").over(window_spec_product)) \
							.withColumn("RevenueProductKeyword",sum("Revenue").over(window_product_keyword))

		return df_final

	def write(self, dataframe):
		time = datetime.datetime.now()
		df_1 = dataframe.select(self.column_list[0], self.column_list[1], "RevenueProduct").filter(dataframe["RankProductKeyword"]==1).filter(dataframe[self.column_list[0]]!="esshopzilla").orderBy(desc("RevenueProductKeyword"))
		df_2 = dataframe.select(self.column_list[0], self.column_list[1], "RevenueProductKeyword").filter(dataframe["RankProduct"]==1).filter(dataframe[self.column_list[0]]!="esshopzilla").orderBy(desc("RevenueProductKeyword"))
		df_3 = dataframe.select(self.column_list[0], self.column_list[1], "RevenueProductKeyword").filter(dataframe["RankProductKeyword"]==1).filter(dataframe[self.column_list[0]]!="esshopzilla").orderBy(desc("RevenueProductKeyword"))
		df_4 = dataframe.select(self.column_list[0], self.column_list[1], "RevenueProduct").filter(dataframe["RankProduct"]==1).filter(dataframe[self.column_list[0]]!="esshopzilla").orderBy(desc("RevenueProductKeyword"))

		df_1 = df_1.withColumnRenamed("Product","Search Engine Domain") \
    			.withColumnRenamed("Keyword","Search Engine Keyword") \
    			.withColumnRenamed("Revenue","Revenue") 
		df_1.write.option("delimiter", "|").option("header", True).csv("/home/adobe/data/output/")

# entry point for PySpark ETL application
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Process data file")
	parser.add_argument("-f", "--filename", type=str, required=True)
	args = parser.parse_args()
	file = args.filename
	data_pipeline = DataPipeline()
	dataframe = data_pipeline.read_data()
	dataframe_extract = data_pipeline.process(dataframe)
	dd = data_pipeline.write(dataframe_extract)