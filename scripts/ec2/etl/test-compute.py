import unittest
from pyspark.sql.functions import *
from compute import DataPipeline
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
            .builder
            .master("local[*]")
            .appName("PySpark-unit-test")
            .config('spark.port.maxRetries', 30)
            .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def tests_etl(self):
        data_pipeline = DataPipeline()
        input_schema = StructType([
                StructField('product_list', StringType(), True),
                StructField('referrer', StringType(), True)
            ])
        input_data = [(["Electronics;Zune - 328GB;3;190;,Electronics;Zune - 328GB;1;430;"], "http://www.google.com/search?q=Zune&go=&form=QBLH&qs=n"),
                    (["Electronics;Zavia - 328GB;3;140;,Electronics;Zavia - 328GB;1;330;"],"http://www.google.com/search?q=Zavia&go=&form=QBLH&qs=n")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        input_final_df = data_pipeline.process(input_df)
        input_final_df.show(truncate=False)

        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, input_final_df.schema.fields)]

        self.assertEqual(len(fields1),13)

        self.assertEqual(input_final_df.count(), 4)

if __name__ == '__main__':
    unittest.main()