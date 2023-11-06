import unittest
from pyspark.sql import SparkSession
from lib import initialize_spark, load_data, clean_data, execute_sql_query, transform_data


class PySparkTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Start a Spark session to use for testing.
        """
        cls.spark = SparkSession.builder.master("local[2]").appName("CourseraCoursesAnalysisUnitTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Stop the Spark session after all tests are run.
        """
        cls.spark.stop()

    def test_initialize_spark(self):
        """
        Test the Spark Session initialization.
        """
        spark = initialize_spark()
        self.assertIsNotNone(spark)

    def test_load_data(self):
        """
        Test loading data into a Spark DataFrame.
        """
        # Create a temporary CSV file to load
        data = [("Introduction to Spark", "University X", 4.5, 100),
                ("Advanced Spark Programming", "University Y", None, None)]
        df = self.spark.createDataFrame(data,
                                        ["course_title", "course_organization", "course_rating", "course_review_num"])
        temp_file_path = "temp_data.csv"
        df.coalesce(1).write.csv(temp_file_path, header=True, mode='overwrite')

        # Load the data
        loaded_df = load_data(self.spark, temp_file_path)
        self.assertEqual(loaded_df.count(), 2)


    def test_execute_sql_query(self):
        """
        Test the execution of an SQL query.
        """
        data = [("Spark Basics", "University X", 4.5),
                ("Advanced Spark", "University Y", 4.8)]
        df = self.spark.createDataFrame(data, ["course_title", "course_organization", "course_rating"])

        # Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("courses")

        # Define a SQL query
        query = "SELECT course_title FROM courses WHERE course_rating > 4.7"

        # Execute the query
        result_df = execute_sql_query(self.spark, df, query)
        self.assertEqual(result_df.count(), 1)
        self.assertEqual(result_df.collect()[0]['course_title'], "Advanced Spark")


if __name__ == '__main__':
    unittest.main()
