from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr


def initialize_spark(app_name="CourseraCoursesAnalysis"):
    """
    Initialize and return a Spark session
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def load_data(spark, file_path):
    """
    Load the data into a Spark DataFrame
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.drop("_c0")
    df = df.drop("course_skills")
    return df


def clean_data(df):
    """
    Clean the data by transforming and dropping unnecessary columns
    """
    # Transform 'course_students_enrolled' to numeric type
    df = df.withColumn("course_students_enrolled", when(col("course_students_enrolled").contains("M"),
                                                        expr(
                                                            "substring(course_students_enrolled, 1, length(course_students_enrolled)-1)").cast(
                                                            "double") * 1000000)
                       .when(col("course_students_enrolled").contains("k"),
                             expr("substring(course_students_enrolled, 1, length(course_students_enrolled)-1)").cast(
                                 "double") * 1000)
                       .otherwise(col("course_students_enrolled").cast("double")))

    # Clean 'course_time' and standardize it to months
    df = df.withColumn("min_months_to_complete",
                       expr("substring(course_time, 1, instr(course_time, '-')-1)").cast("int"))
    df = df.withColumn("max_months_to_complete", expr(
        "substring(course_time, instr(course_time, '-')+1, length(course_time) - instr(course_time, 'Months') +1)").cast(
        "int"))

    # Handle missing values for 'course_rating' and 'course_review_num'
    df = df.withColumn("course_rating", col("course_rating").cast("double"))
    df = df.withColumn("course_reviews_num", col("course_reviews_num").cast("integer"))

    # Drop any rows where 'course_title' or 'course_organization' is null as they are essential
    df = df.dropna(subset=["course_title", "course_organization"])

    return df


def execute_sql_query(spark, df, query):
    """
    Execute a SQL query on the DataFrame
    """
    df.createOrReplaceTempView("courses")
    return spark.sql(query)


def transform_data(df):
    """
    Perform some data transformation
    """
    # Example transformation: count the number of courses per difficulty level
    difficulty_counts = df.groupBy("course_difficulty").count()
    return difficulty_counts
