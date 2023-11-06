from lib import initialize_spark, load_data, clean_data, execute_sql_query, transform_data


def get_top_courses(spark, df_clean, query):
    """
    Execute Spark SQL query to get the top-rated courses.
    """
    return execute_sql_query(spark, df_clean, query)


def get_course_difficulty_distribution(df_clean):
    """
    Get the course difficulty distribution.
    """
    return transform_data(df_clean)


def show_df(df, title):
    """
    Print out a DataFrame with a title.
    """
    print(f"\n## {title}:")
    df.show()


def main():
    """
    Main function to run the data processing tasks.
    """
    spark = initialize_spark()

    # Load and clean data
    dataset_path = 'coursera_courses.csv'
    df = load_data(spark, dataset_path)
    df_clean = clean_data(df)

    # Show the cleaned DataFrame
    show_df(df_clean, "Cleaned Data")

    # Get top rated courses
    top_courses_query = """
    SELECT course_title, course_organization, course_rating
    FROM courses
    WHERE course_rating IS NOT NULL
    ORDER BY course_rating DESC
    LIMIT 10
    """
    top_courses = get_top_courses(spark, df_clean, top_courses_query)
    show_df(top_courses, "Top Rated Courses")

    # Get course difficulty distribution
    difficulty_counts_df = get_course_difficulty_distribution(df_clean)
    show_df(difficulty_counts_df, "Course Difficulty Distribution by 'Beginner', 'Intermediate', 'Advanced', 'Mixed'")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
