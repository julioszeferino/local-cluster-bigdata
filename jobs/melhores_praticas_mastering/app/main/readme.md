# Modular Design and Separation of Concerns [Yelp ETL Job]

This repository contains a Spark ETL job that processes the Yelp dataset. The project is structured to adhere to the 
principles of Modular Design and Separation of Concerns, ensuring a clean, maintainable, and scalable codebase.

## Project Structure
```shell
main/
├── config
│ └── config.json
├── data
│ ├── init.py
│ ├── ingestion.py
│ ├── generic_transformation.py
│ ├── business_transformation.py
│ ├── generic_output.py
│ ├── business_output.py
├── etl-yelp-batch.py
├── utils
│ ├── init.py
│ └── helpers.py
└── tests
├── init.py
├── test_data_ingestion.py
├── test_generic_transformation.py
├── test_business_transformation.py
├── test_generic_output.py
├── test_business_output.py
└── test_helpers.py
```

## Script Descriptions

### `main.py`
The entry point for the ETL job. It coordinates the entire ETL process:
- Reads configuration.
- Creates a Spark session.
- Executes data ingestion, transformation, and output steps.
- Logs the progress and handles exceptions.

### `config/config.json`
Contains configuration settings for the ETL job, such as file paths and output locations.

### `data/ingestion.py`
Handles the data ingestion process:
- `read_data(spark, file_path, file_format='parquet')`: Generic function to read data from various formats.

### `data/generic_transformation.py`
Contains generic transformation functions:
- `filter_data(df, condition)`: Filters DataFrame based on a condition.
- `calculate_aggregate(df, group_by_col, agg_col, agg_func, alias)`: Calculates aggregate values for each group in the DataFrame.
- `join_data(df1, df2, join_col1, join_col2, how='inner')`: Joins two DataFrames based on specified columns.

### `data/business_transformation.py`
Contains business-specific transformation functions that utilize generic transformations:
- `filter_active_businesses(business_df)`: Filters active businesses.
- `calculate_average_rating(review_df)`: Calculates average rating for each business.
- `join_business_review_data(business_df, review_df)`: Joins business and review data.

### `data/generic_output.py`
Contains generic output functions:
- `write_data(df, output_path, format_data='parquet')`: Writes DataFrame to the specified output path in the specified format.

### `data/business_output.py`
Contains business-specific output functions that utilize generic outputs:
- `write_business_data(df, output_path, format_data='parquet')`
- `write_review_data(df, output_path, format_data='parquet')`
- `write_user_data(df, output_path, format_data='parquet')`

### `utils/helpers.py`
Contains utility functions:
- `create_spark_session(app_name)`: Creates a Spark session.
- `get_num_partitions(df)`: Gets the number of partitions in a DataFrame.

## Principles of Modular Design and Separation of Concerns

### Modular Design
Modular design involves breaking down a system into smaller, self-contained units (modules) that can be developed, tested, and maintained independently. In this project, the ETL process is divided into modules for ingestion, transformation, and output, each with generic and business-specific implementations.

### Separation of Concerns
Separation of concerns is the practice of organizing code so that different concerns or aspects of the system are managed by distinct sections of code. This project separates generic functionality (reusable across different contexts) from business-specific logic (unique to this ETL job). This separation improves maintainability and scalability, as changes in one part of the system are less likely to impact others.

## How to Run
1. Ensure that Docker and Spark are set up correctly.
2. Use the following command to run the Spark job:
   ```sh
   docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --deploy-mode client \
     /opt/bitnami/spark/jobs/app/src/main.py
3. Ensure the PYTHONPATH is set to include src/app/src to properly reference the modules.

## Conclusion
This project demonstrates a well-structured ETL pipeline using Spark, adhering to the principles of modular design and separation of concerns. This approach ensures that the codebase is maintainable, scalable, and easy to understand.
