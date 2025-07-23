# Social-Signals-Unlocking-E-commerce-Trends

## Project Structure:

- `airflow-etl-dags/`: Contains DAGs for each API endpoint.
- `sql/schema.sql`: Contains SQL table definitions for staging tables.

## How to run:
1. Copy DAG files from the airflow-etl-dags folder into your Airflow DAGs folder.
2. Open MySQL and run the schema.sql file to create the necessary tables.
3. Start Airflow by running these commands in your terminal:
        airflow scheduler
        airflow webserver
4. Go to the Airflow web page i.e., http://localhost:8080, turn on the DAGs, and click on "Trigger DAG" to run them.

