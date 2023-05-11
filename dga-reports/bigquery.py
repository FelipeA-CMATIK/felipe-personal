from google.cloud import bigquery
import os
from helper import Helper


class BigQuery:
    def __init__(self):
        self.client = bigquery.Client()
        self.helper = Helper()

    def get_rows(self, sensors, start_date, end_date):
        return self.get_sensors_data(self.helper.resolve_ids_for_query(sensors), start_date, end_date)

    def get_sensors_data(self, sensors, start_date, end_date):
        query = self.client.query(f"""
                SELECT 
                    id,
                    sensor_id,
                    result,
                    date
                FROM 
                    {os.environ.get('BQ_DATASET')}.{os.environ.get('BQ_TABLE')}
                WHERE
                    sensor_id IN {sensors}
                    AND date BETWEEN '{start_date}' AND '{end_date}'   
                ORDER BY date ASC      
            """)
        return query.result()
