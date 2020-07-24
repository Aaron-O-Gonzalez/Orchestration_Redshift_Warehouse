from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id="",
                 table_collection=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id=redshift_conn_id
        self.table_collection=table_collection

    def execute(self, context):
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inspecting records in each table.")
        
        '''Counts the number of rows for each table to ensure that there is at least one entry present'''
        for table in self.table_collection:
            record=redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            self.log.info("There are {} records in the {} table.".format(record[0][0], table))
            num_records=int(record[0][0])
            if num_records < 1:
                raise ValueError("There are no records present in {} table".format(table))
            else:
                self.log.info("Data quality inspection for {} table has passed.".format(table))