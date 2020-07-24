from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_columns="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_columns=sql_columns

    def execute(self, context):
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inserting data into {} table.".format(self.table))
        sql_insert = self.sql_columns
        redshift_hook.run(sql_insert)
