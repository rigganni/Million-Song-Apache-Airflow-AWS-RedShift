from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('Load dimension: ' + self.table)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate table if True
        if self.truncate_table:
            trunate_query = "TRUNCATE TABLE {};".format(self.table)
            redshift.run(self.truncate_query)

        redshift.run(self.sql_query)
