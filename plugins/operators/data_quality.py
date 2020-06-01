from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 data_quality_checks,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.data_quality_checks = data_quality_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        failing_tests = []
        error_count = 0

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Adapted from https://knowledge.udacity.com/questions/54406
        for check in self.data_quality_checks:
            self.log.info("Running data quality check: " + check["description"])
            sql = check["check_sql"]
            expected_result = check["expected_result"]

            result = redshift.get_records(sql)[0][0]

            if expected_result != result:
                error_count += 1
                failing_tests.append(sql)
                
            if error_count > 0:
                self.log.info("One or more tests failed")
                self.log.info("Tests that failed:")
                self.log.info(failing_tests)
                raise ValueError("One or more data quality checks failed")
