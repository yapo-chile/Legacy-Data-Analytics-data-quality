import logging
import great_expectations as ge
from datetime import datetime
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)


class SuiteMetricsTimeSeriesAverages:
    def __init__(self):
        self.logger = logging.getLogger('SuiteMetricsTimeSeriesAverages')
        self.context = ge.data_context.DataContext()
        self.suite_name = 'MetricsTimeSeriesAverages'
        self.datasource = "dw_blocket"
        self.context.create_expectation_suite(
            expectation_suite_name=self.suite_name,
            overwrite_existing=True)
        
    def get_bash_data(self):
        query="""
        SELECT *
        FROM dm_analysis.time_series_data_quality_average
        WHERE timedate between CURRENT_DATE - INTERVAL '30 days' and CURRENT_DATE
        """
        batch_kwargs = {
            "query": query,
            "datasource": self.datasource,
            "data_asset_name": None}
        self.batch = self.context.get_batch(batch_kwargs, self.suite_name)

    def get_metrics_batch(self):
        result = dict()
        result['count'] = self.batch.get_row_count()
        result['mean'] = self.batch.get_column_mean("mtr_var")
        result['median'] = self.batch.get_column_median("mtr_var")
        result['stdev'] = self.batch.get_column_stdev("mtr_var")
        return result
    
    def add_expectations(self):
        metrics = self.get_metrics_batch()

        min_value_fixed = round(metrics['median'] - (metrics['stdev'] * 3))
        max_value_fixed = round(metrics['median'] + (metrics['stdev'] * 3))

        self.batch.expect_column_values_to_not_be_null("entity")
        self.batch.expect_column_values_to_be_of_type("entity", "VARCHAR")
        self.batch.expect_column_values_to_not_be_null("entity_var")
        self.batch.expect_column_values_to_be_of_type("entity_var", "VARCHAR")
        self.batch.expect_column_values_to_not_be_null("mtr_var")
        self.batch.expect_column_values_to_be_of_type("mtr_var", "INTEGER")
        self.batch.expect_column_values_to_be_between("mtr_var", 
                                                 min_value_fixed,
                                                 max_value_fixed)

    def execute_suite(self):
        validation_time = datetime.now()
        print(validation_time)
        self.get_bash_data()
        self.add_expectations()
        run_id = {
            "run_name": "Test_metrics_{}".format(validation_time),
            "run_time": validation_time
        }
        results = self.context.run_validation_operator("action_list_operator",
                                                  assets_to_validate=[self.batch],
                                                  run_id=run_id)
        validation_result_identifier = results.list_validation_result_identifiers()[0]
        self.context.build_data_docs()
        self.context.open_data_docs(validation_result_identifier)
