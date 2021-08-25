from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers
import etl

# Defining the plugin class
class John_Dev_Plugin(AirflowPlugin):
    name = "john_dev_plugin"
    operators = [
        operators.LoadToRedshiftOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.Copy_sql,
        helpers.Create_sql,
        helpers.Insert_sql
    ]
    data_cleaning = [
        etl.demography_analysis,
        etl.immigration_analysis,
        etl.stage_to_s3
    ]

