import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

PROJECT = "data-management-2-498012"
DATASET = "bronze"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def get_bq_sample(table, limit=10000):
    client = bigquery.Client(project=PROJECT)
    query  = f"SELECT * FROM `{PROJECT}.{DATASET}.{table}` LIMIT {limit}"
    log(f"Reading {limit} rows from {table}...")
    return client.query(query).to_dataframe()

def validate_energy(context):
    log("=== Validating Bronze Energy Data ===")
    df = get_bq_sample("raw_energy_historical")

    # New GE 1.x API
    suite = context.suites.add_or_update(
        ExpectationSuite(name="bronze_energy_suite")
    )

    ds = context.data_sources.add_or_update_pandas(name="energy_pandas")
    da = ds.add_dataframe_asset(name="energy_asset")
    batch_def = da.add_batch_definition_whole_dataframe("energy_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # Define expectations
    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp_ms"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="energy_source"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="region"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="region", value_set=["DE"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="energy_source",
            value_set=[
                "wind_onshore", "wind_offshore", "solar", "biomass",
                "hydro", "pumped_storage", "other_renewables",
                "other_conventional", "lignite", "nuclear",
                "hard_coal", "natural_gas", "consumption", "price_de_lu"
            ]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="value_mw", min_value=-100000, max_value=500000,
            mostly=0.99
        ),
    ]

    for exp in expectations:
        suite.add_expectation(exp)

    # Validate
    results = batch.validate(suite)
    log("Energy validation: " + ("PASSED" if results.success else "FAILED"))
    for r in results.results:
        status = "PASS" if r.success else "FAIL"
        log("  " + status + " - " + r.expectation_config.type)

    return results.success

def validate_weather(context):
    log("=== Validating Bronze Weather Data ===")
    df = get_bq_sample("raw_weather_historical")

    suite = context.suites.add_or_update(
        ExpectationSuite(name="bronze_weather_suite")
    )

    ds = context.data_sources.add_or_update_pandas(name="weather_pandas")
    da = ds.add_dataframe_asset(name="weather_asset")
    batch_def = da.add_batch_definition_whole_dataframe("weather_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="observation_time"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="region"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="region",
            value_set=["DE", "Amprion", "TenneT", "TransnetBW", "50Hertz"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="temperature_c", min_value=-50, max_value=60, mostly=0.99
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="wind_speed_ms", min_value=0, max_value=100, mostly=0.99
        ),
    ]

    for exp in expectations:
        suite.add_expectation(exp)

    results = batch.validate(suite)
    log("Weather validation: " + ("PASSED" if results.success else "FAILED"))
    for r in results.results:
        status = "PASS" if r.success else "FAIL"
        log("  " + status + " - " + r.expectation_config.type)

    return results.success

def validate_crossborder(context):
    log("=== Validating Bronze Cross-Border Data ===")
    df = get_bq_sample("raw_cross_border_flows")

    suite = context.suites.add_or_update(
        ExpectationSuite(name="bronze_crossborder_suite")
    )

    ds = context.data_sources.add_or_update_pandas(name="crossborder_pandas")
    da = ds.add_dataframe_asset(name="crossborder_asset")
    batch_def = da.add_batch_definition_whole_dataframe("crossborder_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp_ms"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="flow_name"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="region"),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="flow_name", value_set=[
                "commercial_total", "physical_total",
                "physical_flow_A", "physical_flow_B",
                "commercial_exchange_A", "commercial_exchange_B",
                "border_small"
            ]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="region", value_set=["DE"]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="value_mw", min_value=-100000, max_value=100000, mostly=0.99
        ),
    ]

    for exp in expectations:
        suite.add_expectation(exp)

    results = batch.validate(suite)
    log("Cross-border validation: " + ("PASSED" if results.success else "FAILED"))
    for r in results.results:
        status = "PASS" if r.success else "FAIL"
        log("  " + status + " - " + r.expectation_config.type)

    return results.success

if __name__ == "__main__":
    log("Starting Great Expectations validation...")
    context = gx.get_context(
        mode="file",
        project_root_dir="./great_expectations"
    )

    energy_ok      = validate_energy(context)
    weather_ok     = validate_weather(context)
    crossborder_ok = validate_crossborder(context)

    log("=== FINAL RESULTS ===")
    log("Energy:       " + ("PASS" if energy_ok      else "FAIL"))
    log("Weather:      " + ("PASS" if weather_ok     else "FAIL"))
    log("Cross-border: " + ("PASS" if crossborder_ok else "FAIL"))

    if energy_ok and weather_ok and crossborder_ok:
        log("ALL VALIDATIONS PASSED!")
    else:
        log("SOME VALIDATIONS FAILED!")
        exit(1)
