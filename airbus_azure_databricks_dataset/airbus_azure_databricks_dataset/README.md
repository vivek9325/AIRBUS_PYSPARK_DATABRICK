# Airbus-style Azure Databricks PySpark Dataset

Synthetic, production-style aviation operations dataset for Azure Databricks, PySpark, Delta Lake, Auto Loader, ADLS Gen2, and medallion architecture labs.

This is not official Airbus confidential data. It is generated sample data designed for training and portfolio use.

## Files and row counts

- `aircraft_master.csv`: 80 rows
- `airport_master.csv`: 37 rows
- `airline_master.csv`: 12 rows
- `flight_operations.csv`: 2,000 rows
- `maintenance_work_orders.csv`: 1,200 rows
- `sensor_telemetry.json`: 10,000 rows
- `component_master.csv`: 69 rows
- `parts_inventory.csv`: 500 rows
- `weather_daily.csv`: 6,660 rows
- `crew_roster.csv`: 1,000 rows
- `incident_reports.json`: 1,000 rows
- `maintenance_costs.csv`: 1,200 rows

## Notes
- `sensor_telemetry.json` and `incident_reports.json` are newline-delimited JSON files, one JSON object per line. This is Spark-friendly and works well with Databricks Auto Loader.
- Use the files in ADLS Gen2 under `/landing/airbus/<entity_name>/`.
- Relationships are consistent across aircraft, flights, airlines, airports, components, maintenance, costs, crews, sensors, and incidents.

## Suggested ADLS paths

```text
abfss://landing@<storage-account>.dfs.core.windows.net/airbus/aircraft_master/aircraft_master.csv
abfss://landing@<storage-account>.dfs.core.windows.net/airbus/flight_operations/flight_operations.csv
abfss://landing@<storage-account>.dfs.core.windows.net/airbus/sensor_telemetry/sensor_telemetry.json
```
