# Data Dictionary - Airbus-style Dataset

## Core relationships
- `airline_master.airline_id` → `aircraft_master.airline_id`, `flight_operations.airline_id`
- `aircraft_master.aircraft_id` → flights, maintenance, costs, sensors, crew, incidents
- `airport_master.airport_code` → `flight_operations.source_airport`, `flight_operations.destination_airport`, `weather_daily.airport_code`
- `component_master.component_id` → maintenance and parts inventory
- `maintenance_work_orders.work_order_id` → maintenance_costs.work_order_id
- `flight_operations.flight_id` → crew, sensors, incidents

## Minimum event coverage
- Flight events: 2,000
- Sensor events: 10,000
- Incident events: 1,000
- Maintenance work orders: 1,200

## Recommended Spark read options

```python
# CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(path)

# JSON Lines
df = spark.read.option("multiLine", False).json(path)
```
