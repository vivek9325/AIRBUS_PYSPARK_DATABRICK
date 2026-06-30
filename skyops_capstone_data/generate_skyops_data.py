#!/usr/bin/env python3
"""
SkyOps Analytics - Synthetic Aviation Dataset Generator
=======================================================
Generates the uploadable source files for the "SkyOps Analytics" PySpark on
Azure Databricks capstone. All output is plain files (CSV / JSONL) intended to
be uploaded to a Unity Catalog Volume (or DBFS) - no Azure data services
(Event Hubs, ADF, Synapse) are required.

Design notes
------------
* Deterministic (seeded) so every participant gets identical data.
* Intentional data-quality issues are injected into flights + sensors so the
  Silver-layer cleaning exercises have something real to fix:
    - duplicate flight_id rows
    - null / blank tail numbers and delay codes
    - malformed timestamps
    - negative / impossible delay values
    - an "unknown" tail number not present in the fleet master
    - occasional sensor exceedances (high EGT / vibration / low oil pressure)
* Engine telemetry is written as one JSONL file per day so the streaming /
  Auto Loader "file drop" exercise has a natural micro-batch cadence.

Usage
-----
    python generate_skyops_data.py --out ./skyops_data --days 30
"""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #
AIRCRAFT_TYPES = [
    # (type, manufacturer, engine_type, seats, base)
    ("A220-300", "Airbus", "PW1500G", 145, "BLR"),
    ("A320neo", "Airbus", "LEAP-1A", 180, "BLR"),
    ("E190-E2", "Embraer", "PW1900G", 106, "HYD"),
    ("ATR72-600", "ATR", "PW127XT", 70, "HYD"),
    ("B737-800", "Boeing", "CFM56-7B", 189, "DEL"),
]

# IATA, ICAO, name, city, country, lat, lon, tz, elevation_ft
AIRPORTS = [
    ("BLR", "VOBL", "Kempegowda Intl", "Bengaluru", "IN", 13.1979, 77.7063, "Asia/Kolkata", 3000),
    ("HYD", "VOHS", "Rajiv Gandhi Intl", "Hyderabad", "IN", 17.2403, 78.4294, "Asia/Kolkata", 2024),
    ("DEL", "VIDP", "Indira Gandhi Intl", "Delhi", "IN", 28.5562, 77.1000, "Asia/Kolkata", 777),
    ("BOM", "VABB", "Chhatrapati Shivaji", "Mumbai", "IN", 19.0887, 72.8679, "Asia/Kolkata", 39),
    ("MAA", "VOMM", "Chennai Intl", "Chennai", "IN", 12.9941, 80.1709, "Asia/Kolkata", 52),
    ("CCU", "VECC", "Netaji Subhash", "Kolkata", "IN", 22.6547, 88.4467, "Asia/Kolkata", 16),
    ("COK", "VOCI", "Cochin Intl", "Kochi", "IN", 10.1520, 76.4019, "Asia/Kolkata", 30),
    ("GOI", "VOGO", "Goa Intl", "Goa", "IN", 15.3808, 73.8314, "Asia/Kolkata", 150),
    ("PNQ", "VAPO", "Pune Airport", "Pune", "IN", 18.5821, 73.9197, "Asia/Kolkata", 1942),
    ("AMD", "VAAH", "Sardar Vallabhbhai", "Ahmedabad", "IN", 23.0772, 72.6347, "Asia/Kolkata", 189),
    ("JAI", "VIJP", "Jaipur Intl", "Jaipur", "IN", 26.8242, 75.8122, "Asia/Kolkata", 1263),
    ("LKO", "VILK", "Chaudhary Charan", "Lucknow", "IN", 26.7606, 80.8893, "Asia/Kolkata", 410),
]

# IATA delay codes (subset, simplified) -> (category, controllable_flag, description)
DELAY_CODES = [
    ("11", "Passenger/Baggage", "Y", "Late check-in, acceptance after deadline"),
    ("36", "Ramp/Loading", "Y", "Fuelling / defuelling, fuel supplier"),
    ("41", "Technical/Aircraft", "Y", "Aircraft defects, technical"),
    ("51", "Damage/Tech", "Y", "Damage during ground operations"),
    ("61", "Flight Ops/Crew", "Y", "Flight plan, late crew"),
    ("71", "Weather", "N", "Departure station weather"),
    ("73", "Weather", "N", "De-icing of aircraft"),
    ("81", "ATC", "N", "ATC restriction en-route / flow"),
    ("89", "Airport/Govt", "N", "Restrictions at airport, immigration"),
    ("93", "Reactionary", "N", "Aircraft rotation, late inbound"),
]

ATA_CHAPTERS = [
    ("21", "Air Conditioning"), ("24", "Electrical Power"), ("27", "Flight Controls"),
    ("28", "Fuel"), ("29", "Hydraulic Power"), ("32", "Landing Gear"),
    ("34", "Navigation"), ("49", "APU"), ("71", "Power Plant"), ("72", "Engine"),
    ("73", "Engine Fuel"), ("79", "Engine Oil"),
]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_fleet(rng, n=40):
    """Generate the aircraft master and a parallel structural-health profile."""
    fleet = []
    profiles = {}
    base_year = 2014
    for i in range(n):
        t = rng.choice(AIRCRAFT_TYPES)
        tail = f"VT-{chr(rng.randint(65, 90))}{chr(rng.randint(65, 90))}{rng.randint(10, 99)}"
        msn = rng.randint(1000, 9999)
        year = rng.randint(base_year, 2024)
        fleet.append({
            "tail_number": tail,
            "aircraft_type": t[0],
            "manufacturer": t[1],
            "msn": msn,
            "year_built": year,
            "engine_type": t[2],
            "seats": t[3],
            "base_station": t[4],
        })
        # Hidden health profile drives sensor degradation + failure likelihood.
        age = 2024 - year
        wear = min(1.0, age / 12.0) + rng.uniform(-0.1, 0.25)
        profiles[tail] = {
            "wear": max(0.05, min(1.0, wear)),
            "egt_bias": rng.uniform(-8, 18) + wear * 25,
            "vib_bias": rng.uniform(-0.05, 0.10) + wear * 0.25,
            "oil_press_bias": rng.uniform(-3, 2) - wear * 6,
            "seats": t[3],
            "engine_type": t[2],
        }
    return fleet, profiles


def build_routes(rng):
    routes = []
    rid = 1000
    pairs = set()
    iatas = [a[0] for a in AIRPORTS]
    while len(routes) < 120:
        o, d = rng.sample(iatas, 2)
        if (o, d) in pairs:
            continue
        pairs.add((o, d))
        # crude great-circle-ish distance proxy from lat/lon
        ao = next(a for a in AIRPORTS if a[0] == o)
        ad = next(a for a in AIRPORTS if a[0] == d)
        dist = int(((ao[5] - ad[5]) ** 2 + (ao[6] - ad[6]) ** 2) ** 0.5 * 60) + 80
        block = int(dist / 7.5) + 35  # minutes, rough
        routes.append({
            "route_id": rid,
            "origin_iata": o,
            "dest_iata": d,
            "distance_nm": dist,
            "scheduled_block_min": block,
        })
        rid += 1
    return routes


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./skyops_data")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = args.out
    os.makedirs(out, exist_ok=True)
    for sub in ("flights", "sensors"):
        os.makedirs(os.path.join(out, sub), exist_ok=True)

    # ----- reference / master files ---------------------------------------- #
    fleet, profiles = build_fleet(rng, n=40)
    write_csv(os.path.join(out, "aircraft.csv"), fleet,
              ["tail_number", "aircraft_type", "manufacturer", "msn",
               "year_built", "engine_type", "seats", "base_station"])

    write_csv(os.path.join(out, "airports.csv"),
              [dict(zip(["iata", "icao", "name", "city", "country", "lat",
                         "lon", "tz", "elevation_ft"], a)) for a in AIRPORTS],
              ["iata", "icao", "name", "city", "country", "lat", "lon", "tz", "elevation_ft"])

    routes = build_routes(rng)
    write_csv(os.path.join(out, "routes.csv"), routes,
              ["route_id", "origin_iata", "dest_iata", "distance_nm", "scheduled_block_min"])

    write_csv(os.path.join(out, "delay_codes.csv"),
              [dict(zip(["delay_code", "category", "controllable_flag", "description"], d))
               for d in DELAY_CODES],
              ["delay_code", "category", "controllable_flag", "description"])

    tails = [a["tail_number"] for a in fleet]
    route_by_id = {r["route_id"]: r for r in routes}
    start = datetime(2025, 5, 1, tzinfo=timezone.utc)

    # ----- daily flights + engine telemetry -------------------------------- #
    fuel_rows = []
    failure_rows = []
    flight_seq = 0
    dq_dupe_pool = []  # collect a few rows to duplicate

    for day in range(args.days):
        d0 = start + timedelta(days=day)
        date_str = d0.strftime("%Y%m%d")
        day_flights = []
        sensor_lines = []

        # ~8-12 flights/day per active aircraft subset
        active = rng.sample(tails, k=rng.randint(28, 38))
        for tail in active:
            prof = profiles[tail]
            n_legs = rng.randint(2, 5)
            # start the day mid-morning local-ish
            clock = d0 + timedelta(hours=rng.randint(1, 3), minutes=rng.choice([0, 15, 30, 45]))
            cur_station = rng.choice([a[0] for a in AIRPORTS])
            for _leg in range(n_legs):
                cand = [r for r in routes if r["origin_iata"] == cur_station]
                if not cand:
                    cand = routes
                route = rng.choice(cand)
                flight_seq += 1
                fid = f"MR{1000 + flight_seq}"
                block = route["scheduled_block_min"]
                sched_dep = clock
                sched_arr = sched_dep + timedelta(minutes=block)

                # delays
                if rng.random() < 0.62:
                    dep_delay = 0 if rng.random() < 0.55 else rng.randint(1, 14)
                else:
                    dep_delay = rng.randint(15, 180)
                code = "" if dep_delay <= 15 else rng.choice([c[0] for c in DELAY_CODES])
                cancelled = 1 if rng.random() < 0.015 else 0
                diverted = 1 if (cancelled == 0 and rng.random() < 0.006) else 0

                actual_dep = sched_dep + timedelta(minutes=dep_delay)
                air_min = max(20, block - rng.randint(8, 18))
                taxi = rng.randint(10, 28)
                actual_block = air_min + taxi
                arr_delay = dep_delay + rng.randint(-8, 12)
                actual_arr = actual_dep + timedelta(minutes=actual_block)
                pax = int(prof["seats"] * rng.uniform(0.55, 0.98)) if cancelled == 0 else 0
                cargo = round(rng.uniform(200, 2500), 1) if cancelled == 0 else 0.0

                row = {
                    "flight_id": fid,
                    "flight_date": d0.strftime("%Y-%m-%d"),
                    "tail_number": tail,
                    "route_id": route["route_id"],
                    "origin": route["origin_iata"],
                    "dest": route["dest_iata"],
                    "sched_dep_utc": iso(sched_dep),
                    "actual_dep_utc": "" if cancelled else iso(actual_dep),
                    "sched_arr_utc": iso(sched_arr),
                    "actual_arr_utc": "" if cancelled else iso(actual_arr),
                    "dep_delay_min": dep_delay if cancelled == 0 else "",
                    "arr_delay_min": arr_delay if cancelled == 0 else "",
                    "delay_code": code,
                    "cancelled": cancelled,
                    "diverted": diverted,
                    "passengers": pax,
                    "cargo_kg": cargo,
                    "block_min": actual_block if cancelled == 0 else "",
                    "air_min": air_min if cancelled == 0 else "",
                }
                day_flights.append(row)

                # fuel uplift (separate file, joined later by flight_id)
                if cancelled == 0:
                    burn = round(air_min * rng.uniform(38, 55) * (prof["seats"] / 150.0), 1)
                    fuel_rows.append({
                        "flight_id": fid,
                        "fuel_uplift_kg": round(burn * rng.uniform(1.05, 1.25), 1),
                        "fuel_burn_kg": burn,
                        "fuel_price_usd_per_kg": round(rng.uniform(0.78, 0.95), 4),
                    })

                # engine telemetry: a handful of readings per engine per flight
                if cancelled == 0:
                    n_engines = 1 if prof["engine_type"] == "PW127XT" else 2
                    n_engines = max(1, n_engines)
                    for eng in range(1, (1 if prof["engine_type"].startswith("PW127") else 2) + 1):
                        for k in range(rng.randint(3, 6)):
                            t = actual_dep + timedelta(minutes=rng.randint(2, max(3, air_min)))
                            exceed = rng.random() < (0.04 + prof["wear"] * 0.10)
                            egt = 540 + prof["egt_bias"] + rng.uniform(-10, 25) + (90 if exceed else 0)
                            vib = 0.25 + prof["vib_bias"] + rng.uniform(-0.05, 0.12) + (0.9 if exceed else 0)
                            oilp = 52 + prof["oil_press_bias"] + rng.uniform(-3, 3) - (14 if exceed else 0)
                            sensor_lines.append({
                                "reading_id": f"{fid}-E{eng}-{k}",
                                "tail_number": tail,
                                "engine_position": eng,
                                "ts_utc": iso(t),
                                "flight_id": fid,
                                "egt_c": round(egt, 1),
                                "n1_pct": round(rng.uniform(82, 99), 1),
                                "n2_pct": round(rng.uniform(88, 101), 1),
                                "oil_temp_c": round(rng.uniform(75, 120) + prof["wear"] * 15, 1),
                                "oil_pressure_psi": round(oilp, 1),
                                "vib_ips": round(max(0.05, vib), 3),
                                "fuel_flow_pph": round(rng.uniform(1800, 4200), 0),
                            })

                # chain next leg
                cur_station = route["dest_iata"]
                clock = actual_arr + timedelta(minutes=rng.randint(30, 70))

            # occasional failure event for high-wear aircraft
            if rng.random() < (0.02 + prof["wear"] * 0.06):
                ata = rng.choice([c for c in ATA_CHAPTERS if c[0] in ("71", "72", "73", "79")])
                failure_rows.append({
                    "tail_number": tail,
                    "event_date": d0.strftime("%Y-%m-%d"),
                    "ata_chapter": ata[0],
                    "event_type": "Unscheduled Removal",
                    "description": f"{ata[1]} - in-service finding",
                })

        # ---- inject intentional data-quality issues into this day's flights
        if day_flights:
            # 1) duplicate a couple of rows
            for _ in range(min(2, len(day_flights))):
                dq_dupe_pool.append(dict(rng.choice(day_flights)))
            # 2) null/blank tail number
            rng.choice(day_flights)["tail_number"] = ""
            # 3) malformed timestamp
            victim = rng.choice(day_flights)
            victim["sched_dep_utc"] = "2025/13/01 99:61"
            # 4) negative delay
            rng.choice(day_flights)["dep_delay_min"] = -45
            # 5) unknown tail not in fleet master
            if rng.random() < 0.5:
                rng.choice(day_flights)["tail_number"] = "VT-ZZ00"

        # write flights file for the day (append duplicates from earlier days too)
        todays = day_flights + [r for r in dq_dupe_pool if r["flight_date"] == d0.strftime("%Y-%m-%d")]
        fpath = os.path.join(out, "flights", f"flights_{date_str}.csv")
        write_csv(fpath, todays, [
            "flight_id", "flight_date", "tail_number", "route_id", "origin", "dest",
            "sched_dep_utc", "actual_dep_utc", "sched_arr_utc", "actual_arr_utc",
            "dep_delay_min", "arr_delay_min", "delay_code", "cancelled", "diverted",
            "passengers", "cargo_kg", "block_min", "air_min",
        ])

        # write sensor JSONL file for the day (the streaming "drop")
        spath = os.path.join(out, "sensors", f"engine_telemetry_{date_str}.jsonl")
        with open(spath, "w") as f:
            for line in sensor_lines:
                f.write(json.dumps(line) + "\n")

    # ----- fuel + maintenance + failures ----------------------------------- #
    write_csv(os.path.join(out, "fuel_uplift.csv"), fuel_rows,
              ["flight_id", "fuel_uplift_kg", "fuel_burn_kg", "fuel_price_usd_per_kg"])

    # maintenance work orders
    wo_rows = []
    statuses = ["CLOSED", "CLOSED", "CLOSED", "OPEN", "DEFERRED"]
    for i in range(220):
        tail = rng.choice(tails)
        ata = rng.choice(ATA_CHAPTERS)
        opened = start + timedelta(days=rng.randint(-40, args.days - 1))
        status = rng.choice(statuses)
        closed = "" if status != "CLOSED" else iso(opened + timedelta(days=rng.randint(0, 9)))[:10]
        wo_rows.append({
            "wo_id": f"WO{50000 + i}",
            "tail_number": tail,
            "opened_date": opened.strftime("%Y-%m-%d"),
            "closed_date": closed,
            "ata_chapter": ata[0],
            "description": f"{ata[1]} inspection / rectification",
            "mel_category": rng.choice(["", "", "A", "B", "C", "D"]),
            "status": status,
            "labor_hours": round(rng.uniform(0.5, 24), 1),
            "parts_cost_usd": round(rng.uniform(0, 18000), 2),
        })
    write_csv(os.path.join(out, "maintenance_workorders.csv"), wo_rows,
              ["wo_id", "tail_number", "opened_date", "closed_date", "ata_chapter",
               "description", "mel_category", "status", "labor_hours", "parts_cost_usd"])

    write_csv(os.path.join(out, "failure_events.csv"), failure_rows,
              ["tail_number", "event_date", "ata_chapter", "event_type", "description"])

    # ----- manifest -------------------------------------------------------- #
    manifest = {
        "generated_utc": iso(datetime.now(timezone.utc)),
        "seed": args.seed,
        "days": args.days,
        "counts": {
            "aircraft": len(fleet),
            "airports": len(AIRPORTS),
            "routes": len(routes),
            "delay_codes": len(DELAY_CODES),
            "flight_files": args.days,
            "sensor_files": args.days,
            "fuel_uplift_rows": len(fuel_rows),
            "workorders": len(wo_rows),
            "failure_events": len(failure_rows),
        },
        "known_data_quality_issues": [
            "duplicate flight_id rows (same key, repeated)",
            "blank tail_number values",
            "tail_number 'VT-ZZ00' not present in aircraft.csv (referential break)",
            "malformed sched_dep_utc timestamp ('2025/13/01 99:61')",
            "negative dep_delay_min values",
            "blank delay_code on on-time flights (expected, not an error)",
            "engine exceedances: high egt_c / vib_ips, low oil_pressure_psi",
        ],
    }
    with open(os.path.join(out, "_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(json.dumps(manifest["counts"], indent=2))
    print("Output written to:", os.path.abspath(out))


if __name__ == "__main__":
    main()
