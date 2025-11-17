#!/usr/bin/env python
# coding: utf-8

# # City Water Usage & Distribution Monitoring System  
# **Student:** Attaullah Khan  
# **Student ID:** 24093721  
# 
# This notebook programmatically builds and populates the SQLite database
# `attaullah_water_system.db` for a city water usage and distribution monitoring system.
# 
# All tables, data, and relationships are generated here – no external datasets are used.
# 

# ## 1. Setup: Install Libraries
# 
# First, we install the `faker` library, which is used to generate realistic-looking fake data for populating our database tables.

# In[30]:


pip install faker


# ## 2. Setup: Import Libraries and Database Connection
# 
# We import necessary libraries (`sqlite3`, `random`, `datetime` functions from `datetime`, and `Faker`). We then define the database path and a helper function `get_connection()` to establish a connection to our SQLite database, ensuring foreign key constraints are enforced.

# In[31]:


import sqlite3
import random
from datetime import date, datetime, timedelta

from faker import Faker
fake = Faker()

DB_PATH = "attaullah_water_system.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ## 3. Define Database Schema
# 
# This section defines the SQL schema for our water usage monitoring system. It includes `DROP TABLE` statements to clear any existing tables, followed by `CREATE TABLE` statements for all entities: `water_zones`, `reservoirs`, `customers`, `meters`, `bills`, `meter_readings`, `leak_events`, and `zone_daily_stats`. Each table includes primary keys, foreign keys, data types, and relevant constraints.
# 
# The `apply_schema()` function executes this SQL script to create or refresh the database structure.

# In[32]:


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS zone_daily_stats;
DROP TABLE IF EXISTS leak_events;
DROP TABLE IF EXISTS meter_readings;
DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS meters;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS reservoirs;
DROP TABLE IF EXISTS water_zones;

-- (PRAGMA foreign_keys = ON;

-- Drop in dependency order (children first)
DROP TABLE IF EXISTS zone_daily_stats;
DROP TABLE IF EXISTS leak_events;
DROP TABLE IF EXISTS meter_readings;
DROP TABLE IF EXISTS bills;
DROP TABLE IF EXISTS meters;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS reservoirs;
DROP TABLE IF EXISTS water_zones;

------------------------------------------------------------
-- 1. WATER ZONES (DMA / supply zones)
------------------------------------------------------------
CREATE TABLE water_zones (
    zone_id              INTEGER PRIMARY KEY,
    zone_name            TEXT NOT NULL,
    city                 TEXT NOT NULL,
    zone_type            TEXT NOT NULL,           -- Residential, Industrial, Mixed, Rural
    pressure_level       INTEGER NOT NULL CHECK (pressure_level BETWEEN 1 AND 3),
    -- 1 = Low, 2 = Normal, 3 = High
    risk_level           INTEGER NOT NULL CHECK (risk_level BETWEEN 1 AND 3),
    -- 1 = Low NRW risk, 3 = High
    is_active            INTEGER NOT NULL CHECK (is_active IN (0,1))
);

------------------------------------------------------------
-- 2. RESERVOIRS / SOURCES (supply side)
------------------------------------------------------------
CREATE TABLE reservoirs (
    reservoir_id     INTEGER PRIMARY KEY,
    zone_id          INTEGER NOT NULL,
    name             TEXT NOT NULL,
    water_source_type TEXT NOT NULL,       -- River, Groundwater, Lake, Desalination, Mixed
    capacity_m3      REAL NOT NULL CHECK (capacity_m3 > 0),
    current_level_m3 REAL NOT NULL CHECK (current_level_m3 >= 0),
    built_year       INTEGER NOT NULL CHECK (built_year BETWEEN 1950 AND 2025),
    is_operational   INTEGER NOT NULL CHECK (is_operational IN (0,1)),
    FOREIGN KEY (zone_id) REFERENCES water_zones(zone_id)
);

------------------------------------------------------------
-- 3. CUSTOMERS / CONNECTIONS (demand side)
------------------------------------------------------------
CREATE TABLE customers (
    customer_id     INTEGER PRIMARY KEY,
    full_name       TEXT NOT NULL,
    address_line1   TEXT NOT NULL,
    address_line2   TEXT,
    city            TEXT NOT NULL,
    zone_id         INTEGER NOT NULL,
    property_type   TEXT NOT NULL,         -- Apartment, House, Shop, Factory, Office, etc.
    num_residents   INTEGER CHECK (num_residents >= 0),
    has_roof_tank   INTEGER NOT NULL CHECK (has_roof_tank IN (0,1)),
    signup_date     DATE NOT NULL,
    tariff_band     INTEGER NOT NULL CHECK (tariff_band BETWEEN 1 AND 4),
    -- 1 = Lifeline, 2 = Domestic, 3 = Commercial, 4 = Industrial
    is_active       INTEGER NOT NULL CHECK (is_active IN (0,1)),
    FOREIGN KEY (zone_id) REFERENCES water_zones(zone_id)
);

------------------------------------------------------------
-- 4. METERS (physical meters at customer properties)
------------------------------------------------------------
CREATE TABLE meters (
    meter_id            INTEGER PRIMARY KEY,
    customer_id         INTEGER NOT NULL,
    zone_id             INTEGER NOT NULL,
    meter_type          TEXT NOT NULL,         -- Mechanical, Smart, Bulk
    install_date        DATE NOT NULL,
    last_service_date   DATE,
    status              TEXT NOT NULL,         -- active, inactive, faulty, replaced
    nominal_flow_m3_h   REAL NOT NULL CHECK (nominal_flow_m3_h > 0),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (zone_id)    REFERENCES water_zones(zone_id)
);

------------------------------------------------------------
-- 5. BILLS (billing periods per meter)
------------------------------------------------------------
CREATE TABLE bills (
    bill_id               INTEGER PRIMARY KEY,
    customer_id           INTEGER NOT NULL,
    meter_id              INTEGER NOT NULL,
    billing_period_start  DATE NOT NULL,
    billing_period_end    DATE NOT NULL,
    total_consumption_m3  REAL NOT NULL CHECK (total_consumption_m3 >= 0),
    fixed_charge          REAL NOT NULL CHECK (fixed_charge >= 0),
    variable_charge       REAL NOT NULL CHECK (variable_charge >= 0),
    bill_amount           REAL NOT NULL CHECK (bill_amount >= 0),
    issue_date            DATE NOT NULL,
    due_date              DATE NOT NULL,
    paid_amount           REAL NOT NULL CHECK (paid_amount >= 0),
    paid_date             DATE,
    payment_status        TEXT NOT NULL,  -- unpaid, partial, paid, written_off
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (meter_id)    REFERENCES meters(meter_id)
);

------------------------------------------------------------
-- 6. METER READINGS (THIS WILL BE OUR 1000+ ROW TABLE)
------------------------------------------------------------
CREATE TABLE meter_readings (
    reading_id           INTEGER PRIMARY KEY,
    meter_id             INTEGER NOT NULL,
    reading_date         DATE NOT NULL,
    reading_value_m3     REAL NOT NULL CHECK (reading_value_m3 >= 0),
    daily_consumption_m3 REAL CHECK (daily_consumption_m3 >= 0),
    is_estimated         INTEGER NOT NULL CHECK (is_estimated IN (0,1)),
    data_quality_flag    TEXT,                -- ok, outlier, missing_pulse, rollover
    FOREIGN KEY (meter_id) REFERENCES meters(meter_id)
);

------------------------------------------------------------
-- 7. LEAK EVENTS (NRW / leaks detected in network or at connection)
------------------------------------------------------------
CREATE TABLE leak_events (
    leak_id          INTEGER PRIMARY KEY,
    meter_id         INTEGER,
    zone_id          INTEGER NOT NULL,
    detected_time    DATETIME NOT NULL,
    severity_level   INTEGER NOT NULL CHECK (severity_level BETWEEN 1 AND 5),
    estimated_loss_m3 REAL NOT NULL CHECK (estimated_loss_m3 >= 0),
    leak_location    TEXT NOT NULL,           -- Street, Chamber, CustomerPipe, etc.
    status           TEXT NOT NULL,           -- open, in_progress, resolved, false_alarm
    resolved_time    DATETIME,
    FOREIGN KEY (meter_id) REFERENCES meters(meter_id),
    FOREIGN KEY (zone_id)  REFERENCES water_zones(zone_id)
);

------------------------------------------------------------
-- 8. ZONE DAILY STATS (COMPOSITE KEY TABLE FOR DMA ANALYSIS)
------------------------------------------------------------
CREATE TABLE zone_daily_stats (
    zone_id              INTEGER NOT NULL,
    stat_date            DATE NOT NULL,
    total_input_m3       REAL NOT NULL CHECK (total_input_m3 >= 0),
    total_billed_m3      REAL NOT NULL CHECK (total_billed_m3 >= 0),
    non_revenue_water_m3 REAL NOT NULL CHECK (non_revenue_water_m3 >= 0),
    PRIMARY KEY (zone_id, stat_date),
    FOREIGN KEY (zone_id) REFERENCES water_zones(zone_id)
);

"""

def apply_schema():
    conn = get_connection()
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()
    print("Schema created/refreshed inside attaullah_water_system.db")

# Uncomment this line if you want to recreate schema from notebook
# apply_schema()


# ## 4. Data Seeding Functions
# 
# These Python functions generate and insert synthetic data into the database tables using the `faker` library and random values. Each function populates a specific table with realistic, yet random, data.
# 
# ### 4.1. Seed Water Zones
# 
# `seed_water_zones()` populates the `water_zones` table with a specified number of zones, assigning random names, cities, types, pressure levels, risk levels, and active statuses.

# In[33]:


def seed_water_zones(conn, n_zones=10):
    cur = conn.cursor()
    cities = ["London", "Birmingham", "Manchester", "Leeds", "Glasgow"]
    zone_types = ["Residential", "Industrial", "Mixed", "Rural"]

    for zid in range(1, n_zones + 1):
        name = f"Zone-{zid}"
        city = random.choice(cities)
        ztype = random.choice(zone_types)
        pressure = random.randint(1, 3)
        risk = random.randint(1, 3)
        is_active = random.choice([0, 1])

        cur.execute("""
            INSERT INTO water_zones
            (zone_id, zone_name, city, zone_type, pressure_level, risk_level, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (zid, name, city, ztype, pressure, risk, is_active))

    conn.commit()


def seed_reservoirs(conn):
    cur = conn.cursor()
    cur.execute("SELECT zone_id FROM water_zones")
    zones = [row[0] for row in cur.fetchall()]

    source_types = ["River", "Groundwater", "Lake", "Desalination", "Mixed"]

    res_id = 1
    for zone in zones:
        # 1–3 reservoirs per zone
        for _ in range(random.randint(1, 3)):
            name = f"Reservoir-{res_id}"
            src = random.choice(source_types)
            capacity = round(random.uniform(5_000, 200_000), 1)
            level = round(capacity * random.uniform(0.3, 1.0), 1)
            built_year = random.randint(1960, 2024)
            is_operational = random.choice([0, 1])

            cur.execute("""
                INSERT INTO reservoirs
                (reservoir_id, zone_id, name, water_source_type, capacity_m3,
                 current_level_m3, built_year, is_operational)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (res_id, zone, name, src, capacity, level, built_year, is_operational))
            res_id += 1

    conn.commit()


# ### 4.2. Seed Reservoirs
# 
# `seed_reservoirs()` populates the `reservoirs` table. It creates 1 to 3 reservoirs for each existing water zone, assigning attributes like name, water source type, capacity, current water level, build year, and operational status.

# ### 4.3. Seed Customers
# 
# `seed_customers()` generates customer data for the `customers` table. It assigns customers to existing zones and cities, creates fake names and addresses, property types, number of residents, roof tank status, signup dates, tariff bands, and active status.

# In[34]:


def seed_customers(conn, n_customers=350):
    cur = conn.cursor()
    cur.execute("SELECT zone_id, city FROM water_zones")
    zones = cur.fetchall()

    property_types = ["Apartment", "House", "Shop", "Factory", "Office", "Warehouse"]

    for cid in range(1, n_customers + 1):
        full_name = fake.name()
        # Attach customers to zone city for realism
        zone_id, city = random.choice(zones)
        addr1 = fake.street_address()
        addr2 = fake.secondary_address() if random.random() > 0.4 else None  # 60% missing
        prop_type = random.choice(property_types)

        if prop_type in ["Apartment", "House"]:
            num_residents = random.randint(1, 8)
        else:
            # non-residential; treat residents as 0 or NULL
            num_residents = 0 if random.random() > 0.2 else None

        has_roof_tank = random.choice([0, 1])
        signup_date = fake.date_between(start_date="-8y", end_date="today")
        tariff_band = random.randint(1, 4)
        is_active = random.choice([0, 1])

        cur.execute("""
            INSERT INTO customers
            (customer_id, full_name, address_line1, address_line2, city, zone_id,
             property_type, num_residents, has_roof_tank, signup_date,
             tariff_band, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (cid, full_name, addr1, addr2, city, zone_id,
              prop_type, num_residents, has_roof_tank, signup_date,
              tariff_band, is_active))

    conn.commit()


# ### 4.4. Seed Meters
# 
# `seed_meters()` populates the `meters` table, creating 1 or 2 meters for each customer. It assigns meter types, installation dates, last service dates (some missing), status, and nominal flow rates.

# In[35]:


def seed_meters(conn):
    cur = conn.cursor()
    cur.execute("SELECT customer_id, zone_id FROM customers")
    cust_rows = cur.fetchall()

    meter_types = ["Mechanical", "Smart", "Bulk"]
    statuses = ["active", "inactive", "faulty", "replaced"]

    meter_id = 1
    for customer_id, zone_id in cust_rows:
        # 1 or 2 meters per customer
        for _ in range(1 if random.random() > 0.3 else 2):
            mtype = random.choice(meter_types)
            install_date = fake.date_between(start_date="-6y", end_date="today")
            last_service_date = (
                fake.date_between(start_date=install_date, end_date="today")
                if random.random() > 0.3 else None     # ~30% missing service date
            )
            status = random.choices(statuses, weights=[0.6, 0.1, 0.15, 0.15], k=1)[0]
            nominal_flow = round(random.uniform(0.5, 25.0), 2)

            cur.execute("""
                INSERT INTO meters
                (meter_id, customer_id, zone_id, meter_type, install_date,
                 last_service_date, status, nominal_flow_m3_h)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (meter_id, customer_id, zone_id, mtype, install_date,
                  last_service_date, status, nominal_flow))
            meter_id += 1

    conn.commit()


# ### 4.5. Seed Bills
# 
# `seed_bills()` generates billing records for the `bills` table. For each meter, it creates 2 to 4 quarterly bills with simulated consumption, fixed and variable charges, bill amounts, issue dates, due dates, and payment statuses (paid, partial, unpaid, written-off). It also adds some deliberate duplicate bills to simulate data entry issues.

# In[36]:


def seed_bills(conn):
    cur = conn.cursor()
    cur.execute("SELECT meter_id, customer_id FROM meters")
    meter_rows = cur.fetchall()

    bills_insert = []

    for meter_id, customer_id in meter_rows:
        # 2–4 bills per meter (quarterly periods)
        n_bills = random.randint(2, 4)
        start_date = date(2023, 1, 1) - timedelta(days=90 * n_bills)

        for _ in range(n_bills):
            period_start = start_date
            period_end = period_start + timedelta(days=89)

            total_consumption = round(random.uniform(5, 120), 1)
            fixed_charge = round(random.uniform(5, 20), 2)
            variable_charge = round(total_consumption * random.uniform(0.3, 1.2), 2)
            bill_amount = round(fixed_charge + variable_charge, 2)

            issue_date = period_end + timedelta(days=5)
            due_date = issue_date + timedelta(days=20)

            # payment behaviour
            payment_status = random.choices(
                ["unpaid", "partial", "paid", "written_off"],
                weights=[0.15, 0.15, 0.6, 0.1],
                k=1
            )[0]

            if payment_status == "paid":
                paid_amount = bill_amount
                paid_date = issue_date + timedelta(days=random.randint(0, 15))
            elif payment_status == "partial":
                paid_amount = round(bill_amount * random.uniform(0.2, 0.8), 2)
                paid_date = issue_date + timedelta(days=random.randint(10, 40))
            elif payment_status == "written_off":
                paid_amount = 0.0
                paid_date = None
            else:  # unpaid
                paid_amount = 0.0
                paid_date = None

            bills_insert.append((
                customer_id, meter_id,
                period_start, period_end,
                total_consumption, fixed_charge, variable_charge, bill_amount,
                issue_date, due_date, paid_amount, paid_date, payment_status
            ))

            start_date = period_start + timedelta(days=90)

    # Insert bills
    cur.executemany("""
        INSERT INTO bills
        (customer_id, meter_id, billing_period_start, billing_period_end,
         total_consumption_m3, fixed_charge, variable_charge, bill_amount,
         issue_date, due_date, paid_amount, paid_date, payment_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, bills_insert)
    conn.commit()

    # Add deliberate duplicate bills: reinsert some with new bill_id but same content
    cur.execute("SELECT bill_id, customer_id, meter_id, billing_period_start, billing_period_end, \
                        total_consumption_m3, fixed_charge, variable_charge, bill_amount, \
                        issue_date, due_date, paid_amount, paid_date, payment_status \
                 FROM bills LIMIT 8")
    dup_rows = cur.fetchall()

    for row in dup_rows:
        # Ignore original bill_id and insert as new bill
        _, customer_id, meter_id, b_start, b_end, cons, fix, var, amt, \
            issue, due, paid_amt, paid_date, status = row

        cur.execute("""
            INSERT INTO bills
            (customer_id, meter_id, billing_period_start, billing_period_end,
             total_consumption_m3, fixed_charge, variable_charge, bill_amount,
             issue_date, due_date, paid_amount, paid_date, payment_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (customer_id, meter_id, b_start, b_end, cons, fix, var, amt,
              issue, due, paid_amt, paid_date, status))

    conn.commit()


# ### 4.6. Seed Meter Readings
# 
# `seed_meter_readings()` generates a large number of meter readings for a sample of meters. It creates 5 to 15 daily readings per meter, including reading values, calculated daily consumption (sometimes estimated or missing), and data quality flags (ok, outlier, missing_pulse, rollover).

# In[37]:


def seed_meter_readings(conn, target_readings=1500):
    cur = conn.cursor()
    cur.execute("SELECT meter_id FROM meters")
    all_meters = [row[0] for row in cur.fetchall()]

    # Use a subset of meters but multiple readings each
    meters_sample = random.sample(all_meters, min(len(all_meters), 200))

    readings_to_insert = []
    reading_id = 1

    for meter_id in meters_sample:
        # 5–15 consecutive readings per meter
        n_read = random.randint(5, 15)
        start_date = date(2024, 1, 1) - timedelta(days=n_read)

        last_value = round(random.uniform(0, 500), 1)

        for _ in range(n_read):
            reading_date = start_date
            # daily consumption, sometimes zero or estimated
            if random.random() > 0.1:
                daily_cons = round(random.uniform(0.0, 3.5), 2)
                reading_value = round(last_value + daily_cons, 2)
            else:
                # estimated or missing pulse; we keep daily_consumption_m3 as NULL sometimes
                daily_cons = None
                reading_value = round(last_value + random.uniform(0.0, 3.0), 2)

            is_estimated = 1 if daily_cons is None or random.random() < 0.15 else 0
            flag = random.choices(
                ["ok", "outlier", "missing_pulse", "rollover"],
                weights=[0.8, 0.05, 0.1, 0.05],
                k=1
            )[0]

            readings_to_insert.append((
                reading_id, meter_id, reading_date,
                reading_value, daily_cons, is_estimated, flag
            ))

            reading_id += 1
            last_value = reading_value
            start_date = reading_date + timedelta(days=1)

    # If we overshoot target_readings, just truncate list
    readings_to_insert = readings_to_insert[:target_readings]

    cur.executemany("""
        INSERT INTO meter_readings
        (reading_id, meter_id, reading_date, reading_value_m3,
         daily_consumption_m3, is_estimated, data_quality_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, readings_to_insert)

    conn.commit()


# ### 4.7. Seed Leak Events
# 
# `seed_leak_events()` populates the `leak_events` table with simulated leak incidents. About 60% of leaks are tied to a specific meter, while others are zone-wide. It generates detection times, severity levels, estimated water loss, leak locations, and statuses (open, in_progress, resolved, false_alarm).

# In[38]:


def seed_leak_events(conn, n_leaks=200):
    cur = conn.cursor()
    cur.execute("SELECT zone_id FROM water_zones")
    zones = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT meter_id, zone_id FROM meters")
    meter_zone_rows = cur.fetchall()

    locations = ["Street", "Chamber", "CustomerPipe", "MainPipe", "Valve"]

    for leak_id in range(1, n_leaks + 1):
        # 60% tied to a meter, 40% only at zone level
        if random.random() > 0.4 and meter_zone_rows:
            meter_id, zone_id = random.choice(meter_zone_rows)
        else:
            meter_id = None
            zone_id = random.choice(zones)

        detected = fake.date_time_between(start_date="-180d", end_date="now")
        severity = random.randint(1, 5)
        estimated_loss = round(random.uniform(0.1, 500.0), 1)
        loc = random.choice(locations)
        status = random.choices(
            ["open", "in_progress", "resolved", "false_alarm"],
            weights=[0.25, 0.25, 0.4, 0.1],
            k=1
        )[0]

        if status == "resolved":
            resolved_time = detected + timedelta(hours=random.randint(1, 72))
        else:
            resolved_time = None

        cur.execute("""
            INSERT INTO leak_events
            (leak_id, meter_id, zone_id, detected_time, severity_level,
             estimated_loss_m3, leak_location, status, resolved_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (leak_id, meter_id, zone_id, detected,
              severity, estimated_loss, loc, status,
              resolved_time))

    conn.commit()


# ### 4.8. Seed Zone Daily Stats
# 
# `seed_zone_daily_stats()` generates daily statistics for each water zone. For a specified number of days, it simulates total water input, total billed water, and calculates non-revenue water (NRW) based on these figures.

# In[39]:


def seed_zone_daily_stats(conn, n_days=60):
    cur = conn.cursor()
    cur.execute("SELECT zone_id FROM water_zones")
    zones = [row[0] for row in cur.fetchall()]

    end_date = date(2024, 3, 31)
    start_date = end_date - timedelta(days=n_days - 1)

    stats_rows = []

    for zone_id in zones:
        current_date = start_date
        for _ in range(n_days):
            total_input = round(random.uniform(500, 5000), 1)
            total_billed = round(total_input * random.uniform(0.6, 0.95), 1)
            nrw = round(max(0.0, total_input - total_billed), 1)

            stats_rows.append((zone_id, current_date, total_input, total_billed, nrw))
            current_date += timedelta(days=1)

    cur.executemany("""
        INSERT INTO zone_daily_stats
        (zone_id, stat_date, total_input_m3, total_billed_m3, non_revenue_water_m3)
        VALUES (?, ?, ?, ?, ?)
    """, stats_rows)
    conn.commit()


# ## 5. Execute Data Generation
# 
# This block orchestrates the entire data generation process. It first calls `apply_schema()` to create the database tables, and then sequentially calls each `seed_*` function to populate the tables with synthetic data. Finally, it closes the database connection.

# In[40]:


conn = get_connection()

apply_schema()

seed_water_zones(conn, n_zones=10)
seed_reservoirs(conn)
seed_customers(conn, n_customers=350)
seed_meters(conn)
seed_bills(conn)
seed_meter_readings(conn, target_readings=1500)
seed_leak_events(conn, n_leaks=200)
seed_zone_daily_stats(conn, n_days=60)

conn.close()
print("Data generation completed for attaullah_water_system.db")


# ## 6. Verify Data Generation
# 
# This final block connects to the database, queries each table to count the number of rows, and prints these counts to verify that data has been successfully inserted. It also displays sample rows from `meter_readings` and `bills` for a quick data preview.

# In[41]:


conn = get_connection()
cur = conn.cursor()

tables = [
    "water_zones", "reservoirs", "customers", "meters",
    "bills", "meter_readings", "leak_events", "zone_daily_stats"
]

for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"{t}: {cur.fetchone()[0]} rows")

print("\nSample meter_readings:")
for row in cur.execute("SELECT * FROM meter_readings LIMIT 5"):
    print(row)

print("\nSample bills:")
for row in cur.execute("SELECT * FROM bills LIMIT 5"):
    print(row)

conn.close()

