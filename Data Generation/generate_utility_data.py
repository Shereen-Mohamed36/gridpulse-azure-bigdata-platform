#!/usr/bin/env python3
"""
Smart Utility Intelligence Platform - Synthetic Data Generator with Azure Event Hubs Integration

Generates 9 tables: regions, assets, customers, meters, asset_operations_hourly,
meter_readings, weather_hourly, operational_events, billing_monthly
+ anomaly_log.csv (ground truth of every injected anomaly) + generation_summary.json
+ Optional real-time streaming to Azure Event Hubs.

Examples:
  python generate_utility_data.py --size small
  python generate_utility_data.py --size medium --stream-azure
  python generate_utility_data.py --customers 300 --days 30 --interval 60 --stream-azure
"""
import argparse
import json
import os
import sys
import numpy as np
import pandas as pd


try:
  from azure.eventhub import EventData, EventHubProducerClient

  AZURE_AVAILABLE = True
except ImportError:
  AZURE_AVAILABLE = False

PRESETS = {
    "small": dict(regions=5, assets=40, customers=200, days=7),
    "medium": dict(regions=12, assets=200, customers=2000, days=30),
    "large": dict(regions=20, assets=1000, customers=20000, days=30),
}
GOVS = [
    ("Greater Cairo", "Cairo"),
    ("Alexandria", "Alexandria"),
    ("Giza", "Giza"),
    ("Port Said", "Port Said"),
    ("Suez", "Suez"),
    ("Luxor", "Luxor"),
    ("Aswan", "Aswan"),
    ("Sohag", "Sohag"),
    ("Assiut", "Assiut"),
    ("Mansoura", "Dakahlia"),
    ("Tanta", "Gharbia"),
    ("Ismailia", "Ismailia"),
]
LOG = []


def n_anom(rng, rows, rate):
  """Probabilistic rounding so tiny tables still average out to `rate`."""
  x = rows * rate
  f = int(np.floor(x))
  return f + int(rng.random() < x - f)


def log(table, keys, atype, details=""):
  keys = list(keys)
  if keys:
    LOG.append(
        pd.DataFrame({
            "table": table,
            "record_key": keys,
            "anomaly_type": atype,
            "details": details,
        })
    )


# ----------------------------------------------------------------- regions
def gen_regions(rng, n):
  rows = []
  for i in range(n):
    name, gov = GOVS[i % len(GOVS)]
    if i >= len(GOVS):
      name = f"{name} {i // len(GOVS) + 1}"
    rows.append(
        dict(
            region_id=f"REG{i+1:03d}",
            region_name=name,
            governorate=gov,
            country="Egypt",
            latitude=round(rng.uniform(22, 31.5), 4),
            longitude=round(rng.uniform(25, 34.5), 4),
            population=int(rng.integers(150_000, 9_000_000)),
            area_km2=round(rng.uniform(50, 3000), 1),
            timezone="Africa/Cairo",
        )
    )
  return pd.DataFrame(rows)


# ------------------------------------------------------------------ assets
def gen_assets(rng, n, regions):
  n = max(n, 8)
  n_pl, n_sub, n_line = (
      max(2, int(n * 0.08)),
      max(2, int(n * 0.12)),
      max(1, int(n * 0.15)),
  )
  n_tr = max(1, n - n_pl - n_sub - n_line)
  rid = regions.region_id.values
  counter = [0]

  def common(k, atype, prefix):
    ids = [f"AST{counter[0]+i+1:05d}" for i in range(k)]
    counter[0] += k
    return pd.DataFrame({
        "asset_id": ids,
        "asset_name": [f"{prefix} {i+1}" for i in range(k)],
        "asset_type": atype,
        "region_id": rng.choice(rid, k),
        "operational_status": rng.choice(
            ["In Service", "Maintenance", "Standby"], k, p=[.9, .05, .05]
        ),
        "commissioning_date": (
            pd.Timestamp("1995-01-01")
            + pd.to_timedelta(rng.integers(0, 10500, k), unit="D")
        )
        .date.astype(str),
        "latitude": rng.uniform(22, 31.5, k).round(4),
        "longitude": rng.uniform(25, 34.5, k).round(4),
    })

  sub = common(n_sub, "Substation", "Substation")
  sub["voltage_level_kv"] = rng.choice([132, 66, 33], n_sub)
  sub["capacity_mva"] = rng.choice([40, 63, 125, 250], n_sub)
  sub_ids = sub.asset_id.values
  sub_region = dict(zip(sub.asset_id, sub.region_id))

  pl = common(n_pl, "PowerPlant", "Plant")
  pl["plant_type"] = rng.choice(
      ["Gas", "Solar", "Wind", "Hydro"], n_pl, p=[.4, .3, .2, .1]
  )
  src = {
      "Gas": ("SRC-GAS", "Natural Gas", "Gas", False),
      "Solar": ("SRC-SUN", "Solar", "None", True),
      "Wind": ("SRC-WND", "Wind", "None", True),
      "Hydro": ("SRC-HYD", "Hydro", "None", True),
  }
  pl["installed_capacity_mw"] = rng.uniform(20, 600, n_pl).round(1)
  for j, c in enumerate([
      "primary_source_id",
      "primary_source_name",
      "fuel_type",
      "is_renewable",
  ]):
    pl[c] = [src[t][j] for t in pl.plant_type]

  tr = common(n_tr, "Transformer", "Transformer")
  tr["parent_asset_id"] = rng.choice(sub_ids, n_tr)
  tr["region_id"] = tr.parent_asset_id.map(sub_region)
  tr["voltage_level_kv"] = 11
  tr["capacity_mva"] = rng.choice(
      [0.4, 0.63, 1.0, 1.6, 2.5, 5.0, 10.0], n_tr
  )

  ln = common(n_line, "TransmissionLine", "Line")
  frm = rng.choice(len(sub_ids), n_line)
  to = np.where(
      rng.choice(len(sub_ids), n_line) == frm,
      (frm + 1) % len(sub_ids),
      rng.choice(len(sub_ids), n_line),
  )
  to = np.where(to == frm, (frm + 1) % len(sub_ids), to)
  ln["from_substation_id"], ln["to_substation_id"] = sub_ids[frm], sub_ids[to]
  ln["region_id"] = ln.from_substation_id.map(sub_region)
  ln["voltage_level_kv"] = rng.choice([132, 66, 33], n_line)

  cols = [
      "asset_id",
      "asset_name",
      "asset_type",
      "plant_type",
      "region_id",
      "operational_status",
      "commissioning_date",
      "latitude",
      "longitude",
      "parent_asset_id",
      "from_substation_id",
      "to_substation_id",
      "voltage_level_kv",
      "capacity_mva",
      "installed_capacity_mw",
      "primary_source_id",
      "primary_source_name",
      "fuel_type",
      "is_renewable",
  ]
  return (
      pd.concat([sub, pl, tr, ln], ignore_index=True)
      .reindex(columns=cols)
  )


# ------------------------------------------------------ customers + meters
def gen_customers_meters(rng, n, regions, assets, rate):
  rid = regions.region_id.values
  ctype = rng.choice(["Residential", "Commercial", "Industrial"], n, p=[.82, .15, .03])
  cap = np.select(
      [ctype == "Residential", ctype == "Commercial"],
      [rng.uniform(3, 15, n), rng.uniform(15, 100, n)],
      rng.uniform(100, 2000, n),
  ).round(1)
  cust = pd.DataFrame({
      "customer_id": [f"CUS{i+1:07d}" for i in range(n)],
      "customer_type": ctype,
      "region_id": rng.choice(rid, n),
      "contracted_capacity_kw": cap,
      "connection_date": (
          pd.Timestamp("2005-01-01")
          + pd.to_timedelta(rng.integers(0, 7000, n), unit="D")
      )
      .date.astype(str),
      "tariff_plan": np.select(
          [ctype == "Residential", ctype == "Commercial"],
          ["Residential-Tiered", "Commercial-Flat"],
          "Industrial-TOU",
      ),
      "account_status": rng.choice(
          ["Active", "Suspended", "Closed"], n, p=[.95, .03, .02]
      ),
  })

  per = rng.choice([1, 2], n, p=[.92, .08])
  m = len(np.repeat(np.arange(n), per))
  ci = np.repeat(np.arange(n), per)
  three = (ctype[ci] != "Residential") | (rng.random(m) < 0.05)
  tr = assets[assets.asset_type == "Transformer"]
  tr_by_region = {r: g.asset_id.values for r, g in tr.groupby("region_id")}
  all_tr = tr.asset_id.values
  creg = cust.region_id.values[ci]
  trans = np.empty(m, dtype=object)
  for r in np.unique(creg):
    mask = creg == r
    trans[mask] = rng.choice(tr_by_region.get(r, all_tr), mask.sum())
  meters = pd.DataFrame({
      "meter_id": [f"MTR{i+1:07d}" for i in range(m)],
      "customer_id": cust.customer_id.values[ci],
      "meter_type": rng.choice(["Smart", "AMI"], m),
      "meter_installation_date": (
          pd.Timestamp("2018-01-01")
          + pd.to_timedelta(rng.integers(0, 2500, m), unit="D")
      )
      .date.astype(str),
      "rated_voltage_v": np.where(three, 400, 230),
      "meter_status": rng.choice(["Active", "Inactive"], m, p=[.97, .03]),
      "transformer_id": trans,
      "network_id": [f"NET{x:02d}" for x in rng.integers(1, 20, m)],
      "phase_type": np.where(three, "Three-Phase", "Single-Phase"),
      "communication_type": rng.choice(
          ["GPRS", "RF Mesh", "PLC", "NB-IoT"], m
      ),
  })

  k = n_anom(rng, n, rate)
  if k:
    idx = rng.choice(n, k, replace=False)
    half = len(idx) // 2
    cust.loc[idx[:half], "contracted_capacity_kw"] = (
        -1 * cust.loc[idx[:half], "contracted_capacity_kw"]
    )
    cust.loc[idx[half:], "region_id"] = np.nan
    log("customers", cust.customer_id.values[idx[:half]], "negative_capacity")
    log("customers", cust.customer_id.values[idx[half:]], "missing_region")
  k = n_anom(rng, m, rate)
  if k:
    idx = rng.choice(m, k, replace=False)
    half = len(idx) // 2
    meters.loc[idx[:half], "transformer_id"] = "AST_UNKNOWN"
    meters.loc[idx[half:], "meter_status"] = "Faulty"
    log("meters", meters.meter_id.values[idx[:half]], "orphan_transformer_fk")
    log("meters", meters.meter_id.values[idx[half:]], "faulty_meter_status")
  return cust, meters


# ----------------------------------------------------------------- weather
def gen_weather(rng, regions, hours, start, rate):
  R = len(regions)
  hi = np.tile(np.arange(hours), (R, 1))
  hod = hi % 24
  base = rng.uniform(20, 30, R)[:, None]
  temp = (
      base
      + 7 * np.sin(2 * np.pi * (hod - 9) / 24)
      + rng.normal(0, 1.5, (R, hours))
  )
  clouds = rng.uniform(0.6, 1, (R, hours))
  irr = np.maximum(0, 950 * np.sin(np.pi * (hod - 6) / 12)) * clouds
  wind = rng.gamma(2, 2, (R, hours))
  hum = np.clip(
      70 - 1.5 * (temp - 25) + rng.normal(0, 5, (R, hours)), 15, 100
  )
  clean = dict(temp=temp.copy(), irr=irr.copy(), wind=wind.copy())

  N = R * hours
  t, ir, hm = temp.ravel().copy(), irr.ravel().copy(), hum.ravel().copy()
  k = n_anom(rng, N, rate)
  idx = rng.choice(N, k, replace=False) if k else np.array([], dtype=int)
  parts = np.array_split(idx, 3)
  t[parts[0]] = rng.choice([-20, 65], len(parts[0]))
  ir[parts[1]] = rng.uniform(300, 800, len(parts[1]))
  hm[parts[2]] = np.nan
  cond = np.select(
      [clouds.ravel() > 0.85, clouds.ravel() > 0.7],
      ["Clear", "Partly Cloudy"],
      "Cloudy",
  )
  ts = pd.Timestamp(start) + pd.to_timedelta(
      np.tile(np.arange(hours), R), unit="h"
  )
  df = pd.DataFrame({
      "weather_record_id": [f"W{i+1:08d}" for i in range(N)],
      "region_id": np.repeat(regions.region_id.values, hours),
      "timestamp": ts,
      "temperature_c": t.round(1),
      "solar_irradiance_w_m2": ir.round(1),
      "wind_speed_m_s": wind.ravel().round(1),
      "humidity_pct": hm.round(1),
      "weather_condition": cond,
  })
  keys = df.weather_record_id.values
  log("weather_hourly", keys[parts[0]], "temperature_out_of_range")
  log("weather_hourly", keys[parts[1]], "irradiance_impossible")
  log("weather_hourly", keys[parts[2]], "missing_humidity")
  return df, clean, N, k


# ------------------------------------------------------- asset operations
def gen_asset_ops(rng, assets, regions, hours, start, W, rate):
  a = (
      assets[assets.asset_type.isin(["PowerPlant", "Transformer"])]
      .reset_index(drop=True)
  )
  A = len(a)
  N = A * hours
  rmap = {r: i for i, r in enumerate(regions.region_id)}
  ai, hi = np.repeat(np.arange(A), hours), np.tile(np.arange(hours), A)
  hod = hi % 24
  ri = a.region_id.map(rmap).values[ai]
  temp, irr, wind = W["temp"][ri, hi], W["irr"][ri, hi], W["wind"][ri, hi]
  is_tr = (a.asset_type.values == "Transformer")[ai]
  cap = np.where(
      a.asset_type == "Transformer",
      a.capacity_mva,
      a.installed_capacity_mw,
  ).astype(float)[ai]
  ptype = a.plant_type.fillna("").values[ai]

  loading = np.where(
      is_tr,
      np.clip(
          rng.normal(62, 8, N)
          + 18 * np.sin(2 * np.pi * (hod - 15) / 24)
          + 0.6 * np.maximum(temp - 30, 0),
          20,
          98,
      ),
      np.nan,
  )
  oil = np.where(
      is_tr,
      35 + 0.45 * loading + 0.3 * (temp - 25) + rng.normal(0, 2, N),
      np.nan,
  )
  util = np.select(
      [ptype == "Solar", ptype == "Wind"],
      [irr / 1000 * 0.85, np.clip((wind / 12) ** 3, 0, 1)],
      rng.uniform(0.5, 0.9, N),
  )
  avail = np.where(~is_tr, cap * rng.uniform(0.92, 1, N), np.nan)
  gen = np.where(~is_tr, np.minimum(cap * util, avail), np.nan)
  status = np.full(N, "Normal", dtype=object)
  notes = np.full(N, "", dtype=object)

  k = n_anom(rng, N, rate)
  idx = rng.choice(N, k, replace=False) if k else np.array([], dtype=int)
  rid_all = np.array([f"OP{i+1:09d}" for i in range(N)]) if k else None
  t_idx, p_idx = idx[is_tr[idx]], idx[~is_tr[idx]]
  ov, hot = t_idx[: int(len(t_idx) * 0.6)], t_idx[int(len(t_idx) * 0.6) :]
  loading[ov] = rng.uniform(105, 150, len(ov))
  oil[ov] = 35 + 0.45 * loading[ov] + 10
  oil[hot] = rng.uniform(95, 120, len(hot))
  status[ov], notes[ov] = "Overloaded", "Loading above rated capacity"
  status[hot], notes[hot] = "Warning", "High oil temperature"
  gen[p_idx], avail[p_idx] = 0.0, 0.0
  status[p_idx], notes[p_idx] = "Outage", "Unplanned outage"
  if k:
    log(
        "asset_operations_hourly",
        rid_all[ov],
        "transformer_overload",
        "loading_pct 105-150",
    )
    log(
        "asset_operations_hourly",
        rid_all[hot],
        "transformer_overheating",
        "oil_temperature 95-120C",
    )
    log(
        "asset_operations_hourly",
        rid_all[p_idx],
        "plant_outage",
        "generation forced to 0",
    )

  ts = pd.Timestamp(start) + pd.to_timedelta(hi, unit="h")
  df = pd.DataFrame({
      "record_id": [f"OP{i+1:09d}" for i in range(N)],
      "asset_id": a.asset_id.values[ai],
      "timestamp": ts,
      "generated_energy_mwh": np.round(gen, 2),
      "available_capacity_mw": np.round(avail, 2),
      "capacity_utilization_ratio": np.where(
          avail > 0,
          np.round(gen / np.where(avail > 0, avail, 1), 3),
          np.nan,
      ),
      "load_kva": np.where(
          is_tr, np.round(loading / 100 * cap * 1000, 1), np.nan
      ),
      "loading_pct": np.round(loading, 1),
      "oil_temperature_c": np.round(oil, 1),
      "power_factor": np.where(
          is_tr, np.clip(rng.normal(0.93, 0.02, N), 0.8, 1).round(3), np.nan
      ),
      "operating_status": status,
      "overload_flag": np.where(is_tr, loading > 100, False),
      "notes": notes,
  })
  return df, (ov, hot, p_idx), N, k


# ------------------------------------------------------------ meter readings
def gen_readings(
    rng, meters, cust, regions, hours, start, W, rate, interval_mode="mixed"
):
  M = len(meters)
  days = hours // 24
  if interval_mode == "mixed":
    interval_m = rng.choice([15, 60], M, p=[.5, .5])
  else:
    interval_m = np.full(M, int(interval_mode))
  counts = days * (1440 // interval_m)
  N = int(counts.sum())
  mi = np.repeat(np.arange(M), counts)
  within = np.arange(N) - np.repeat(np.cumsum(counts) - counts, counts)
  interval = interval_m[mi]
  minutes = within * interval
  hi = (minutes // 60).astype(int)
  hod = hi % 24

  cmap = cust.set_index("customer_id")
  ctype = cmap.customer_type.reindex(meters.customer_id).values
  creg = (
      cmap.region_id.reindex(meters.customer_id)
      .fillna(regions.region_id.iloc[0])
      .values
  )
  rmap = {r: i for i, r in enumerate(regions.region_id)}
  ri = np.array([rmap[r] for r in creg])[mi]
  base = np.select(
      [ctype == "Residential", ctype == "Commercial"],
      [rng.lognormal(-0.4, 0.5, M), rng.lognormal(1.6, 0.5, M)],
      rng.lognormal(3.5, 0.4, M),
  )[mi]
  temp = W["temp"][ri, hi]
  kw = (
      base
      * (1 + 0.5 * np.sin(2 * np.pi * (hod - 14) / 24))
      * (1 + 0.03 * np.maximum(temp - 25, 0))
      * rng.lognormal(0, 0.12, N)
  )
  nominal = meters.rated_voltage_v.values[mi].astype(float)
  volt = nominal * (1 + rng.normal(0, 0.01, N))
  pf = np.clip(rng.normal(0.92, 0.03, N), 0.75, 1)
  quality = np.full(N, "Good", dtype=object)
  note = np.full(N, "", dtype=object)
  kwh_nan = np.zeros(N, dtype=bool)

  k = n_anom(rng, N, rate)
  idx = rng.choice(N, k, replace=False) if k else np.array([], dtype=int)
  g_volt, g_spike, g_poor, g_miss = np.array_split(idx, 4)
  volt[g_volt] = nominal[g_volt] * rng.choice(
      [0.80, 0.82, 1.15, 1.20], len(g_volt)
  )
  note[g_volt] = "Voltage excursion"
  kw[g_spike] = kw[g_spike] * rng.uniform(3, 8, len(g_spike))
  note[g_spike] = "Consumption surge"
  quality[g_poor] = rng.choice(
      ["Estimated", "Suspect", "Invalid"], len(g_poor)
  )
  kwh_nan[g_poor[: len(g_poor) // 2]] = True

  rid = np.array([f"RD{i+1:010d}" for i in range(N)])
  ts = pd.Timestamp(start) + pd.to_timedelta(minutes, unit="m")
  mid = meters.meter_id.values[mi]
  kvar = kw * np.tan(np.arccos(pf))
  phase = np.where(
      meters.phase_type.values[mi] == "Three-Phase", np.sqrt(3), 1.0
  )
  df = pd.DataFrame({
      "reading_id": rid,
      "meter_id": mid,
      "timestamp": ts,
      "interval_minutes": interval,
      "energy_consumption_kwh": np.where(
          kwh_nan, np.nan, (kw * interval / 60).round(4)
      ),
      "active_power_kw": kw.round(3),
      "reactive_energy_kvarh": (kvar * interval / 60).round(4),
      "reactive_power_kvar": kvar.round(3),
      "voltage_v": volt.round(1),
      "current_a": (kw * 1000 / (volt * pf * phase)).round(2),
      "power_factor": pf.round(3),
      "reading_quality": quality,
      "event_note": note,
  })
  if k:
    log(
        "meter_readings",
        rid[g_volt],
        "voltage_anomaly",
        "voltage +-15-20% of nominal",
    )
    log("meter_readings", rid[g_spike], "abnormal_consumption", "x3-x8 spike")
    log("meter_readings", rid[g_poor], "poor_reading_quality")
    log(
        "meter_readings",
        [f"{mid[i]}|{ts[i]}|{interval[i]}" for i in g_miss],
        "missing_reading",
        "row removed from output",
    )
  df = df.drop(df.index[g_miss]).reset_index(drop=True)
  return df, N, k


# ------------------------------------------------------------------ billing
def gen_billing(rng, cust, meters, readings, rate):
  prim = meters.drop_duplicates("customer_id")[["meter_id", "customer_id"]]
  r = readings[readings.meter_id.isin(prim.meter_id)].copy()
  r["billing_month"] = r.timestamp.dt.strftime("%Y-%m")
  b = r.groupby(["meter_id", "billing_month"], as_index=False).energy_consumption_kwh.sum()
  b = b.merge(prim, on="meter_id").merge(
      cust[["customer_id", "customer_type"]], on="customer_id"
  )
  n = len(b)
  rt = np.select(
      [b.customer_type == "Residential", b.customer_type == "Commercial"],
      [rng.uniform(0.9, 2.2, n), rng.uniform(1.8, 2.6, n)],
      rng.uniform(1.5, 2.2, n),
  ).round(3)
  fixed = np.select(
      [b.customer_type == "Residential", b.customer_type == "Commercial"],
      [10.0, 40.0],
      150.0,
  )
  energy = b.energy_consumption_kwh.round(2).to_numpy().copy()
  est = np.zeros(n, dtype=bool)
  amount = energy * rt + fixed
  pay = rng.choice(
      ["Paid", "Unpaid", "Overdue", "Partial"], n, p=[.8, .1, .07, .03]
  ).astype(object)

  k = n_anom(rng, n, rate)
  idx = rng.choice(n, k, replace=False) if k else np.array([], dtype=int)
  g1, g2, g3 = np.array_split(idx, 3)
  energy[g1] = energy[g1] * rng.uniform(0.4, 0.7, len(g1))
  est[g1] = True
  amount[g1] = energy[g1] * rt[g1] + fixed[g1]
  amount[g2] = amount[g2] * rng.uniform(1.3, 2.0, len(g2))
  pay[g3] = None
  df = pd.DataFrame({
      "bill_id": [f"BILL{i+1:08d}" for i in range(n)],
      "customer_id": b.customer_id.values,
      "meter_id": b.meter_id.values,
      "billing_month": b.billing_month.values,
      "billed_energy_kwh": energy,
      "applied_rate_egp_per_kwh": rt,
      "fixed_charge_egp": fixed,
      "amount_egp": amount.round(2),
      "estimated_flag": est,
      "payment_status": pay,
  })
  ids = df.bill_id.values
  log(
      "billing_monthly",
      ids[g1],
      "estimated_bill_mismatch",
      "billed 40-70% of metered",
  )
  log("billing_monthly", ids[g2], "amount_calculation_error")
  log("billing_monthly", ids[g3], "missing_payment_status")
  return df, n, k


# ------------------------------------------------------------------- events
def gen_events(rng, assets, meters, ops, ov_idx, hours, start, rate):
  n_norm = max(5, int(len(assets) * (hours / 24) * 0.02))
  norm = assets.sample(
      n_norm, replace=True, random_state=int(rng.integers(1e9))
  )
  st = pd.Timestamp(start) + pd.to_timedelta(
      rng.integers(0, hours * 60, n_norm), unit="m"
  )
  dur = rng.integers(5, 600, n_norm)
  rows = pd.DataFrame({
      "event_level": "asset",
      "entity_id": norm.asset_id.values,
      "region_id": norm.region_id.values,
      "event_type": rng.choice(
          [
              "Line Outage",
              "Cable Fault",
              "Transformer Trip",
              "Voltage Dip",
              "Planned Maintenance",
          ],
          n_norm,
      ),
      "severity": rng.choice(
          ["Low", "Medium", "High", "Critical"], n_norm, p=[.4, .35, .2, .05]
      ),
      "start_timestamp": st,
      "end_timestamp": st + pd.to_timedelta(dur, unit="m"),
      "duration_minutes": dur,
      "affected_customers": rng.integers(0, 3000, n_norm),
      "cause_or_code": rng.choice(["E101", "E204", "W310", "M001"], n_norm),
      "event_status": rng.choice(["Closed", "Open"], n_norm, p=[.9, .1]),
      "parent_event_id": None,
      "anomaly_id": None,
      "anomaly_type": None,
      "injection_parameters": None,
      "expected_detection": None,
      "notes": "",
  })
  k = n_anom(rng, n_norm, rate)
  if k:
    i = rng.choice(n_norm, k, replace=False)
    rows.loc[rows.index[i], "end_timestamp"] = (
        rows.loc[rows.index[i], "start_timestamp"] - pd.Timedelta(minutes=30)
    )
  bad_idx = list(rows.index[i]) if k else []

  ov = ops.iloc[ov_idx]
  syn = None
  if len(ov):
    ov = ov.assign(day=ov.timestamp.dt.date)
    g = (
        ov.groupby(["asset_id", "day"])
        .agg(s=("timestamp", "min"), e=("timestamp", "max"), mx=("loading_pct", "max"))
        .reset_index()
    )
    reg = assets.set_index("asset_id").region_id
    syn = pd.DataFrame({
        "event_level": "asset",
        "entity_id": g.asset_id,
        "region_id": g.asset_id.map(reg),
        "event_type": "Transformer Overload",
        "severity": "High",
        "start_timestamp": g.s,
        "end_timestamp": g.e + pd.Timedelta(hours=1),
        "duration_minutes": (
            (g.e - g.s).dt.total_seconds() // 60 + 60
        ).astype(int),
        "affected_customers": rng.integers(50, 2000, len(g)),
        "cause_or_code": "OVL01",
        "event_status": "Closed",
        "parent_event_id": None,
        "anomaly_id": [f"ANM{i+1:06d}" for i in range(len(g))],
        "anomaly_type": "transformer_overload",
        "injection_parameters": [
            json.dumps({"max_loading_pct": float(x)}) for x in g.mx
        ],
        "expected_detection": "loading_pct > 100",
        "notes": "Synthetic scenario",
    })
  ev = pd.concat([rows, syn] if syn is not None else [rows], ignore_index=True)
  ev.insert(0, "event_id", [f"EVT{i+1:07d}" for i in range(len(ev))])
  if bad_idx:
    log("operational_events", ev.event_id.values[bad_idx], "end_before_start")
  return ev, n_norm, k


# ----------------------------------------------------------------- Azure Stream Function
def stream_to_azure_event_hub(df):
 
  if not AZURE_AVAILABLE:
    print(
        "\n[Warning] azure-eventhub package is not installed."
        " Run: pip install azure-eventhub"
    )
    return

  
  connection_str = ""
  event_hub_name = "gridpulse-telemetry"

  print("\nStreaming meter readings to Azure Event Hubs...")
  try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_str, eventhub_name=event_hub_name
    )
    batch = producer.create_batch()
    count = 0

    # نرسل عينة أو البيانات كلها تدفقياً
    for _, row in df.iterrows():
      event_data = EventData(
          row.to_json()
      )  # تحويل كل صف لـ JSON وإرساله كـ Event
      try:
        batch.add(event_data)
      except ValueError:
        producer.send_batch(batch)
        batch = producer.create_batch()
        batch.add(event_data)

      count += 1
      if count % 5000 == 0:
        print(f"-> Streamed {count:,} records to Azure Event Hubs so far...")

    if len(batch) > 0:
      producer.send_batch(batch)
    producer.close()
    print(
        f"Successfully streamed total of {count:,} records to Azure Event"
        " Hubs! 🚀"
    )
  except Exception as e:
    print(f"Error streaming to Azure Event Hubs: {e}")


# --------------------------------------------------------------------- main
def parse():
  p = argparse.ArgumentParser(description="Smart Utility synthetic data generator")
  p.add_argument(
      "--size", choices=PRESETS, default="small", help="preset size (default: small)"
  )
  p.add_argument("--regions", type=int)
  p.add_argument("--assets", type=int)
  p.add_argument("--customers", type=int)
  p.add_argument("--days", type=int)
  p.add_argument(
      "--anomaly-rate",
      type=float,
      default=0.015,
      help="between 0.01 and 0.02 (default 0.015)",
  )
  p.add_argument("--start", default="2025-01-01")
  p.add_argument("--seed", type=int, default=42)
  p.add_argument("--out", default="output_data")
  p.add_argument("--excel", action="store_true")
  p.add_argument(
      "--interval",
      choices=["mixed", "15", "60"],
      default="mixed",
      help=(
          "reading interval in minutes: mixed (50/50 of 15 and 60, default),"
          " 15, or 60"
      ),
  )
  p.add_argument(
      "--max-rows",
      type=int,
      default=20_000_000,
      help="safety limit on meter_readings rows",
  )
  p.add_argument("--force", action="store_true", help="ignore --max-rows")
  p.add_argument(
      "--stream-azure",
      action="store_true",
      help="Stream generated meter readings live to Azure Event Hubs",
  )
  a = p.parse_args()
  for k, v in PRESETS[a.size].items():
    if getattr(a, k) is None:
      setattr(a, k, v)
  if not 0.01 <= a.anomaly_rate <= 0.02:
    p.error("--anomaly-rate must be between 0.01 and 0.02")
  return a


def main():
  a = parse()
  rng = np.random.default_rng(a.seed)
  hours = a.days * 24
  per_day = {"mixed": 60, "15": 96, "60": 24}[a.interval]
  est = int(a.customers * 1.08 * a.days * per_day)
  print(f"Estimated meter_readings rows: ~{est:,}")
  if est > a.max_rows and not a.force:
    sys.exit("Too many rows. Reduce --customers/--days or pass --force.")
  os.makedirs(a.out, exist_ok=True)

  regions = gen_regions(rng, a.regions)
  assets = gen_assets(rng, a.assets, regions)
  cust, meters = gen_customers_meters(rng, a.customers, regions, assets, a.anomaly_rate)
  weather, W, nW, kW = gen_weather(rng, regions, hours, a.start, a.anomaly_rate)
  ops, (ov, hot, outg), nO, kO = gen_asset_ops(
      rng, assets, regions, hours, a.start, W, a.anomaly_rate
  )
  readings, nR, kR = gen_readings(
      rng, meters, cust, regions, hours, a.start, W, a.anomaly_rate, a.interval
  )
  billing, nB, kB = gen_billing(rng, cust, meters, readings, a.anomaly_rate)
  events, nE, kE = gen_events(
      rng, assets, meters, ops, ov, hours, a.start, a.anomaly_rate
  )

  tables = {
      "regions": regions,
      "assets": assets,
      "customers": cust,
      "meters": meters,
      "asset_operations_hourly": ops,
      "meter_readings": readings,
      "weather_hourly": weather,
      "operational_events": events,
      "billing_monthly": billing,
  }
  anomalies = (
      pd.concat(LOG, ignore_index=True)
      if LOG
      else pd.DataFrame(
          columns=["table", "record_key", "anomaly_type", "details"]
      )
  )
  tables["anomaly_log"] = anomalies

  summary = {}
  base_rows = {
      "weather_hourly": nW,
      "asset_operations_hourly": nO,
      "meter_readings": nR,
      "billing_monthly": nB,
      "operational_events": nE,
  }
  for name, df in tables.items():
    df.to_csv(os.path.join(a.out, f"{name}.csv"), index=False)
    entry = {"rows": len(df)}
    if name in base_rows:
      c = int((anomalies.table == name).sum())
      entry["anomalies"] = c
      entry["anomaly_pct"] = round(100 * c / max(base_rows[name], 1), 3)
    summary[name] = entry
  with open(
      os.path.join(a.out, "generation_summary.json"), "w", encoding="utf-8"
  ) as f:
    json.dump({"params": vars(a), "tables": summary}, f, indent=2, default=str)

  if a.excel:
    path = os.path.join(a.out, "utility_data.xlsx")
    with pd.ExcelWriter(path) as xw:
      for name, df in tables.items():
        if len(df) > 1_048_575:
          print(f"Skipping {name} in Excel (too many rows, CSV is available)")
          continue
        df.to_excel(xw, sheet_name=name[:31], index=False)

  print(f"\nDone -> {os.path.abspath(a.out)}")
  for n_, e in summary.items():
    extra = (
        f"  anomalies={e['anomalies']} ({e['anomaly_pct']}%)"
        if "anomalies" in e
        else ""
    )
    print(f"  {n_:26s} {e['rows']:>12,} rows{extra}")

  
  if a.stream_azure:
    stream_to_azure_event_hub(readings)


if __name__ == "__main__":
  main()