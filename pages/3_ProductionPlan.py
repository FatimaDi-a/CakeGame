#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production Plan Page — Round-Based Version
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime
import math
import pytz

BEIRUT_TZ = pytz.timezone("Asia/Beirut")

# =====================================
# 🌍 SUPABASE SETUP
# =====================================
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("❌ Missing Supabase credentials.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
st.set_page_config(page_title="Production Plan", page_icon="🍰", layout="wide")


def submissions_locked(supabase_client):
    lock_row = (
        supabase_client.table("game_state")
        .select("value")
        .eq("key", "locked")
        .single()
        .execute()
    )
    return lock_row.data and lock_row.data["value"] == "true"


def get_current_round():
    resp = (
        supabase.table("game_state")
        .select("value")
        .eq("key", "current_round")
        .single()
        .execute()
    )
    return int(resp.data["value"])


# =====================================
# 🔒 LOGIN CHECK
# =====================================
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Please log in first.")
    st.stop()


# =====================================
# 📌 Round System
# =====================================
current_round = get_current_round()
team = st.session_state.team_name

submitted_rounds_resp = (
    supabase.table("production_plans")
    .select("round_number")
    .eq("team_name", team)
    .execute()
)
submitted_rounds = {r["round_number"] for r in submitted_rounds_resp.data}

# Next unsubmitted round defaults in the selector
if "round" not in st.session_state:
    for r in range(1, current_round + 1):
        if r not in submitted_rounds:
            st.session_state.round = r
            break
    else:
        st.session_state.round = current_round

selected_round = st.session_state.round

# =====================================
# 🎨 PAGE STYLING
# =====================================
st.markdown(
    """
    <style>
    html, body, [class*="css"] {
        font-size: 10px !important;
    }
    .stApp { background-color: #FFF9F3; }

    h1, h2, h3, h4, h5, h6 {
        color: #4B2E05 !important;
        text-shadow: 1px 1px 2px rgba(150,100,50,0.3);
        font-family: 'Poppins', sans-serif;
    }

    .stSubheader, .stMarkdown p {
        color: #3B2C1A !important;
        font-size: 1.8rem; 
        line-height: 1.6;
    }

    hr {
        border: none;
        border-top: 1px solid rgba(120,80,40,0.25);
        margin: 1.5rem 0;
    }

    .stButton>button {
        background-color: #F5D2A4 !important;
        color: #4B2E05 !important;
        border: 1px solid #C68E53 !important;
        border-radius: 10px !important;
        font-weight: 700 !important;
        box-shadow: 0px 2px 6px rgba(0, 0, 0, 0.25);
        transition: all 0.2s ease-in-out;
    }

    .stButton>button:hover {
        background-color: #E0B070 !important;
        transform: scale(1.05);
    }

    [data-testid="stMetricValue"] {
        color: #4B2E05 !important;
    }

    div[data-testid="stExpander"] {
        background-color: rgba(255, 247, 234, 0.95);
        border-radius: 12px;
        border: 1px solid rgba(180,140,80,0.2);
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }

    div[data-testid="stDataFrame"] table {
        background: linear-gradient(180deg, #FFF6E8, #FCEBD6);
        border-radius: 12px;
        border-collapse: collapse;
        overflow: hidden;
        box-shadow: 0px 3px 8px rgba(60, 40, 20, 0.15);
        color: #3B2C1A;
        font-size: 1rem;
    }

    th {
        background-color: #D6A76E !important;
        color: #fffdf9 !important;
        text-transform: uppercase;
        font-weight: 700;
        text-align: center !important;
        padding: 10px 8px !important;
    }

    td {
        text-align: center !important;
        padding: 8px !important;
        border-bottom: 1px solid rgba(180,140,80,0.2);
    }

    tr:nth-child(even) td {
        background-color: rgba(255, 248, 230, 0.85) !important;
    }

    tr:nth-child(odd) td {
        background-color: rgba(255, 242, 220, 0.95) !important;
    }

    tr:hover td {
        background-color: #F4D6B3 !important;
        transition: all 0.2s ease-in-out;
    }

    canvas {
        background-color: #FFF9F3 !important;
        border-radius: 12px;
    }

    .stButton>button[kind="secondary"] {
        background-color: #FFF2E0 !important;
        color: #4B2E05 !important;
        border: 1px solid #D6A76E !important;
    }

    [data-testid="stDataEditor"] table,
    [data-testid="stDataFrame"] table {
        width: 100% !important;
        table-layout: fixed !important;
        text-align: center !important;
    }
    
    [data-testid="stDataEditor"] th,
    [data-testid="stDataFrame"] th {
        text-align: center !important;
        vertical-align: middle !important;
    }
    
    [data-testid="stDataEditor"] td div,
    [data-testid="stDataFrame"] td div {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        text-align: center !important;
    }
    
    [data-testid="stDataEditor"] input {
        text-align: center !important;
    }
    
    [data-testid="stDataEditor"] td, 
    [data-testid="stDataFrame"] td {
        padding: 8px !important;
        vertical-align: middle !important;
    }

    [data-testid="stDataEditor"] .st-emotion-cache,
    [data-testid="stDataFrame"] .st-emotion-cache,
    [data-testid="stDataEditor"] div[data-testid="cell-container"],
    [data-testid="stDataFrame"] div[data-testid="cell-container"] {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
    }
    
    [data-testid="stDataEditor"] input::placeholder {
        text-align: center !important;
    }

    [data-testid="stDataFrame"] table {
        width: 100% !important;
        text-align: center !important;
        table-layout: fixed !important;
    }
    
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrame"] td {
        text-align: center !important;
        vertical-align: middle !important;
    }
    
    [data-testid="stDataFrame"] div[role="gridcell"],
    [data-testid="stDataFrame"] div[data-testid="cell-container"] {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
    }

    div[data-testid="stDataEditor"] thead tr th {
        text-align: center !important;
        justify-content: center !important;
        align-items: center !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =====================================
# 🎯 ROUND BANNER
# =====================================

round_number = selected_round

st.markdown(
    f"""
    <div style="
        background: linear-gradient(90deg, #F5D2A4, #E0B070);
        padding: 0.6rem 1rem;
        border-radius: 10px;
        text-align: center;
        color: #4B2E05;
        font-weight: 700;
        font-size: 1.5rem;
        margin-bottom: 1.2rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    ">
        🎯 <span style="font-size:1.5rem;">Round {round_number}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================================
# 🧁 HEADER
# ======================================
st.title("🍰 Production Plan by Channel")
st.write(f"Welcome, **{team}**!")

# ======================================
# 🧾 LOAD DATA
# ======================================
try:
    cakes_df = pd.DataFrame(supabase.table("cakes").select("*").execute().data)
    channels_df = pd.DataFrame(
        supabase.table("channels").select("channel").execute().data
    )
except Exception as e:
    st.error("❌ Failed to load cakes or channels data.")
    st.exception(e)
    st.stop()

channels = channels_df["channel"].tolist()

# ======================================
# ⏱️ Normalize time units to hours
# ======================================
for col in ["oven_min_per_batch", "prep_min_per_unit", "pack_min_per_unit"]:
    if col in cakes_df.columns:
        cakes_df[col] = cakes_df[col] / 60.0

# ======================================
# 📦 INVENTORY
# ======================================
def get_inventory(team_name, category):
    data = (
        supabase.table("inventory")
        .select("resource_name, quantity")
        .eq("team_name", team_name)
        .eq("category", category)
        .execute()
        .data
    )
    # keys lowercased for comparisons
    return {d["resource_name"].lower(): d["quantity"] for d in data}


ingredient_stock = get_inventory(team, "ingredient")
capacity_totals = get_inventory(team, "capacity")

# ======================================
# 📝 BUILD PRODUCTION TABLE
# ======================================
st.subheader("🧁 Production Planning")

rows = []
for _, cake_row in cakes_df.iterrows():
    cake_name = cake_row["name"]
    min_units = int(cake_row["minimum_units_if_made"])
    row = {"Cake (min qty)": f"{cake_name} (min {min_units})"}
    for ch in channels:
        row[ch] = 0
    rows.append(row)

production_table = pd.DataFrame(rows)

# 🔄 FORCE RESET AFTER SUBMISSION
if st.session_state.get("force_reset_prod"):
    production_table = pd.DataFrame(rows)
    st.session_state.force_reset_prod = False

editor_key = f"prod_editor_{st.session_state.get('editor_version', 0)}"

edited_plan = st.data_editor(
    production_table,
    key=editor_key,
    use_container_width=True,
    num_rows="fixed",
    hide_index=True,
    column_config={
        "Cake (min qty)": st.column_config.Column(disabled=True),
        **{
            ch: st.column_config.NumberColumn(ch, min_value=0, step=1)
            for ch in channels
        },
    },
)


# ======================================
# 🔄 Convert wide → long & apply min batch rules
# ======================================
plan_entries = []
violations = []
min_batch = {r["name"]: int(r["minimum_units_if_made"]) for _, r in cakes_df.iterrows()}

for _, row in edited_plan.iterrows():
    cake = row["Cake (min qty)"].split(" (min")[0].strip()
    min_req = min_batch[cake]
    total_qty = sum(row[ch] for ch in channels)

    if 0 < total_qty < min_req:
        violations.append((cake, min_req))

    for ch in channels:
        qty = row[ch]
        if qty > 0:
            plan_entries.append({"cake": cake, "channel": ch, "qty": qty})

batch_ok = len(violations) == 0

for cake, req in violations:
    st.warning(f"⚠️ {cake}: minimum total quantity is {req} units.")

# Normalize plan entries for merging (strip only, keep original case)
plan_df = pd.DataFrame(plan_entries)
if not plan_df.empty:
    plan_df["cake"] = plan_df["cake"].astype(str).str.strip()
    plan_df["channel"] = plan_df["channel"].astype(str).str.strip()

# ======================================
# 💰 LOAD PRICES & DEMAND FOR THIS ROUND
# ======================================
def get_json(table, round_num):
    resp = (
        supabase.table(table)
        .select("*")
        .eq("team_name", team)
        .eq("round_number", round_num)
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if resp.data:
        raw = resp.data[0]
        key = "prices_json" if table == "prices" else "demands_json"
        return pd.DataFrame(json.loads(raw.get(key, "[]")))
    return pd.DataFrame()


price_df = get_json("prices", selected_round)
demand_df = get_json("demands", selected_round)

if price_df.empty:
    st.error("❌ You must submit final prices for this round before planning production.")
    st.stop()

if demand_df.empty:
    st.error("❌ Demand data is missing. Submit final prices first.")
    st.stop()

# Normalize column names ONLY
price_df.columns = price_df.columns.map(str).str.lower()
demand_df.columns = demand_df.columns.map(str).str.lower()

# Normalize key fields (values) only for join (strip, no lowercasing)
for df in (price_df, demand_df):
    if "cake" in df.columns:
        df["cake"] = df["cake"].astype(str).str.strip()
    if "channel" in df.columns:
        df["channel"] = df["channel"].astype(str).str.strip()

# ======================================
# 🧠 CAPACITY & INGREDIENT CHECKS
# ======================================
required = {"prep": 0, "oven": 0, "package": 0, "oven rental": 0}

if not plan_df.empty:
    totals = plan_df.groupby("cake")["qty"].sum().to_dict()

    for _, r in cakes_df.iterrows():
        cake = r["name"]
        qty = totals.get(cake, 0)

        required["prep"] += qty * r["prep_min_per_unit"]

        batches = math.ceil(qty / r["batch_size_units"]) if r["batch_size_units"] else 0
        required["oven"] += batches * r["oven_min_per_batch"]
        required["oven rental"] = required["oven"]

        required["package"] += qty * r["pack_min_per_unit"]

# Ingredient requirements
recipes = pd.DataFrame(supabase.table("recipes").select("*").execute().data)
recipes.columns = [c.lower() for c in recipes.columns]


def compute_needs(plan_df_):
    needs = {}
    totals = plan_df_.groupby("cake")["qty"].sum().to_dict()
    for cake, qty in totals.items():
        rec = recipes[recipes["name"].str.lower() == cake.lower()]
        if rec.empty:
            continue
        row = rec.iloc[0]
        for ing in row.index:
            if ing in ["id", "cake_id", "name", "created_at"]:
                continue
            amt = qty * float(row[ing])
            needs[ing] = needs.get(ing, 0) + amt
    return needs


ingredient_needs = compute_needs(plan_df) if not plan_df.empty else {}

capacity_ok = all(required[k] <= capacity_totals.get(k, 0) for k in required)
ingredient_ok = all(
    ingredient_needs[i] <= ingredient_stock.get(i, 0) for i in ingredient_needs
)

# ======================================
# 💰 PROFIT CALCULATION
# ======================================
if plan_df.empty:
    merged = pd.DataFrame()
    profit_today = 0.0
else:
    # Standardize price column name
    if "price_usd" in price_df.columns:
        price_df = price_df.rename(columns={"price_usd": "price"})
    elif "net_usd" in price_df.columns:
        price_df = price_df.rename(columns={"net_usd": "price"})

    # Merge plan + price + demand
    merged = (
        plan_df.merge(price_df, on=["cake", "channel"], how="left")
        .merge(demand_df, on=["cake", "channel"], how="left")
    )

    # Detect transport cost column
    transport_col = None
    if "transport_cost_usd" in merged.columns:
        transport_col = "transport_cost_usd"
    elif "transport_cost_per_unit_usd" in merged.columns:
        transport_col = "transport_cost_per_unit_usd"

    # Fill numeric fields
    merged["qty"] = merged["qty"].astype(float).fillna(0)
    if "price" in merged.columns:
        merged["price"] = merged["price"].astype(float).fillna(0)
    else:
        merged["price"] = 0.0

    if "demand" in merged.columns:
        merged["demand"] = merged["demand"].astype(float).fillna(0)
    else:
        merged["demand"] = 0.0

    if transport_col:
        merged[transport_col] = merged[transport_col].astype(float).fillna(0)
    else:
        merged["transport_cost_usd"] = 0.0
        transport_col = "transport_cost_usd"

    # Sold units = min(qty, demand)
    merged["sold_units"] = merged[["qty", "demand"]].min(axis=1)

    # Revenue & transport cost
    merged["revenue"] = merged["sold_units"] * merged["price"]
    merged["transport_cost_total"] = merged["sold_units"] * merged[transport_col]

    # Profit
    merged["profit"] = merged["revenue"] - merged["transport_cost_total"]
    profit_today = float(merged["profit"].sum())

st.markdown("### 🧂 Ingredient Feasibility Check")

if ingredient_needs:
    ing_table = pd.DataFrame(
        [
            {
                "Ingredient": i.title(),
                "Needed": round(ingredient_needs[i], 2),
                "Available": round(ingredient_stock.get(i, 0.0), 2),
                "OK": "✅"
                if ingredient_needs[i] <= ingredient_stock.get(i, 0.0)
                else "❌",
            }
            for i in ingredient_needs
        ]
    )
    st.dataframe(ing_table, use_container_width=True)
else:
    st.info("No ingredients needed yet.")

st.markdown("### 🧰 Capacity Requirements")

cap_table = pd.DataFrame(
    [
        {
            "Capacity": "Prep",
            "Used": required["prep"],
            "Available": capacity_totals.get("prep", 0),
        },
        {
            "Capacity": "Oven",
            "Used": required["oven"],
            "Available": capacity_totals.get("oven", 0),
        },
        {
            "Capacity": "Oven Rental",
            "Used": required["oven rental"],
            "Available": capacity_totals.get("oven rental", 0),
        },
        {
            "Capacity": "Packaging",
            "Used": required["package"],
            "Available": capacity_totals.get("package", 0),
        },
    ]
)

cap_table["OK"] = cap_table.apply(
    lambda r: "✅" if r["Used"] <= r["Available"] else "❌", axis=1
)

st.dataframe(cap_table, use_container_width=True)

# ======================================
# 📊 SUMMARY METRICS
# ======================================
st.markdown("---")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Prep", f"{required['prep']:.2f} / {capacity_totals.get('prep',0):.2f}")
c2.metric("Oven", f"{required['oven']:.2f} / {capacity_totals.get('oven',0):.2f}")
c3.metric(
    "Oven Rental",
    f"{required['oven rental']:.2f} / {capacity_totals.get('oven rental',0):.2f}",
)
c4.metric(
    "Pack", f"{required['package']:.2f} / {capacity_totals.get('package',0):.2f}"
)
c5.metric("Expected Profit", f"${profit_today:,.2f}")

if not capacity_ok:
    st.error("❌ Capacity exceeded!")
elif not ingredient_ok:
    st.error("❌ Not enough ingredients!")
elif not batch_ok:
    st.error("❌ Minimum batch rule violated!")
else:
    st.success("✅ Feasible plan!")

# ======================================
# 🔒 CHECK IF ALREADY SUBMITTED THIS ROUND
# ======================================
already_submitted = (
    supabase.table("production_plans")
    .select("id")
    .eq("team_name", team)
    .eq("round_number", selected_round)
    .execute()
    .data
)
locked = bool(already_submitted)

# ======================================
# 💾 SAVE PLAN — WITH CONFIRMATION
# ======================================
if "confirm_prod_submit" not in st.session_state:
    st.session_state.confirm_prod_submit = False

if submissions_locked(supabase):
    st.error("🚫 Submissions are locked by the instructor.")
else:
    if st.button("💾 Submit Production Plan", disabled=locked):
        if locked:
            st.warning("You already submitted this round.")
        elif not plan_entries:
            st.warning("⚠️ Enter quantities before saving.")
        else:
            st.session_state.confirm_prod_submit = True
            st.rerun()

# Confirmation dialog
if st.session_state.confirm_prod_submit:
    st.warning(
        "⚠️ Are you sure you want to submit this production plan? "
        "You cannot edit it afterward!"
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("✅ Yes, submit now"):
            try:
                # 1️⃣ Save the production plan
                supabase.table("production_plans").insert(
                    {
                        "team_name": team,
                        "round_number": selected_round,
                        "plan_json": json.dumps(plan_entries),
                        "profit_usd": profit_today,
                        "required_json": json.dumps(required),
                    }
                ).execute()

                # 2️⃣ Load inventory rows once for safe matching (case-insensitive)
                ing_rows = (
                    supabase.table("inventory")
                    .select("id, resource_name, quantity")
                    .eq("team_name", team)
                    .eq("category", "ingredient")
                    .execute()
                    .data
                )
                cap_rows = (
                    supabase.table("inventory")
                    .select("id, resource_name, quantity")
                    .eq("team_name", team)
                    .eq("category", "capacity")
                    .execute()
                    .data
                )

                # 3️⃣ DEDUCT INGREDIENT STOCK
                for ing, used_qty in ingredient_needs.items():
                    match = next(
                        (
                            row
                            for row in ing_rows
                            if row["resource_name"].lower() == ing.lower()
                        ),
                        None,
                    )
                    if match:
                        new_qty = max(0, float(match["quantity"]) - used_qty)
                        supabase.table("inventory").update(
                            {"quantity": new_qty}
                        ).eq("id", match["id"]).execute()

                # 4️⃣ DEDUCT CAPACITY HOURS
                for cap_name, hours_used in required.items():
                    match = next(
                        (
                            row
                            for row in cap_rows
                            if row["resource_name"].lower() == cap_name.lower()
                        ),
                        None,
                    )
                    if match:
                        new_qty = max(0, float(match["quantity"]) - hours_used)
                        supabase.table("inventory").update(
                            {"quantity": new_qty}
                        ).eq("id", match["id"]).execute()

        
                st.success("✅ Production plan submitted and stock updated!")
                st.session_state.editor_version = st.session_state.get("editor_version", 0) + 1
                st.session_state.confirm_prod_submit = False
                st.rerun()


            except Exception as e:
                st.error("❌ Failed to save.")
                st.exception(e)

    with col2:
        if st.button("❌ Cancel"):
            st.session_state.confirm_prod_submit = False
            st.info("Submission cancelled.")
            st.rerun()

# ======================================
# 📜 HISTORY
# ======================================
st.markdown("---")
st.subheader("📜 Previous Production Plans")

history = (
    supabase.table("production_plans")
    .select("*")
    .eq("team_name", team)
    .order("round_number", desc=True)
    .execute()
    .data
)

for r in history:
    with st.expander(
        f"Round {r['round_number']} — Expected Profit ${r['profit_usd']:,.2f}"
    ):
        df = pd.DataFrame(json.loads(r["plan_json"]))
        if not df.empty:
            pivot = df.pivot(index="cake", columns="channel", values="qty").fillna(0)
            pivot = pivot.reset_index().rename(columns={"cake": "Cake"})
            st.dataframe(pivot, use_container_width=True)

# ======================================
# 🚪 LOGOUT
# ======================================
if st.button("🚪 Log out"):
    st.session_state.clear()
    st.success("Logged out.")
    st.switch_page("Login.py")
