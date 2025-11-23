#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Market Demand Page - Cake Game
Now round-based instead of date-based.
"""

import os
import json
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import pytz



BEIRUT_TZ = pytz.timezone("Asia/Beirut")

st.set_page_config(page_title="Demand", page_icon="📈", layout="wide")

# =====================================
# 🔒 LOGIN CHECK
# =====================================

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Please log in first.")
    st.stop()

# ❌ Specific real-world calendar dates when the game is closed
CLOSED_DATES = []

today = datetime.now(BEIRUT_TZ).date()
if today.isoformat() in CLOSED_DATES:
    st.warning(f"🚫 The game is closed today ({today}). Please come back tomorrow!")
    st.stop()

# =====================================
# 🌍 SUPABASE SETUP
# =====================================

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("❌ Missing Supabase credentials. Check .env file.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def submissions_locked(supabase):
    lock_row = supabase.table("game_state").select("value").eq("key", "locked").single().execute()
    return lock_row.data and lock_row.data["value"] == "true"

def get_current_round():
    response = (
        supabase.table("game_state")
        .select("value")
        .eq("key", "current_round")
        .single()
        .execute()
    )
    return int(response.data["value"])


# =====================================
# 🔄 CURRENT ROUND
# =====================================

current_round = get_current_round()
st.session_state.round = current_round
round_number = current_round

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

    /* ✅ Reliable Center Alignment for All Streamlit Tables */
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

    /* 🧭 FORCE Center Alignment inside Streamlit’s Virtualized Table Cells */
    [data-testid="stDataEditor"] .st-emotion-cache,
    [data-testid="stDataFrame"] .st-emotion-cache,
    [data-testid="stDataEditor"] div[data-testid="cell-container"],
    [data-testid="stDataFrame"] div[data-testid="cell-container"] {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
    }
    
    /* Center text in all number inputs inside data editors */
    [data-testid="stDataEditor"] input {
        text-align: center !important;
    }
    
    /* Ensure editable columns have centered placeholders too */
    [data-testid="stDataEditor"] input::placeholder {
        text-align: center !important;
    }
    
    /* Optional: Fix width-based overflow alignment */
    [data-testid="stDataEditor"] div[role="gridcell"],
    [data-testid="stDataFrame"] div[role="gridcell"] {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        text-align: center !important;
    }
    /* ✅ Center alignment for Investment History tables (st.dataframe) */
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
    
    /* Streamlit virtualized cell containers */
    [data-testid="stDataFrame"] div[role="gridcell"],
    [data-testid="stDataFrame"] div[data-testid="cell-container"] {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
    }
    
    /* Optional: Center numbers inside scrollable or virtualized cells */
    [data-testid="stDataFrame"] input {
        text-align: center !important;
    }
    /* ✅ Center align headers and cells inside read-only data editors (investment history) */
    div[data-testid="stDataEditor"] thead tr th {
        text-align: center !important;
        justify-content: center !important;
        align-items: center !important;
    }


    </style>
    """,
    unsafe_allow_html=True
)

# =====================================
# 🎯 ROUND BANNER
# =====================================

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

# =====================================
# 🎨 PAGE SETUP
# =====================================

st.title("📈 Market Demand")
st.write(f"Welcome, **{st.session_state.team_name}**!")

# Load current balance from DB (and sync session)
try:
    team_finances = (
        supabase.table("teams")
        .select("money")
        .eq("team_name", st.session_state.team_name)
        .single()
        .execute()
    )
    current_balance = float(team_finances.data["money"])
    st.session_state.money = current_balance
except Exception:
    current_balance = float(st.session_state.get("money", 0.0))

st.write(f"Current Balance: **${current_balance:,.2f}**")

# =====================================
# 🧾 LOAD DATA
# =====================================

try:
    cakes_df = pd.DataFrame(
        supabase.table("cakes").select("name").execute().data
    )
    channels_df = pd.DataFrame(
        supabase.table("channels").select("*").execute().data
    )
except Exception:
    local_path = os.path.join(os.path.dirname(__file__), "..", "data", "channels.csv")
    if os.path.exists(local_path):
        channels_df = pd.read_csv(local_path)
    else:
        st.error("❌ Could not load channels.")
        st.stop()

if cakes_df.empty or channels_df.empty:
    st.error("❌ Missing data: please ensure cakes and channels exist.")
    st.stop()


# =====================================
# 🔒 CHECK IF PRICES ALREADY FINALIZED FOR THIS ROUND
# =====================================

existing_final = (
    supabase.table("prices")
    .select("id, finalized, auto_filled, round_number")
    .eq("team_name", st.session_state.team_name)
    .eq("round_number", round_number)
    .execute()
    .data
)

record = existing_final[0] if existing_final else {}
is_finalized = record.get("finalized", False)
is_auto_filled = record.get("auto_filled", False)
finalized_this_round = bool(is_finalized and not is_auto_filled)

# =====================================
# 💲 PRICING INPUTS — Unified Table Layout
# =====================================

st.subheader("📊 Set Prices per Channel and Cake")

st.markdown(
    """
    <div style="
        background-color: #FFF2E0;
        border: 1px solid #E0B070;
        border-radius: 10px;
        padding: 0.8rem 1rem;
        margin-bottom: 1rem;
        color: #4B2E05;
        font-weight: 600;
        font-size: 1.05rem;
    ">
        💡 <strong>Note:</strong> Submitting prices can only be done <u>once per round</u>.
        After you click <em>Submit Final Prices</em>, you won't be able to edit them again for this round.
    </div>
    """,
    unsafe_allow_html=True,
)

# Table is always editable; only submit button is disabled after finalization
table_inputs_disabled = False
submit_disabled = finalized_this_round

try:
    # Fetch ALL competitor prices from the immediate previous round
    prev_round = round_number - 1

    if prev_round > 0:
        prev_prices_data = (
            supabase.table("prices")
            .select("team_name, prices_json, round_number")
            .eq("round_number", prev_round)
            .execute()
            .data
        )
    else:
        prev_prices_data = []

    if prev_prices_data:
        prev_rows = []

        # Unpack all price entries from all teams
        for rec in prev_prices_data:
            for p in json.loads(rec["prices_json"]):
                prev_rows.append(
                    {
                        "channel": p["channel"],
                        "cake": p["cake"],
                        "price_usd": p["price_usd"],
                    }
                )

        prev_prices_df = pd.DataFrame(prev_rows)

        # True average across all teams: (channel, cake)
        avg_prices = (
            prev_prices_df.groupby(["channel", "cake"])["price_usd"]
            .mean()
            .to_dict()
        )

        st.info(f"ℹ️ Using competitor prices from Round {prev_round} as baseline.")

    else:
        avg_prices = {}
        st.warning("⚠️ No prior competitor prices found for previous round.")

except Exception as e:
    avg_prices = {}
    st.warning("⚠️ Could not load competitor prices.")


# ============================
# 📥 Load this round's existing prices to prefill the table
# ============================

today_prices_rec = (
    supabase.table("prices")
    .select("prices_json")
    .eq("team_name", st.session_state.team_name)
    .eq("round_number", round_number)
    .limit(1)
    .execute()
    .data
)

prefill_map = {}  # (channel, cake) → price_usd

if today_prices_rec:
    try:
        loaded_prices = json.loads(today_prices_rec[0]["prices_json"])
        for p in loaded_prices:
            key = (p["channel"], p["cake"])
            prefill_map[key] = float(p["price_usd"])
    except Exception:
        pass  # If corrupted or empty, just start clean

# ============================
# 📊 Build Pivot-Style Pricing Table
# ============================

pricing_rows = []

for _, cake_row in cakes_df.iterrows():
    cake = cake_row["name"]

    row = {"Cake": cake}

    for _, ch_row in channels_df.iterrows():
        channel = ch_row["channel"]
        prev_avg = avg_prices.get((channel, cake), 0.0)

        row[f"{channel} (prev)"] = round(prev_avg, 2)
        row[channel] = prefill_map.get((channel, cake), 0.0)

    pricing_rows.append(row)

pricing_df = pd.DataFrame(pricing_rows)

# ============================
# 📝 Column Configuration
# ============================

col_cfg = {
    "Cake": st.column_config.Column(disabled=True, width="medium")
}

for _, ch_row in channels_df.iterrows():
    channel = ch_row["channel"]

    col_cfg[f"{channel} (prev)"] = st.column_config.NumberColumn(
        label=f"{channel} Avg (Prev Round)",
        format="%.2f",
        disabled=True,
    )

    col_cfg[channel] = st.column_config.NumberColumn(
        label=f"{channel} Your Price ($)",
        min_value=0.0,
        step=0.1,
    )

# ============================
# 📝 Show Table to User
# ============================

st.markdown("Enter your selling prices for each cake and channel below:")

edited_prices = st.data_editor(
    pricing_df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    key="price_table",
    disabled=table_inputs_disabled,
    column_config=col_cfg,
)

# ============================
# 📤 Convert wide format → long format for database
# ============================

pricing_entries = []

for _, row in edited_prices.iterrows():
    cake = row["Cake"]

    for _, ch_row in channels_df.iterrows():
        channel = ch_row["channel"]
        price = float(row[channel])

        if price > 0:
            pricing_entries.append(
                {
                    "team_name": st.session_state.team_name,
                    "channel": channel,
                    "cake": cake,
                    "price_usd": price,
                    "transport_cost_usd": float(
                        channels_df.loc[
                            channels_df["channel"] == channel,
                            "transport_cost_per_unit_usd",
                        ].iloc[0]
                    ),
                }
            )

# =====================================
# 📊 CALCULATE DEMAND (TEST)
# =====================================

st.subheader("📈 Test Market Demand")

if st.button("📊 Calculate Demand"):
    try:
        demand_params = pd.read_csv(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "data",
                "instructor_demand_competition.csv",
            )
        )

        # First round: no previous competition data
        if round_number == 1:
            prev_prices_data = []
        else:
            prev_round = round_number - 1
            prev_prices_data = (
                supabase.table("prices")
                .select("team_name, prices_json, round_number")
                .eq("round_number", prev_round)
                .execute()
                .data
            )

        if prev_prices_data:
            prev_rows = []
            for rec in prev_prices_data:
                for p in json.loads(rec["prices_json"]):
                    prev_rows.append(
                        {
                            "team_name": rec["team_name"],
                            "channel": p["channel"],
                            "cake": p["cake"],
                            "price_usd": p["price_usd"],
                        }
                    )
            prev_prices_df = pd.DataFrame(prev_rows)
            avg_prices_round = (
                prev_prices_df.groupby(["channel", "cake"])["price_usd"].mean().to_dict()
            )
        else:
            avg_prices_round = {}
            st.info("ℹ️ No previous-round prices found — using 0 as competitor baseline.")

        results = []
        for entry in pricing_entries:
            cake = entry["cake"]
            channel = entry["channel"]
            my_price = entry["price_usd"]

            params = demand_params[
                (demand_params["cake_name"] == cake)
                & (demand_params["channel"] == channel)
            ]
            if params.empty:
                continue

            alpha = params["alpha"].values[0]
            beta = params["beta"].values[0]
            gamma = params["gamma_competition"].values[0]

            avg_other = avg_prices_round.get((channel, cake), 0.0)

            if round_number == 1:
                D = max(0, alpha - beta * my_price)
            else:
                D = max(0, alpha - beta * my_price + gamma * (avg_other - my_price))

            results.append(
                {
                    "Cake": cake,
                    "Channel": channel,
                    "Your Price ($)": my_price,
                    "Prev-Round Avg ($)": round(avg_other, 2),
                    "Expected Demand": round(D, 1),
                }
            )

        if results:
            st.success("✅ Demand calculated successfully!")
            st.dataframe(pd.DataFrame(results), use_container_width=True)
        else:
            st.warning("⚠️ No matching demand parameters found.")

    except Exception as e:
        st.error("❌ Failed to calculate demand.")
        st.exception(e)

# =====================================
# 💾 SAVE FINAL PRICES (LOCK-IN PER ROUND)
# =====================================

if "confirm_submit" not in st.session_state:
    st.session_state.confirm_submit = False
if submissions_locked(supabase):
    st.error("🚫 Submissions are locked by the instructor.")
else:
    if st.button("💾 Submit Final Prices", disabled=submit_disabled):
        if not pricing_entries:
            st.warning("⚠️ Please enter prices before saving.")
            st.stop()
        st.session_state.confirm_submit = True
        st.rerun()

if st.session_state.confirm_submit:
    st.warning("⚠️ Are you sure you want to submit? This can only be done once per round!")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("✅ Yes, submit now"):
            try:
                payload = {
                    "team_name": st.session_state.team_name,
                    "prices_json": json.dumps(pricing_entries),
                    "round_number": round_number,
                    "finalized": True,
                    "auto_filled": False,
                }

                existing_this_round = (
                    supabase.table("prices")
                    .select("id")
                    .eq("team_name", st.session_state.team_name)
                    .eq("round_number", round_number)
                    .limit(1)
                    .execute()
                    .data
                )

                if existing_this_round:
                    supabase.table("prices").update(payload).eq(
                        "id", existing_this_round[0]["id"]
                    ).execute()
                else:
                    supabase.table("prices").insert(payload).execute()
                # =====================================
                # 📈 CALCULATE & SAVE DEMAND FOR THIS ROUND
                # =====================================
                
                # Load demand parameters
                demand_params = pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__),
                        "..",
                        "data",
                        "instructor_demand_competition.csv",
                    )
                )
                
                # Load competitor prices (previous round)
                if round_number == 1:
                    competitor_avg = {}
                else:
                    prev_round = round_number - 1
                    prev_prices_data = (
                        supabase.table("prices")
                        .select("team_name, prices_json")
                        .eq("round_number", prev_round)
                        .execute()
                        .data
                    )
                
                    prev_rows = []
                    for rec in prev_prices_data:
                        for p in json.loads(rec["prices_json"]):
                            prev_rows.append({
                                "team_name": rec["team_name"],
                                "channel": p["channel"],
                                "cake": p["cake"],
                                "price_usd": p["price_usd"],
                            })
                
                    if prev_rows:
                        prev_df = pd.DataFrame(prev_rows)
                        competitor_avg = prev_df.groupby(
                            ["channel", "cake"]
                        )["price_usd"].mean().to_dict()
                    else:
                        competitor_avg = {}
                
                # Compute demand for each (cake, channel) you priced
                demand_results = []
                for entry in pricing_entries:
                    cake = entry["cake"]
                    channel = entry["channel"]
                    my_price = entry["price_usd"]
                
                    params = demand_params[
                        (demand_params["cake_name"] == cake) &
                        (demand_params["channel"] == channel)
                    ]
                    if params.empty:
                        continue
                
                    alpha = params["alpha"].values[0]
                    beta = params["beta"].values[0]
                    gamma = params["gamma_competition"].values[0]
                
                    avg_other = competitor_avg.get((channel, cake), 0.0)
                
                    if round_number == 1:
                        D = max(0, alpha - beta * my_price)
                        D = int(D)
                    else:
                        D = max(0, alpha - beta * my_price + gamma * (avg_other - my_price))
                        D = int(D)
                    demand_results.append({
                        "cake": cake,
                        "channel": channel,
                        "demand": round(D, 1)
                    })
                
                # Save into the demands table
                payload_demand = {
                    "team_name": st.session_state.team_name,
                    "round_number": round_number,
                    "demands_json": json.dumps(demand_results),
                }
                
                existing_demand = (
                    supabase.table("demands")
                    .select("id")
                    .eq("team_name", st.session_state.team_name)
                    .eq("round_number", round_number)
                    .execute()
                    .data
                )
                
                if existing_demand:
                    supabase.table("demands").update(payload_demand).eq(
                        "id", existing_demand[0]["id"]
                    ).execute()
                else:
                    supabase.table("demands").insert(payload_demand).execute()

                

                st.success("✅ Final prices saved! You can’t edit them again this round.")
                st.session_state.confirm_submit = False
                st.rerun()

            except Exception as e:
                st.error("❌ Failed to save final prices.")
                st.exception(e)

    with col2:
        if st.button("❌ Cancel"):
            st.session_state.confirm_submit = False
            st.info("Submission cancelled.")
            st.rerun()

# =====================================
# 📜 HISTORY
# =====================================

st.markdown("---")
st.subheader("📜 Previous Price Submissions")

try:
    records = (
        supabase.table("prices")
        .select("*")
        .eq("team_name", st.session_state.team_name)
        .order("round_number", desc=True)
        .execute()
        .data
    )

    if records:
        # Only show manual submissions (not auto-filled placeholders)
        filtered_records = [r for r in records if not r.get("auto_filled", False)]

        if filtered_records:
            for r in filtered_records:
                status = "✅ Finalized" if r.get("finalized") else "🧪 Test"
                rnd = r.get("round_number", "?")
                with st.expander(f"{status} — Round {rnd}"):
                    df = pd.DataFrame(json.loads(r["prices_json"]))

                    pivot_df = df.pivot(
                        index="cake", columns="channel", values="price_usd"
                    )

                    pivot_df = pivot_df.reindex(
                        columns=channels_df["channel"].tolist(), fill_value=0
                    )

                    pivot_df = (
                        pivot_df.reset_index()
                        .rename(columns={"cake": "Cake"})
                    )

                    st.dataframe(pivot_df, use_container_width=True)
        else:
            st.info("No manually submitted price history yet.")
    else:
        st.info("No prior price submissions yet.")

except Exception as e:
    st.error("❌ Could not load price history.")
    st.exception(e)

# =====================================
# 🚪 LOGOUT
# =====================================

if st.button("🚪 Log out"):
    st.session_state.clear()
    st.success("You’ve been logged out.")
    st.switch_page("Login.py")
