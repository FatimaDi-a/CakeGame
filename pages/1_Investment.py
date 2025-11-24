#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 14:47:57 2025

@author: fatima
"""
import streamlit as st
import pandas as pd
import json
import os
from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path
from datetime import date, datetime, timedelta
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
    st.error("❌ Missing Supabase credentials. Check .env file.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================
# 🧁 PAGE HEADER
# ============================
st.set_page_config(page_title="Investments", page_icon="💰", layout="wide")

# =====================================
# 🔒 LOGIN CHECK
# =====================================
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Please log in first.")
    st.stop()

if "investment_saved" not in st.session_state:
    st.session_state.investment_saved = False




def get_current_round():
    response = supabase.table("game_state").select("value").eq("key", "current_round").maybe_single().execute()
    return int(response.data["value"])

# 🔁 NEW LOGIC: always fetch round and store in session
current_round = get_current_round()
st.session_state.round = current_round
def submissions_locked(supabase):
    lock_row = supabase.table("game_state").select("value").eq("key", "locked").maybe_single().execute()
    return lock_row.data and lock_row.data["value"] == "true"


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
# 🎯 ROUND BANNER (fixed)
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
        font-size: 1.2rem;
        margin-bottom: 1.2rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    ">
        🎯 <span style="font-size:1.3rem;">Round {current_round}</span>
    </div>
    """,
    unsafe_allow_html=True
)

st.title("💰 Investment Decisions")
st.write(f"Welcome, **{st.session_state.team_name}**!")

if "last_save_time" not in st.session_state:
    st.session_state.last_save_time = None


COOLDOWN_SECONDS = 30

def save_on_cooldown():
    last = st.session_state.last_save_time
    if last is None:
        return False, 0

    elapsed = (datetime.now() - last).total_seconds()
    remaining = max(0, int(COOLDOWN_SECONDS - elapsed))

    return remaining > 0, remaining

# ============================
# 📊 LOAD CSV DATA
# ============================
try:
    data_dir = Path(__file__).parent.parent / "data"
    ingredients = pd.read_csv(data_dir / "ingredients.csv")
    wages = pd.read_csv(data_dir / "wages_energy.csv")
except FileNotFoundError:
    st.error("❌ Missing required CSV files (ingredients.csv or wages_energy.csv).")
    st.stop()

valid_params = [
    "prep_wage_usd_per_hour",
    "oven_wage_usd_per_hour",
    "package_wage_usd_per_hour",
    "oven_rental_wage_usd_per_hour"
]
filtered_wages = wages[wages["parameter"].isin(valid_params)]

# ============================
# 💵 CASH AND STOCK VALUE
# ============================
inventory_data = (
    supabase.table("inventory")
    .select("resource_name, quantity")
    .eq("team_name", st.session_state.team_name)
    .eq("category", "ingredient")
    .execute()
)


# Build a dictionary of ingredient → quantity
current_stock = {i["resource_name"]: i["quantity"] for i in inventory_data.data} if inventory_data.data else {}

capacity_data = (
    supabase.table("inventory")
    .select("resource_name, quantity")
    .eq("team_name", st.session_state.team_name)
    .eq("category", "capacity")
    .execute()
)

capacity_stock = {i["resource_name"]: i["quantity"] for i in capacity_data.data} if capacity_data.data else {}

# Copy the ingredients CSV data
ingredients_df = ingredients.copy()

# Numeric stock column for calculations
ingredients_df["Current stock (num)"] = (
    ingredients_df["ingredient"]
    .map(current_stock)
    .fillna(0)
)

# String stock column for display (centering)
ingredients_df["Current stock"] = ingredients_df["Current stock (num)"].map(lambda x: f"{x:g}")

# ✅ Financial calculations use the numeric version
team_finances = supabase.table("teams").select("money, stock_value").eq("team_name", st.session_state.team_name).maybe_single().execute()

current_balance = float(team_finances.data["money"])
st.session_state.money = current_balance  # keep UI consistent

# Ingredient stock value
ingredient_value = sum(ingredients_df["unit_cost_usd"] * ingredients_df["Current stock (num)"])

# Build a lookup for capacity unit cost from wages table
capacity_cost_lookup = {
    row["parameter"]
        .replace("_wage_usd_per_hour", "")
        .replace("_usd_per_hour", "")
        .replace("_wage", "")
        .replace("_", " ")
        .title(): float(row["value"])
    for _, row in filtered_wages.iterrows()
}

# Capacity stock value
capacity_value = sum(
    capacity_cost_lookup.get(cap_name, 0) * qty
    for cap_name, qty in capacity_stock.items()
)

# Total stock value (ingredients + capacity)
stock_value = ingredient_value + capacity_value

st.markdown(
    f"""
    <div style="display:flex; gap:40px; font-size:1.2rem; font-weight:700; color:#4B2E05;">
        <div>💵 Cash value: ${current_balance:,.2f}</div>
        <div>📦 Stock value: ${stock_value:,.2f}</div>
    </div>
    """,
    unsafe_allow_html=True
)

# ============================
# 🧺 INGREDIENTS SECTION
# ============================
st.subheader("🧺 Ingredients Purchase")

ingredients_df["Unit cost (unit)"] = ingredients_df.apply(
    lambda x: f"${x['unit_cost_usd']:.2f} ({x['unit']})", axis=1
)
ingredients_df["Enter value"] = 0.0

ingredients_display = ingredients_df[["ingredient", "Unit cost (unit)", "Current stock", "Enter value"]]
ingredients_display = ingredients_display.rename(columns={"ingredient": "Ingredient"})

st.markdown("Enter the quantities you want to buy below:")
edited_df = st.data_editor(
    ingredients_display,
    use_container_width=True,
    num_rows="fixed",
    hide_index=True,
    key="ingredients_table",
    column_config={
        "Ingredient": {
            "disabled": True,
            "alignment": "center",
        },
        "Unit cost (unit)": {
            "disabled": True,
            "alignment": "center",
        },
        "Current stock": {
            "disabled": True,
            "alignment": "center",
        },
        "Enter value": {
            "type": "number",
            "min_value": 0.0,
            "step": 0.5,
            "alignment": "center",
        },
    },
)


merged = ingredients.merge(edited_df, left_on="ingredient", right_on="Ingredient")
merged["subtotal_usd"] = merged["Enter value"] * merged["unit_cost_usd"]
total_ingredients_cost = merged["subtotal_usd"].sum()

ingredient_entries = [
    {
        "ingredient": row["ingredient"],
        "unit": row["unit"],
        "unit_cost_usd": float(row["unit_cost_usd"]),
        "buy_qty": float(row["Enter value"]),
        "subtotal_usd": float(row["subtotal_usd"])
    }
    for _, row in merged.iterrows()
    if row["Enter value"] > 0
]

st.markdown(f"**Total Ingredients Cost:** ${total_ingredients_cost:,.2f}")

# ============================
# 🏭 PRODUCTION CAPACITY
# ============================
# === Build Capacity Table ===
st.subheader("🏭 Production Capacity")
capacity_entries = []

# Build base numeric df
capacity_df = pd.DataFrame([
    {
        "Capacity": row["parameter"]
            .replace("_wage_usd_per_hour", "")
            .replace("_usd_per_hour", "")
            .replace("_wage", "")
            .replace("_", " ")
            .title(),
        "Rate (USD/hr)": float(row["value"]),
        "Current stock (hours)": float(
            capacity_stock.get(
                row["parameter"]
                    .replace("_wage_usd_per_hour", "")
                    .replace("_usd_per_hour", "")
                    .replace("_wage", "")
                    .replace("_", " ")
                    .title(),
                0
            )
        ),
        "Buy hours": 0.0,
        "param_key": row["parameter"],  # keep original param for saving
    }
    for _, row in filtered_wages.iterrows()
])

# 👉 Make pretty *string* versions for display so they center
capacity_df["Rate display"] = capacity_df["Rate (USD/hr)"].map(lambda x: f"${x:,.2f}")
capacity_df["Current stock display"] = capacity_df["Current stock (hours)"].map(lambda x: f"{x:g}")

# This is what the user sees
capacity_display = capacity_df[
    ["Capacity", "Rate display", "Current stock display", "Buy hours"]
].rename(columns={
    "Rate display": "Rate (USD/hr)",
    "Current stock display": "Current stock (hours)",
})

edited_capacity = st.data_editor(
    capacity_display,
    use_container_width=True,
    num_rows="fixed",
    hide_index=True,
    key="capacity_table",
    column_config={
        "Capacity": {"disabled": True, "alignment": "center"},
        "Rate (USD/hr)": {"disabled": True, "alignment": "center"},
        "Current stock (hours)": {"disabled": True, "alignment": "center"},
        "Buy hours": {
            "type": "number",
            "min_value": 0.0,
            "step": 0.5,
            "alignment": "center",
        },
    },
)

# Pull back the edited numeric values for calculations
capacity_df["Buy hours"] = edited_capacity["Buy hours"].astype(float)

# Calculate totals using the numeric df
capacity_df["Subtotal USD"] = capacity_df["Buy hours"] * capacity_df["Rate (USD/hr)"]
total_capacity_cost = capacity_df["Subtotal USD"].sum()

st.markdown(f"**Total Capacity Cost:** ${total_capacity_cost:,.2f}")

# Build capacity_entries for saving
capacity_entries = [
    {
        "parameter": row["param_key"],
        "display_name": row["Capacity"],
        "unit_cost_usd": float(row["Rate (USD/hr)"]),
        "hours": float(row["Buy hours"]),
        "subtotal_usd": float(row["Subtotal USD"]),
    }
    for _, row in capacity_df.iterrows()
    if row["Buy hours"] > 0
]


# ============================
# 📈 SUMMARY
# ============================
st.markdown("---")
team_data = supabase.table("teams").select("money").eq("team_name", st.session_state.team_name).execute()
if team_data.data:
    current_balance = team_data.data[0]["money"]
    st.session_state.money = current_balance
else:
    current_balance = st.session_state.money

total_investment = total_ingredients_cost + total_capacity_cost
remaining = current_balance - total_investment

col1, col2, col3 = st.columns(3)
col1.metric("Cash Value", f"${current_balance:,.2f}")
col2.metric("Total Investment", f"${total_investment:,.2f}")
col3.metric("Remaining After Purchase", f"${remaining:,.2f}")


# ================================
# INIT FLAGS
# ================================
if "saving_investment" not in st.session_state:
    st.session_state.saving_investment = False


# ================================
# RENDER SAVE BUTTON
# ================================
button_disabled = st.session_state.saving_investment

if st.button("💾 Save Investment", disabled=button_disabled):
    if total_investment == 0:
        st.warning("⚠️ Please enter some investment before saving.")
    else:
        st.session_state.saving_investment = True
        st.rerun()


# ================================
# PROCESS SAVE AFTER RERUN
# ================================
if st.session_state.saving_investment:
    try:
        supabase.rpc(
            "save_investment_atomic",
            {
                "p_team_name": st.session_state.team_name,
                "p_round": current_round,
                "p_ingredients": ingredient_entries,
                "p_capacity": capacity_entries,
                "p_total": total_investment
            }
        ).execute()

        # Reset the saving flag so button becomes active again
        st.session_state.saving_investment = False

        st.success("✅ Investment saved successfully!")
        st.rerun()

    except Exception as e:
        st.session_state.saving_investment = False
        st.error("❌ Failed to save investment.")
        st.exception(e)


# ============================
# 📜 INVESTMENT HISTORY
# ============================
st.markdown("---")
st.subheader("📜 Round Investment History")

try:
    response = (
        supabase.table("investments")
        .select("*")
        .eq("team_name", st.session_state.team_name)
        .order("round_number", desc=True)
        .execute()
    )
    investments = response.data

    if not investments:
        st.info("No previous investments found yet.")
    else:
        inv_df = pd.DataFrame(investments)

        # Group by ROUND instead of by DATE
        for rnd, group in inv_df.groupby("round_number", sort=False):

            total_round_cost = group["total_cost_usd"].sum()
            if total_round_cost <= 0:
                continue

            with st.expander(f"🎯 Round {rnd} — Total Spent: ${total_round_cost:,.2f}"):
                all_ingredients, all_capacity = [], []

                # Collect JSON entries
                for _, inv in group.iterrows():
                    try:
                        all_ingredients.extend(json.loads(inv["ingredients_json"]))
                        all_capacity.extend(json.loads(inv["capacity_json"]))
                    except:
                        pass

                # ============================
                # 🧺 INGREDIENTS PURCHASED
                # ============================
                if all_ingredients:
                    ing_df = pd.DataFrame(all_ingredients)
                    ing_df = ing_df[(ing_df["buy_qty"] > 0) & (ing_df["subtotal_usd"] > 0)]

                    if not ing_df.empty:
                        st.markdown("**🧺 Ingredients Purchased:**")

                        ing_df["Ingredient (unit)"] = ing_df.apply(
                            lambda x: f"{x['ingredient']} ({x['unit']})", axis=1
                        )

                        ingredient_order = [
                            f"{row['ingredient']} ({row['unit']})"
                            for _, row in ingredients.iterrows()
                        ]

                        ing_grouped = (
                            ing_df.groupby("Ingredient (unit)", as_index=False)[["buy_qty", "subtotal_usd"]]
                            .sum()
                        )

                        ing_display = (
                            ing_grouped.set_index("Ingredient (unit)")
                                       .reindex(ingredient_order)
                                       .dropna(how="all")
                                       .reset_index()
                        )

                        ing_display["subtotal_usd"] = ing_display["subtotal_usd"].round(2)

                        ing_display = ing_display.rename(
                            columns={
                                "Ingredient (unit)": "Ingredient",
                                "buy_qty": "Quantity",
                                "subtotal_usd": "Subtotal (USD)",
                            }
                        )

                        ing_display["Quantity"] = ing_display["Quantity"].map(lambda x: f"{x:g}")
                        ing_display["Subtotal (USD)"] = ing_display["Subtotal (USD)"].map(
                            lambda x: f"${x:,.2f}"
                        )

                        st.data_editor(
                            ing_display,
                            use_container_width=True,
                            hide_index=True,
                            disabled=True,
                            key=f"history_ing_round_{rnd}",
                        )

                # ============================
                # 🏭 CAPACITY PURCHASED
                # ============================
                if all_capacity:
                    cap_df = pd.DataFrame(all_capacity)
                    cap_df = cap_df[(cap_df["hours"] > 0) & (cap_df["subtotal_usd"] > 0)]

                    if not cap_df.empty:
                        st.markdown("**🏭 Capacity Purchased:**")

                        cap_df["Capacity (unit)"] = cap_df["display_name"] + " (hours)"


                        capacity_order = [name + " (hours)" for name in capacity_cost_lookup.keys()]



                        cap_grouped = (
                            cap_df.groupby("Capacity (unit)", as_index=False)[["hours", "subtotal_usd"]]
                                 .sum()
                        )

                        cap_display = (
                            cap_grouped.set_index("Capacity (unit)")
                                       .reindex(capacity_order)
                                       .dropna(how="all")
                                       .reset_index()
                        )

                        cap_display["subtotal_usd"] = cap_display["subtotal_usd"].round(2)

                        cap_display = cap_display.rename(
                            columns={
                                "Capacity (unit)": "Capacity",
                                "hours": "Hours Purchased",
                                "subtotal_usd": "Subtotal (USD)",
                            }
                        )

                        cap_display["Hours Purchased"] = cap_display["Hours Purchased"].map(lambda x: f"{x:g}")
                        cap_display["Subtotal (USD)"] = cap_display["Subtotal (USD)"].map(
                            lambda x: f"${x:,.2f}"
                        )

                        st.data_editor(
                            cap_display,
                            use_container_width=True,
                            hide_index=True,
                            disabled=True,
                            key=f"history_cap_round_{rnd}",
                        )

except Exception as e:
    st.error("❌ Failed to load investment history.")
    st.exception(e)



# ============================
# 🚪 LOGOUT
# ============================
if st.button("🚪 Log out"):
    st.session_state.clear()
    st.success
