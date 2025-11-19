# -*- coding: utf-8 -*-
"""
Leaderboard Page - Cake Simulation (Round-based)
"""

import streamlit as st
import pandas as pd
import os
from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path


st.set_page_config(page_title="🏆 Leaderboard", page_icon="🥇", layout="wide")


# =====================================
# 🌍 LOAD ENV
# =====================================
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("❌ Missing Supabase credentials. Check .env file.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =====================================
# 🎯 ROUND BANNER
# =====================================
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
    unsafe_allow_html=True,)

# =====================================
# 🔒 LOGIN CHECK
# =====================================
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Please log in first.")
    st.stop()

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
# 🏆 HEADER
# =====================================
st.title("🏆 Cake Business Leaderboard")
st.markdown(f"🎯 **Current Round: {current_round}**")
st.write(f"Welcome, **{st.session_state.team_name}** 👋")

# =====================================
# 🔍 OPTIONAL: Select Round to View
# =====================================
st.subheader("📅 View Leaderboard by Round")

selected_round = st.number_input(
    "Select Round",
    min_value=1,
    max_value=current_round,
    value=current_round,
    step=1
)

st.markdown(f"📊 Showing results up to **Round {selected_round}**")

# =====================================
# 📊 LOAD LEADERBOARD DATA
# =====================================
try:
    response = supabase.table("teams").select("*").execute()
    teams = pd.DataFrame(response.data)

    if teams.empty:
        st.info("No team data found yet.")
    else:

        # if your system stores total value directly:
        if "total_value" in teams.columns:
            teams["Total Value 💰"] = teams["total_value"]
        else:
            teams["Total Value 💰"] = teams["money"] + teams["stock_value"]

        # Rank
        teams = teams.sort_values(by="Total Value 💰", ascending=False).reset_index(drop=True)
        medals = ["🥇", "🥈", "🥉"]
        teams.insert(0, "Rank", [medals[i] if i < 3 else str(i + 1) for i in range(len(teams))])

        teams["Total Value 💰"] = teams["Total Value 💰"].map(lambda x: f"${x:,.2f}")

        st.dataframe(
            teams[["Rank", "team_name", "Total Value 💰"]].rename(columns={
                "team_name": "Team"
            }),
            use_container_width=True,
            height=500
        )

except Exception as e:
    st.error("❌ Failed to load leaderboard data.")
    st.exception(e)

# =====================================
# 🚪 LOGOUT
# =====================================
st.divider()
if st.button("🚪 Log out"):
    st.session_state.clear()
    st.success("Logged out.")
    st.switch_page("Login.py")
