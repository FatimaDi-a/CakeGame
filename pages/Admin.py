
# -*- coding: utf-8 -*-
"""
Admin Control Panel — Cake Simulation (Round-based)
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path
from utils.finalize_round import finalize_round





# =====================================
# LOAD ENV + SUPABASE
# =====================================
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("❌ Missing Supabase credentials.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =====================================
# GET/SET CURRENT ROUND
# =====================================
def get_current_round():
    resp = supabase.table("game_state").select("value").eq("key", "current_round").single().execute()
    return int(resp.data["value"])

def set_current_round(new_round: int):
    supabase.table("game_state").update({"value": str(new_round)}).eq("key", "current_round").execute()


# =====================================
# ADMIN LOGIN CHECK
# =====================================
if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

st.title("🔧 Admin Control Panel")

if not st.session_state.is_admin:
    st.warning("Admin login required.")

    admin_user = st.text_input("Username")
    admin_pass = st.text_input("Password", type="password")

    if st.button("Login"):
        if admin_user == os.getenv("ADMIN_USER") and admin_pass == os.getenv("ADMIN_PASS"):
            st.session_state.is_admin = True
            st.success("Logged in as admin.")
            st.rerun()
        else:
            st.error("Incorrect admin credentials.")
    st.stop()


# =====================================
# SUCCESSFUL ADMIN LOGIN
# =====================================
st.success("Admin privileges granted.")

current_round = get_current_round()
st.header(f"🎯 Current Round: **{current_round}**")

st.markdown("---")

# =====================================
# 🔁 ADVANCE TO NEXT ROUND
# =====================================
st.subheader("➡️ Advance to Next Round")

if st.button("📈 Move to Round " + str(current_round + 1)):
    finalize_round(current_round)
    new_round = current_round + 1
    set_current_round(new_round)
    supabase.table("teams") \
    .update({"round_number": new_round}) \
    .neq("team_name", "") \
    .execute()

    st.success(f"Successfully advanced to Round {current_round + 1}.")
    st.rerun()

# =====================================
# ⬅️ REOPEN PREVIOUS ROUND
# =====================================
st.subheader("↩️ Reopen Previous Round")

if current_round > 1:
    if st.button("🔓 Reopen Round " + str(current_round - 1)):
        set_current_round(current_round - 1)
        st.info(f"Round reverted back to {current_round - 1}. You may re-enable submissions.")
        st.rerun()
else:
    st.info("Cannot reopen before Round 1.")

st.markdown("---")

# =====================================
# 🔒 LOCK / UNLOCK SUBMISSIONS
# =====================================

st.subheader("🔐 Control Submissions")

lock_state = supabase.table("game_state").select("value").eq("key", "locked").single().execute()
locked = lock_state.data["value"] == "true" if lock_state.data else False

if locked:
    st.success("Submissions are currently **LOCKED**.")
    if st.button("🔓 Unlock Submissions"):
        supabase.table("game_state").update({"value": "false"}).eq("key", "locked").execute()
        st.success("Submissions unlocked.")
        st.rerun()
else:
    st.warning("Submissions are currently **OPEN**.")
    if st.button("🔒 Lock Submissions"):
        supabase.table("game_state").update({"value": "true"}).eq("key", "locked").execute()
        st.success("Submissions locked.")
        st.rerun()

st.markdown("---")

# =====================================
# 📊 VIEW ALL ROUND SUBMISSIONS
# =====================================
st.subheader("📂 View Data by Round")

selected = st.number_input("Select Round", min_value=1, max_value=current_round, value=current_round)

col1, col2 = st.columns(2)

with col1:
    st.write("### 💲 Prices")
    try:
        price_data = supabase.table("prices").select("*").eq("round_number", selected).execute().data
        if price_data:
            st.dataframe(pd.DataFrame(price_data), use_container_width=True)
        else:
            st.info("No prices found for this round.")
    except:
        st.error("Failed to load price data.")

with col2:
    st.write("### 📦 Production Plans")
    try:
        prod_data = supabase.table("production_plans").select("*").eq("round_number", selected).execute().data
        if prod_data:
            st.dataframe(pd.DataFrame(prod_data), use_container_width=True)
        else:
            st.info("No production plans found for this round.")
    except:
        st.error("Failed to load production plan data.")

st.markdown("---")

# =====================================
# 🔄 RESET A SPECIFIC ROUND
# =====================================
st.subheader("🗑️ Reset a Specific Round")

reset_round = st.number_input("Round to reset", min_value=1, max_value=current_round)

if st.button("❗ Delete All Data for This Round"):
    try:
        supabase.table("prices").delete().eq("round_number", reset_round).execute()
        supabase.table("demands").delete().eq("round_number", reset_round).execute()
        supabase.table("production_plans").delete().eq("round_number", reset_round).execute()
        supabase.table("investments").delete().eq("round_number", reset_round).execute()
        st.success(f"All data for round {reset_round} has been cleared.")
    except Exception as e:
        st.error("Failed to reset round.")
        st.exception(e)

st.markdown("---")



# =====================================
# LOGOUT
# =====================================
st.markdown("---")
if st.button("🚪 Log out"):
    st.session_state.clear()
    st.success("Logged out.")
    st.switch_page("Login.py")
