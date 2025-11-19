#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 14:47:27 2025
Login Page — Cake Business Simulation
@author: Fatima
"""

import base64
import streamlit as st
from supabase import create_client
import os
from dotenv import load_dotenv
from pathlib import Path
import bcrypt

# ======================================
# ⚙️ INITIAL SETUP
# ======================================
st.set_page_config(page_title="Cake Business Game", page_icon="🎂", layout="centered")

# Session defaults
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

# Hide sidebar for logged-out users
if not st.session_state.logged_in:
    st.sidebar.empty()

# ======================================
# 🌍 LOAD SUPABASE CREDENTIALS
# ======================================
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("❌ Supabase credentials missing. Please check your .env file.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# ======================================
# 🖼️ BACKGROUND IMAGE
# ======================================
def add_bg_from_local(image_file: str):
    """Add a background image to Streamlit app."""
    image_path = Path(__file__).parent / image_file
    if not image_path.exists():
        st.error(f"🚫 Image not found at {image_path}")
        return

    with open(image_path, "rb") as image:
        encoded = base64.b64encode(image.read()).decode()

    st.markdown(
        f"""
        <style>
        .stApp {{
            background: url("data:image/jpeg;base64,{encoded}") no-repeat center center fixed;
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

add_bg_from_local("assets/cakebg.jpg")

# ======================================
# 🎨 CUSTOM STYLING
# ======================================
st.markdown(
    """
    <style>
    /* ===== Global text size ===== */
    html, body, [class*="css"] {
        font-size: 12px !important;
    }

    /* ===== Background overlay ===== */
    .stApp {
        background-color: rgba(0, 0, 0, 0.35);
        background-blend-mode: overlay;
    }

    /* ===== Title styling ===== */
    h1, h2, h3, h4, h5, h6 {
        color: #FFF8E7 !important;
        text-shadow: 1px 1px 3px rgba(60, 40, 20, 0.6);
        font-family: 'Poppins', sans-serif;
    }

    /* ===== General text ===== */
    .stMarkdown, label, p, span, div {
        color: #FFF9F1 !important;
    }

    /* ===== Sidebar styling ===== */
    section[data-testid="stSidebar"] {
    min-width: 400px !important;   /* was ~290px */
    max-width: 400px !important;
}

    section[data-testid="stSidebar"] > div:first-child {
    width: 400px !important;
}
    section[data-testid="stSidebar"] {
        background-color: rgba(255, 255, 255, 0.75);
    }
    section[data-testid="stSidebar"] * {
        color: #4E2C00 !important;
        font-weight: 600 !important;
    }

    /* ===== Input fields ===== */
    input, textarea {
        background-color: rgba(255, 249, 240, 0.85) !important;
        color: #3B2C1A !important;
        border: 1px solid #CDAE82 !important;
        border-radius: 8px !important;
        font-size: 1.4rem !important;
        padding: 0.4rem 0.6rem !important;
    }

    /* ===== Login Form Labels ===== */
    div.stTextInput label {
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        color: #FFDFA6 !important; /* soft golden cream */
        text-shadow: 1px 1px 3px rgba(60, 40, 20, 0.6);
    }

    /* ===== Input Placeholder Text ===== */
    div.stTextInput input::placeholder {
        font-size: 1.6em !important;
        color: rgba(60, 40, 20, 0.6) !important;
    }

    /* ===== Buttons ===== */
    button[kind="primary"], div.stButton > button {
        background-color: #F5D2A4 !important;
        color: #4B2E05 !important;
        border-radius: 10px !important;
        border: 1px solid #C68E53 !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        padding: 0.6rem 1.2rem !important;
        box-shadow: 0px 2px 6px rgba(0, 0, 0, 0.25);
        transition: all 0.2s ease-in-out;
    }

    button[kind="primary"]:hover, div.stButton > button:hover {
        background-color: #E0B070 !important;
        transform: scale(1.03);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ======================================
# 🔐 LOGIN LOGIC
# ======================================
if not st.session_state.logged_in:

    st.title("🎂 Cake Business Simulation - Login")

    team_name = st.text_input("Team Name")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        response = supabase.table("teams").select("*").eq("team_name", team_name).execute()

        if response.data:
            team = response.data[0]
            stored_hash = team["password"].encode("utf-8")
            entered_pw = password.encode("utf-8")

            if bcrypt.checkpw(entered_pw, stored_hash):

                # ==========================
                # SUCCESSFUL LOGIN
                # ==========================
                st.session_state.logged_in = True
                st.session_state.team_name = team_name
                st.session_state.money = team["money"]

                # ---- NEW: detect admin team ----
                if team_name.lower() == "admin":
                    st.session_state.is_admin = True
                else:
                    st.session_state.is_admin = False

                st.session_state.game_day = st.session_state.get("game_day", 1)
                st.session_state.day = st.session_state.game_day

                st.success(f"Welcome back, {team_name}!")

                # Redirect based on role
                if st.session_state.is_admin:
                    st.switch_page("pages/Admin.py")
                else:
                    st.switch_page("pages/4_Leaderboard.py")

            else:
                st.error("Incorrect password. Please try again.")
        else:
            st.error("Team not found. Please contact your instructor.")

else:
    st.title("✅ You’re already logged in!")
    st.write(f"Welcome back, **{st.session_state.team_name}** 🎂")
    st.write(f"Current balance: **${st.session_state.money:,.2f}**")