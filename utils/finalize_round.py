import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv
import pytz
import math


BEIRUT_TZ = pytz.timezone("Asia/Beirut")
def init_supabase():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

def finalize_round(round_number: int):
    """Finalize profits for a specific game round (round-based simulation)."""
    supabase = init_supabase()

    # --- Idempotency Guard ---
    teams_resp = supabase.table("teams").select("team_name, last_finalized_round").execute()
    teams_data = teams_resp.data or []
    if teams_data and all((t.get("last_finalized_round") == round_number) for t in teams_data):
        print(f"Round {round_number} already finalized. Skipping.")
        return

    print(f"📅 Finalizing Round {round_number}")

    # =====================================================================
    # 🔄 Ensure all teams have a price entry for the current round
    # =====================================================================
    all_teams = supabase.table("teams").select("team_name").execute().data or []

    for row in all_teams:
        team = row["team_name"]

        current_price = (
            supabase.table("prices")
            .select("*")
            .eq("team_name", team)
            .eq("round_number", round_number)
            .execute()
        )
        has_current_price = (
            current_price is not None
            and hasattr(current_price, "data")
            and isinstance(current_price.data, list)
            and len(current_price.data) > 0
        )
        if has_current_price:
            continue

        # Find most recent earlier round
        prev_price = (
            supabase.table("prices")
            .select("*")
            .eq("team_name", team)
            .lt("round_number", round_number)
            .order("round_number", desc=True)
            .limit(1)
            .execute()
            .data
        )

        if prev_price:
            last = prev_price[0]

            supabase.table("prices").insert({
                "team_name": team,
                "prices_json": last["prices_json"],
                "round_number": round_number,
                "finalized": True,
                "auto_filled": True,
                "copied_from_round": last["round_number"]
            }).execute()

            print(f"🔁 Auto-filled prices for {team} from Round {last['round_number']} → Round {round_number}")

        else:
            supabase.table("prices").insert({
                "team_name": team,
                "prices_json": "[]",
                "round_number": round_number,
                "finalized": True,
                "auto_filled": True,
                "copied_from_round": None
            }).execute()

            print(f"⚠️ {team} has no prior prices — inserted empty price list.")

    # === LOAD DATA FOR THIS ROUND ===

    plans_resp = supabase.table("production_plans").select("*").eq("round_number", round_number).execute()
    plans_df = pd.DataFrame(plans_resp.data or [])

    # === Build map of cakes produced this round ===
    team_producing_map = {}
    if not plans_df.empty:
        for _, row in plans_df.iterrows():
            team = row["team_name"]
            raw_plan = row["plan_json"]
            plan_json = json.loads(raw_plan) if isinstance(raw_plan, str) else raw_plan or []
            cakes = {item["cake"] for item in plan_json}
            team_producing_map[team] = cakes

    # === Load latest submitted prices per team ===
    price_history_resp = supabase.table("prices") \
        .select("*") \
        .lte("round_number", round_number) \
        .order("round_number", desc=True) \
        .execute()

    price_history = price_history_resp.data or []
    if price_history:
        latest_price_per_team = {}
        for row in price_history:
            team_name = row["team_name"]
            if team_name not in latest_price_per_team:
                latest_price_per_team[team_name] = row

        # Flatten JSON
        price_rows = []
        for rec in latest_price_per_team.values():
            prices_raw = rec.get("prices_json", [])
            prices_list = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            for item in prices_list:
                price_rows.append({
                    "team_name": rec["team_name"],
                    "channel": item["channel"],
                    "cake": item["cake"],
                    "price_usd": item["price_usd"],
                    "round_used": rec["round_number"]
                })

        price_df = pd.DataFrame(price_rows)
    else:
        price_df = pd.DataFrame()

    # === Compute avg_price using only producing teams ===
    if not price_df.empty:
        price_df_filtered = []
        for _, row in price_df.iterrows():
            team = row["team_name"]
            cake = row["cake"]

            # Keep only if team produces that cake this round
            if team in team_producing_map and cake in team_producing_map[team]:
                price_df_filtered.append(row)

        price_df_filtered = pd.DataFrame(price_df_filtered)

        if not price_df_filtered.empty:
            avg_price = (
                price_df_filtered.groupby(["channel", "cake"])["price_usd"]
                .mean()
                .to_dict()
            )
        else:
            avg_price = {}
    else:
        avg_price = {}

    # Load demand + costs
    demand_params = pd.read_csv(os.path.join(os.path.dirname(__file__), "..", "data", "instructor_demand_competition.csv"))
    ch_resp = supabase.table("channels").select("channel, transport_cost_per_unit_usd").execute()
    ch_df = pd.DataFrame(ch_resp.data or [])
    ch_map = dict(zip(ch_df["channel"], ch_df["transport_cost_per_unit_usd"]))

    ingredients_df = pd.read_csv(os.path.join(os.path.dirname(__file__), "..", "data", "ingredients.csv"))
    wages_df = pd.read_csv(os.path.join(os.path.dirname(__file__), "..", "data", "wages_energy.csv"))

    ing_cost_map = {row["ingredient"].lower(): float(row["unit_cost_usd"]) for _, row in ingredients_df.iterrows()}
    wage_param_map = {
        "prep": "prep_wage_usd_per_hour",
        "oven": "oven_wage_usd_per_hour",
        "package": "package_wage_usd_per_hour",
        "oven rental": "oven_rental_wage_usd_per_hour",
    }
    wage_map = {
        key: float(wages_df[wages_df["parameter"] == param]["value"].iloc[0])
        for key, param in wage_param_map.items()
        if not wages_df[wages_df["parameter"] == param].empty
    }

    recipes_data = supabase.table("recipes").select("*").execute().data
    recipes_df = pd.DataFrame(recipes_data or [])
    recipes_df.columns = [c.lower() for c in recipes_df.columns]

    cakes_resp = supabase.table("cakes").select("name, packaging_cost_per_unit_usd").execute()
    cakes_df = pd.DataFrame(cakes_resp.data or [])
    packaging_map = dict(zip(cakes_df["name"], cakes_df["packaging_cost_per_unit_usd"]))

    def compute_required_ingredients(plan_json):
        if not plan_json or recipes_df.empty:
            return {}
        df = pd.DataFrame(plan_json)
        if df.empty or "cake" not in df:
            return {}
        df.columns = [c.lower() for c in df.columns]
        totals = df.groupby("cake")["qty"].sum().to_dict()
        needs = {}
        for cake, qty in totals.items():
            recipe = recipes_df[recipes_df["name"].str.lower() == str(cake).lower()]
            if recipe.empty:
                continue
            row = recipe.iloc[0]
            for col in row.index:
                if col in ["id", "cake_id", "name", "created_at"]:
                    continue
                usage = qty * float(row[col])
                needs[col] = needs.get(col, 0) + usage
        return needs

    # ============================================================
    # PROCESS TEAMS WITH PLANS
    # ============================================================
    if not plans_df.empty:
        for team in plans_df["team_name"].unique():

            team_plan = plans_df[plans_df["team_name"] == team].iloc[0]
            raw_plan = team_plan["plan_json"]
            plan_json = json.loads(raw_plan) if isinstance(raw_plan, str) else raw_plan or []

            raw_required = team_plan["required_json"]
            required_json = json.loads(raw_required) if isinstance(raw_required, str) else raw_required or {}

            team_prices = price_df[price_df["team_name"] == team]

            total_profit = 0.0
            total_transport = 0.0
            total_packaging_cost = 0

            ing_needs = compute_required_ingredients(plan_json)
            ing_cost = sum(qty * ing_cost_map.get(ing.lower(), 0) for ing, qty in ing_needs.items())

            cap_cost = sum(float(hours) * wage_map.get(cap.lower(), 0) for cap, hours in required_json.items())
            total_resource_cost = ing_cost + cap_cost

            # ============================================================
            # SALES & PROFIT CALCULATION
            # ============================================================
            cakes_produced = {item["cake"] for item in plan_json}

            for item in plan_json:
                cake = item["cake"]
                channel = item["channel"]
                qty = math.floor(item["qty"])

                # Determine price logic
                subset = team_prices[
                    (team_prices["cake"] == cake) &
                    (team_prices["channel"] == channel)
                ]

                if cake not in cakes_produced:
                    my_price = float(avg_price.get((channel, cake), 0))
                elif subset.empty:
                    my_price = float(avg_price.get((channel, cake), 0))
                else:
                    my_price = float(subset["price_usd"].iloc[0])

                # Demand model
                params = demand_params[
                    (demand_params["cake_name"] == cake) &
                    (demand_params["channel"] == channel)
                ]
                if params.empty:
                    continue

                alpha = params["alpha"].iloc[0]
                beta = params["beta"].iloc[0]
                gamma = params["gamma_competition"].iloc[0]

                avg_p = avg_price.get((channel, cake), my_price)

                demand = max(0, math.floor(alpha - beta * my_price + gamma * (avg_p - my_price)))
                sold = min(math.floor(qty), demand)

                revenue = sold * my_price
                transport = sold * ch_map.get(channel, 0)
                packaging_cost = sold * float(packaging_map.get(cake, 0))

                total_profit += revenue - transport - packaging_cost
                total_transport += transport
                total_packaging_cost += packaging_cost

            team_data = supabase.table("teams").select("money, stock_value").eq("team_name", team).execute().data[0]

            new_money = float(team_data["money"]) + total_profit
            new_stock = max(float(team_data["stock_value"]) - total_resource_cost, 0)
            total_value = new_money + new_stock

            supabase.table("teams").update({
                "money": new_money,
                "stock_value": new_stock,
                "total_value": total_value,
                "last_profit": total_profit,
                "last_transport_cost": total_transport,
                "last_resource_cost": total_resource_cost,
                "last_packaging_cost": total_packaging_cost,
                "last_finalized_round": round_number
            }).eq("team_name", team).execute()

            supabase.table("production_plans").update({
                "profit_usd": total_profit
            }).eq("team_name", team).eq("round_number", round_number).execute()

    # ============================================================
    # TEAMS WITHOUT PLANS (carry forward)
    # ============================================================
    all_teams = supabase.table("teams").select("*").execute().data
    submitted = set(plans_df["team_name"].unique()) if not plans_df.empty else set()

    for team in [t["team_name"] for t in all_teams if t["team_name"] not in submitted]:
        data = next(t for t in all_teams if t["team_name"] == team)
        money = float(data["money"])
        stock = float(data["stock_value"])
        total_value = money + stock

        supabase.table("teams").update({
            "total_value": total_value,
            "money": money,
            "stock_value": stock,
            "last_finalized_round": round_number
        }).eq("team_name", team).execute()

    print(f"✅ Round {round_number} finalized.")

