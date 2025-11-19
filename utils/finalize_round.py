import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv
import pytz


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
    
        # Does the team have a price row for the current round?
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
    
            # Insert copy as new price row for current round
            supabase.table("prices").insert({
                "team_name": team,
                "prices_json": last["prices_json"],
                "round_number": round_number,
                "finalized": True,            # treat as final
                "auto_filled": True,          # mark as carried forward
                "copied_from_round": last["round_number"]
            }).execute()
    
            print(f"🔁 Auto-filled prices for {team} from Round {last['round_number']} → Round {round_number}")
    
        else:
            # No history at all → insert an empty record
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

    # Production Plans for this round
    plans_resp = supabase.table("production_plans").select("*").eq("round_number", round_number).execute()
    plans_df = pd.DataFrame(plans_resp.data or [])


    # === Load latest submitted prices per team (fallback logic) ===
    price_history_resp = supabase.table("prices") \
        .select("*") \
        .lte("round_number", round_number) \
        .order("round_number", desc=True) \
        .execute()
    
    price_history = price_history_resp.data or []
    if price_history:
        # Keep only the LAST submitted row per team
        latest_price_per_team = {}
        for row in price_history:
            team_name = row["team_name"]
            if team_name not in latest_price_per_team:
                latest_price_per_team[team_name] = row
    
        # Flatten JSON into usable rows
        price_rows = []
        for rec in latest_price_per_team.values():
            for item in json.loads(rec.get("prices_json", "[]")):
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

    # Demand parameters
    demand_params = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "..", "data", "instructor_demand_competition.csv")
    )

    # Channels (transport cost)
    ch_resp = supabase.table("channels").select("channel, transport_cost_per_unit_usd").execute()
    ch_df = pd.DataFrame(ch_resp.data or [])
    ch_map = dict(zip(ch_df["channel"], ch_df["transport_cost_per_unit_usd"]))

    # Ingredients and wages
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    ingredients_df = pd.read_csv(os.path.join(data_dir, "ingredients.csv"))
    wages_df = pd.read_csv(os.path.join(data_dir, "wages_energy.csv"))

    ing_cost_map = {
        row["ingredient"].lower(): float(row["unit_cost_usd"])
        for _, row in ingredients_df.iterrows()
    }

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

    # Recipes
    recipes_data = supabase.table("recipes").select("*").execute().data
    recipes_df = pd.DataFrame(recipes_data or [])
    recipes_df.columns = [c.lower() for c in recipes_df.columns]

    # === Helper: ingredient requirements ===
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

    if not price_df.empty:
        avg_price = (
            price_df.groupby(["channel", "cake"])["price_usd"]
            .mean()
            .to_dict()
        )
    else:
        avg_price = {}
    # ============================================================
    # PROCESS TEAMS WITH PRODUCTION PLANS
    # ============================================================
    if not plans_df.empty:
        for team in plans_df["team_name"].unique():

            team_plan = plans_df[plans_df["team_name"] == team].iloc[0]
            plan_json = json.loads(team_plan["plan_json"])
            required_json = json.loads(team_plan["required_json"])

            team_prices = price_df[price_df["team_name"] == team]

            total_profit = 0.0
            total_transport = 0.0

            # Ingredient costs
            ing_needs = compute_required_ingredients(plan_json)
            ing_cost = sum(qty * ing_cost_map.get(ing.lower(), 0)
                           for ing, qty in ing_needs.items())

            # Capacity costs
            cap_cost = sum(float(hours) * wage_map.get(cap.lower(), 0)
                           for cap, hours in required_json.items())

            total_resource_cost = ing_cost + cap_cost

            # Sales / Profit
            for item in plan_json:
                cake = item["cake"]
                channel = item["channel"]
                qty = float(item["qty"])

                my_price = float(
                    team_prices[
                        (team_prices["cake"] == cake) &
                        (team_prices["channel"] == channel)
                    ]["price_usd"].fillna(0).values[0]
                    if not team_prices.empty else 0
                )

                params = demand_params[
                    (demand_params["cake_name"] == cake) &
                    (demand_params["channel"] == channel)
                ]
                if params.empty:
                    continue

                alpha, beta, gamma = params["alpha"].iloc[0], params["beta"].iloc[0], params["gamma_competition"].iloc[0]
                avg_p = avg_price.get((channel, cake), my_price)

                demand = max(0, alpha - beta * my_price + gamma * (avg_p - my_price))
                sold = min(qty, demand)

                revenue = sold * my_price
                transport = sold * ch_map.get(channel, 0)

                total_profit += revenue - transport
                total_transport += transport

            # Update team financials
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
                "last_finalized_round": round_number
            }).eq("team_name", team).execute()

            # Update plan profit
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
