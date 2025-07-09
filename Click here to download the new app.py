
import streamlit as st
from data import create_tables, update_recent_games, run_trend_discovery, get_top_trends_for_today, get_todays_games, get_all_trends

st.set_page_config(layout="wide")
st.title("📊 MLB Betting Trend Engine")

# Buttons
if st.button("🔄 Refresh Data and Trends"):
    create_tables()
    update_recent_games(30)
    run_trend_discovery()
    st.success("Database and trends updated!")

# --- Trend Table (All Trends) ---
st.subheader("📈 All Available Trends (Global Backtested)")
all_trends = get_all_trends()
if all_trends:
    st.dataframe(all_trends, use_container_width=True)
else:
    st.info("No trends found yet. Click 'Refresh' above to generate trends.")

# --- Today's Games & Matching Trends ---
st.subheader("🧠 Today's Games & Matching Trends")
games = get_todays_games()
trends_map = get_top_trends_for_today()

for game in games:
    st.markdown(f"### ⚾ {game['away_team']} @ {game['home_team']}")
    game_trends = trends_map.get(game['game_pk'], [])
    if game_trends:
        for t in game_trends:
            st.markdown(f"- 🧪 **{t['description']}** — {t['win_pct']:.1f}% win rate (Sample: {t['sample_size']})")
    else:
        st.write("No strong trends found for this game.")
