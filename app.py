import streamlit as st
import data

st.set_page_config(page_title="MLB Trend Discovery", layout="wide")

st.title("MLB Automated Trend Discovery Engine")

# Create tables if not exist
data.create_tables()

# Refresh button triggers data update and trend recomputation
if st.button("Refresh Data and Trends"):
    with st.spinner("Refreshing MLB data and recalculating trends..."):
        data.update_recent_games(days=7)
        data.run_trend_discovery()
    st.success("Data refreshed!")

# Display today's games and matching trends
st.header("Today's MLB Games & Top Trends")
games_today = data.get_todays_games()
trends = data.get_top_trends_for_today()

for game in games_today:
    st.subheader(f"{game['away_team']} @ {game['home_team']} ({game['game_date']})")
    matching = trends.get(game['game_pk'], [])
    if matching:
        for t in matching:
            st.markdown(f"- **{t['description']}** ({t['bet_type']}) — Win%: {t['win_pct']:.1f}%, Sample: {t['sample_size']}")
    else:
        st.write("No strong trends found for this game.")

# Optional: Show all discovered trends in a table with filters
st.header("All Discovered Trends")
all_trends = data.get_all_trends()
import pandas as pd
df = pd.DataFrame(all_trends)
df = df.sort_values(by=['win_pct', 'sample_size'], ascending=[False, False])
st.dataframe(df)
