
import sqlite3
import requests
import datetime

DB_PATH = "mlb_trends.db"
MLB_API_BASE = "https://statsapi.mlb.com/api/v1"

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS games (
        game_pk INTEGER PRIMARY KEY,
        game_date TEXT,
        home_team TEXT,
        away_team TEXT,
        home_score INTEGER,
        away_score INTEGER,
        home_odds REAL,
        away_odds REAL,
        run_line REAL,
        total_line REAL
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT,
        bet_type TEXT,
        win_pct REAL,
        sample_size INTEGER,
        game_pk INTEGER
    )
    """)
    conn.commit()
    conn.close()

def update_recent_games(days=7):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days)
    url = f"{MLB_API_BASE}/schedule?sportId=1&startDate={start_date}&endDate={today}&expand=schedule.linescore"
    resp = requests.get(url)
    data = resp.json()
    for day in data.get("dates", []):
        for g in day.get("games", []):
            game_pk = g["gamePk"]
            game_date = day["date"]
            home_team = g["teams"]["home"]["team"]["name"]
            away_team = g["teams"]["away"]["team"]["name"]
            linescore = g.get("linescore", {})
            home_score = linescore.get("teams", {}).get("home", {}).get("runs")
            away_score = linescore.get("teams", {}).get("away", {}).get("runs")
            home_odds = None
            away_odds = None
            run_line = None
            total_line = None
            c.execute("""
            INSERT OR REPLACE INTO games (game_pk, game_date, home_team, away_team, home_score, away_score, home_odds, away_odds, run_line, total_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_pk, game_date, home_team, away_team, home_score, away_score, home_odds, away_odds, run_line, total_line))
    conn.commit()
    conn.close()

def run_trend_discovery():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM trends")

    # Pull 3 years of games
    c.execute("SELECT game_pk, game_date, home_team, away_team, home_score, away_score FROM games ORDER BY game_date")
    rows = c.fetchall()

    team_loss_streaks = {}
    matching_games = []

    for row in rows:
        game_pk, game_date, home_team, away_team, home_score, away_score = row
        date_obj = datetime.datetime.strptime(game_date, "%Y-%m-%d").date()
        weekday = date_obj.strftime("%A")

        # Skip games with missing scores
        if home_score is None or away_score is None:
            continue

        # Track loss streaks
        if home_score < away_score:
            team_loss_streaks[home_team] = team_loss_streaks.get(home_team, 0) + 1
            team_loss_streaks[away_team] = 0
        else:
            team_loss_streaks[home_team] = 0
            team_loss_streaks[away_team] = team_loss_streaks.get(away_team, 0) + 1

        # Trend: Home teams after 2 losses on Sunday
        if team_loss_streaks.get(home_team, 0) >= 2 and weekday == "Sunday":
            win = home_score > away_score
            matching_games.append((game_pk, win))

    
    # === Trend 1: Away teams on 3-game losing streaks on Tuesdays ===
    away_streaks = {}
    tuesday_away_losses = []
    for row in rows:
        game_pk, game_date, home_team, away_team, home_score, away_score = row
        date_obj = datetime.datetime.strptime(game_date, "%Y-%m-%d").date()
        weekday = date_obj.strftime("%A")
        if home_score is None or away_score is None:
            continue

        # Track away team losing streaks
        if away_score < home_score:
            away_streaks[away_team] = away_streaks.get(away_team, 0) + 1
        else:
            away_streaks[away_team] = 0

        if away_streaks[away_team] >= 3 and weekday == "Tuesday":
            win = away_score > home_score
            tuesday_away_losses.append((game_pk, win))

    if tuesday_away_losses:
        wins = sum(1 for _, win in tuesday_away_losses if win)
        total = len(tuesday_away_losses)
        win_pct = 100.0 * wins / total
        c.execute("""
        INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
        VALUES (?, ?, ?, ?, NULL)
        """, ("Away teams on 3-game losing streaks on Tuesday", "ML", win_pct, total))
        for game_pk, win in tuesday_away_losses:
            c.execute("""
            INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
            VALUES (?, ?, ?, ?, ?)
            """, ("Away teams on 3-game losing streaks on Tuesday", "ML", win_pct, total, game_pk))

    # === Trend 2: Over hits when both teams hit 2+ HR in previous game ===
    hr_history = {}  # team -> HRs from previous game
    over_games = []
    for row in rows:
        game_pk, game_date, home_team, away_team, home_score, away_score = row
        if home_score is None or away_score is None:
            continue

        # Check if both had 2+ HR last game
        if hr_history.get(home_team, 0) >= 2 and hr_history.get(away_team, 0) >= 2:
            over = (home_score + away_score) > 8  # rough line proxy
            over_games.append((game_pk, over))

        # Simulate HRs (random logic for now)
        hr_history[home_team] = 2 if home_score >= 5 else 0
        hr_history[away_team] = 2 if away_score >= 5 else 0

    if over_games:
        overs = sum(1 for _, is_over in over_games if is_over)
        total = len(over_games)
        win_pct = 100.0 * overs / total
        c.execute("""
        INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
        VALUES (?, ?, ?, ?, NULL)
        """, ("Over hits when both teams hit 2+ HR in last game", "Over/Under", win_pct, total))
        for game_pk, is_over in over_games:
            c.execute("""
            INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
            VALUES (?, ?, ?, ?, ?)
            """, ("Over hits when both teams hit 2+ HR in last game", "Over/Under", win_pct, total, game_pk))

    # === Trend 3: Favorites after a shutout loss ===
    shutout_favorites = []
    team_last_score = {}
    for row in rows:
        game_pk, game_date, home_team, away_team, home_score, away_score = row
        if home_score is None or away_score is None:
            continue

        # Determine if favorite (guess by higher score in previous game)
        home_fav = team_last_score.get(home_team, 0) >= team_last_score.get(away_team, 0)

        # If team was shutout last game and now favored
        if home_score > away_score and team_last_score.get(home_team) == 0 and home_fav:
            win = home_score > away_score
            shutout_favorites.append((game_pk, win))

        team_last_score[home_team] = home_score
        team_last_score[away_team] = away_score

    if shutout_favorites:
        wins = sum(1 for _, win in shutout_favorites if win)
        total = len(shutout_favorites)
        win_pct = 100.0 * wins / total
        c.execute("""
        INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
        VALUES (?, ?, ?, ?, NULL)
        """, ("Favorites after a shutout loss", "ML", win_pct, total))
        for game_pk, win in shutout_favorites:
            c.execute("""
            INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
            VALUES (?, ?, ?, ?, ?)
            """, ("Favorites after a shutout loss", "ML", win_pct, total, game_pk))
    
    # Compute trend stats
    if matching_games:
        wins = sum(1 for _, win in matching_games if win)
        total = len(matching_games)
        win_pct = 100.0 * wins / total

        # Add to trend table (global)
        c.execute("""
        INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
        VALUES (?, ?, ?, ?, NULL)
        """, ("Home teams after 2 losses on Sunday", "ML", win_pct, total))

        # Optionally link to each game
        for game_pk, win in matching_games:
            c.execute("""
            INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
            VALUES (?, ?, ?, ?, ?)
            """, ("Home teams after 2 losses on Sunday", "ML", win_pct, total, game_pk))

    conn.commit()
    conn.close()

def get_todays_games():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.date.today().isoformat()
    c.execute("SELECT game_pk, game_date, home_team, away_team FROM games WHERE game_date = ?", (today,))
    rows = c.fetchall()
    conn.close()
    return [{"game_pk": r[0], "game_date": r[1], "home_team": r[2], "away_team": r[3]} for r in rows]

def get_top_trends_for_today():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today_games = get_todays_games()
    trends_map = {}
    for game in today_games:
        c.execute("SELECT description, bet_type, win_pct, sample_size FROM trends WHERE game_pk = ?", (game["game_pk"],))
        trend_rows = c.fetchall()
        trends_map[game["game_pk"]] = [{"description": tr[0], "bet_type": tr[1], "win_pct": tr[2], "sample_size": tr[3]} for tr in trend_rows]
    conn.close()
    return trends_map

def get_all_trends():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT description, bet_type, win_pct, sample_size FROM trends WHERE game_pk IS NULL")
    rows = c.fetchall()
    conn.close()
    return [{"description": r[0], "bet_type": r[1], "win_pct": r[2], "sample_size": r[3]} for r in rows]
