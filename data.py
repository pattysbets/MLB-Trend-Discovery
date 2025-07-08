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
    url = f"{MLB_API_BASE}/schedule?sportId=1&startDate={start_date}&endDate={today}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    for day in data.get("dates", []):
        for g in day.get("games", []):
            game_pk = g["gamePk"]
            game_date = day["date"]
            home_team = g["teams"]["home"]["team"]["name"]
            away_team = g["teams"]["away"]["team"]["name"]
            home_score = g["teams"]["home"].get("score")
            away_score = g["teams"]["away"].get("score")
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
    c.execute("""
    INSERT INTO trends (description, bet_type, win_pct, sample_size, game_pk)
    VALUES (?, ?, ?, ?, ?)
    """, ("Home teams after 2 losses on Sunday", "ML", 64.5, 42, None))
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
        c.execute("SELECT description, bet_type, win_pct, sample_size FROM trends WHERE game_pk IS NULL OR game_pk = ?", (game["game_pk"],))
        trend_rows = c.fetchall()
        trends_map[game["game_pk"]] = [{"description": tr[0], "bet_type": tr[1], "win_pct": tr[2], "sample_size": tr[3]} for tr in trend_rows]
    conn.close()
    return trends_map

def get_all_trends():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT description, bet_type, win_pct, sample_size FROM trends")
    rows = c.fetchall()
    conn.close()
    return [{"description": r[0], "bet_type": r[1], "win_pct": r[2], "sample_size": r[3]} for r in rows]
