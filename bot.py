import os
import time
import requests
import json
import asyncio
from datetime import datetime, timezone, timedelta
from telegram import Bot
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID = "-1001981134607"

LIVE_API_URL = "https://app3.caveiratips.com.br/api/live-events/"
PLAYER_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/partidas-assincrono"
H2H_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/confronto/{home}/{away}"
ENDED_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"

AUTH_TOKEN = "Bearer oat_MTEyNTEx.aS1EdDJaNWw2dUkzREpqOGI3Mmo1eHdVeUZOZmZyQmZkclR2bE1RODM0ODg3NzEzODQ"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 OPR/124.0.0.0",
    "Accept": "*/*",
    "Referer": "https://app3.caveiratips.com.br/jogos-ao-vivo",
    "Origin": "https://app3.caveiratips.com.br",
    "Sec-Ch-Ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Opera Air";v="124"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=1, i"
}

# Manaus timezone is UTC-4
MANAUS_TZ = timezone(timedelta(hours=-4))

# Global state
sent_tips = []
last_summary = None

# --- SESSION SETUP ---
def get_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = get_session()

# --- API FUNCTIONS ---

def fetch_live_matches():
    try:
        response = session.get(LIVE_API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Handle different potential structures based on test results
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get('events', []) or data.get('data', [])
        return []
    except Exception as e:
        print(f"[ERROR] fetch_live_matches: {e}")
        return []

def fetch_player_history(player_name):
    try:
        params = {"jogador": player_name, "limit": 5, "page": 1}
        response = session.get(PLAYER_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('partidas', [])
    except Exception as e:
        print(f"[ERROR] fetch_player_history {player_name}: {e}")
        return []

def fetch_h2h_data(player1, player2):
    try:
        url = H2H_API_URL.format(home=player1, away=player2)
        params = {"limit": 20, "page": 1}
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] fetch_h2h_data {player1} vs {player2}: {e}")
        return None

def fetch_ended_matches():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
        "Accept": "application/json",
        "Referer": "https://green365.com.br/",
        "Origin": "https://green365.com.br",
        "Authorization": AUTH_TOKEN
    }
    items = []
    params = {"page": 1, "limit": 200, "sport": "esoccer", "status": "ended"}
    
    try:
        response = session.get(ENDED_API_URL, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
    except Exception as e:
        print(f"[ERROR] fetch_ended_matches: {e}")
    return items

# --- HELPER FUNCTIONS ---

def get_match_time_in_minutes(match):
    timer = match.get('timer') or {}
    if timer:
        minute = timer.get('minute', 0)
        second = timer.get('second', 0)
        return float(minute) + float(second) / 60.0
        
    if 'time_played_seconds' in match:
        return float(match['time_played_seconds']) / 60.0
        
    return 0.0

def parse_score(match):
    score = match.get('score', {})
    if score:
        return int(score.get('home', 0)), int(score.get('away', 0))
    
    if 'score_home' in match and 'score_away' in match:
        return int(match['score_home']), int(match['score_away'])
    
    ss = match.get('ss')
    if ss:
        try:
            return map(int, ss.split('-'))
        except:
            pass
            
    return 0, 0

def calculate_player_stats(matches):
    if not matches:
        return None
    
    total = len(matches)
    over_0_5_ht = 0
    over_1_5_ht = 0
    over_2_5_ht = 0
    over_3_5_ht = 0
    
    for m in matches:
        ht_h = int(m.get('halftime_score_home', 0))
        ht_a = int(m.get('halftime_score_away', 0))
        ht_total = ht_h + ht_a
        
        if ht_total > 0.5: over_0_5_ht += 1
        if ht_total > 1.5: over_1_5_ht += 1
        if ht_total > 2.5: over_2_5_ht += 1
        if ht_total > 3.5: over_3_5_ht += 1
        
    return {
        'total': total,
        'over_0_5_ht_pct': (over_0_5_ht / total) * 100,
        'over_1_5_ht_pct': (over_1_5_ht / total) * 100,
        'over_2_5_ht_pct': (over_2_5_ht / total) * 100,
        'over_3_5_ht_pct': (over_3_5_ht / total) * 100,
        'matches': matches
    }

def calculate_detailed_player_stats(matches, player_name):
    if not matches:
        return None
        
    total = len(matches)
    conceded_1_5_ht = 0
    scored_1_5_ht = 0
    
    for m in matches:
        ht_h = int(m.get('halftime_score_home', 0))
        ht_a = int(m.get('halftime_score_away', 0))
        
        p_home = m.get('home_player', '').lower()
        p_away = m.get('away_player', '').lower()
        target = player_name.lower()
        
        player_goals = 0
        opponent_goals = 0
        
        if target in p_home:
            player_goals = ht_h
            opponent_goals = ht_a
        elif target in p_away:
            player_goals = ht_a
            opponent_goals = ht_h
        else:
            continue
            
        if player_goals > 1.5: scored_1_5_ht += 1
        if opponent_goals > 1.5: conceded_1_5_ht += 1
        
    return {
        'scored_1_5_ht_pct': (scored_1_5_ht / total) * 100 if total > 0 else 0,
        'conceded_1_5_ht_pct': (conceded_1_5_ht / total) * 100 if total > 0 else 0
    }

# --- STRATEGY LOGIC ---

async def analyze_strategies(bot, match, sent_matches):
    league_name = match.get('leagueName', '')
    
    is_8min = "8 mins" in league_name
    is_12min = "12 mins" in league_name
    
    if not (is_8min or is_12min):
        return

    match_id = match.get('id')
    home_player = match.get('homePlayer', '')
    away_player = match.get('awayPlayer', '')
    
    current_time = get_match_time_in_minutes(match)
    home_goals, away_goals = parse_score(match)
    
    valid_time = False
    if is_8min and 2.0 < current_time < 3.25:
        valid_time = True
    elif is_12min and 4.0 < current_time < 5.25:
        valid_time = True
        
    if not valid_time:
        return

    strategies_to_check = []
    
    if home_goals == 0 and away_goals == 0:
        strategies_to_check.append("OVER_0_5_HT")
        strategies_to_check.append("BTTS_HT")
    elif (home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
        strategies_to_check.append("OVER_1_5_HT")
    elif (home_goals == 2 and away_goals == 0) or (home_goals == 0 and away_goals == 2) or (home_goals == 1 and away_goals == 1):
        strategies_to_check.append("OVER_2_5_HT")
        
    if not strategies_to_check:
        return

    print(f"[DEBUG] Checking {home_player} vs {away_player} ({league_name}) Time: {current_time:.2f} Score: {home_goals}-{away_goals}")

    home_history = fetch_player_history(home_player)
    away_history = fetch_player_history(away_player)
    
    if not home_history or not away_history:
        return

    h_stats = calculate_player_stats(home_history)
    a_stats = calculate_player_stats(away_history)
    
    if not h_stats or not a_stats:
        return

    strategy_name = None
    
    if "OVER_0_5_HT" in strategies_to_check:
        if (h_stats['over_0_5_ht_pct'] == 100 and h_stats['over_1_5_ht_pct'] >= 90 and
            a_stats['over_0_5_ht_pct'] == 100 and a_stats['over_1_5_ht_pct'] >= 90):
            strategy_name = "⚽ +0.5 GOL HT"

    if "OVER_1_5_HT" in strategies_to_check:
        if (h_stats['over_1_5_ht_pct'] == 100 and h_stats['over_2_5_ht_pct'] >= 80 and
            a_stats['over_1_5_ht_pct'] == 100 and a_stats['over_2_5_ht_pct'] >= 80):
            strategy_name = "⚽ +1.5 GOLS HT"

    if "OVER_2_5_HT" in strategies_to_check:
        if (h_stats['over_2_5_ht_pct'] == 100 and h_stats['over_3_5_ht_pct'] >= 70 and
            a_stats['over_2_5_ht_pct'] == 100 and a_stats['over_3_5_ht_pct'] >= 70):
            strategy_name = "⚽ +2.5 GOLS HT"

    if "BTTS_HT" in strategies_to_check and not strategy_name:
        h_detailed = calculate_detailed_player_stats(home_history, home_player)
        a_detailed = calculate_detailed_player_stats(away_history, away_player)
        
        if (h_detailed['scored_1_5_ht_pct'] == 100 and h_detailed['conceded_1_5_ht_pct'] == 100 and
            a_detailed['scored_1_5_ht_pct'] == 100 and a_detailed['conceded_1_5_ht_pct'] == 100):
            strategy_name = "⚽ AMBAS MARCAM HT"

    if strategy_name:
        h2h_data = fetch_h2h_data(home_player, away_player)
        h2h_metrics = calculate_h2h_metrics(h2h_data)
        
        msg = format_message(match, h2h_metrics, strategy_name, home_player, away_player)
        await send_message(bot, match_id, msg, sent_matches, strategy_name)

# --- METRICS & MESSAGING ---

def calculate_h2h_metrics(h2h_data):
    if not h2h_data or 'matches' not in h2h_data:
        return None

    matches = h2h_data['matches']
    total = len(matches)
    if total == 0: return None

    over_0_5_ht = 0
    over_1_5_ht = 0
    over_2_5_ht = 0
    btts_ht = 0
    player1_wins = 0
    player2_wins = 0
    player1_total_goals = 0
    player2_total_goals = 0

    for m in matches:
        ht_home = m.get('halftime_score_home', 0) or 0
        ht_away = m.get('halftime_score_away', 0) or 0
        ht_goals = ht_home + ht_away
        
        if ht_goals > 0: over_0_5_ht += 1
        if ht_goals > 1: over_1_5_ht += 1
        if ht_goals > 2: over_2_5_ht += 1
        if ht_home > 0 and ht_away > 0: btts_ht += 1
        
        ft_home = int(m.get('score_home') or m.get('final_score_home') or 0)
        ft_away = int(m.get('score_away') or m.get('final_score_away') or 0)
        
        if ft_home > ft_away: player1_wins += 1
        elif ft_away > ft_home: player2_wins += 1
        
        player1_total_goals += ft_home
        player2_total_goals += ft_away

    return {
        'player1_win_percentage': (player1_wins / total) * 100,
        'player2_win_percentage': (player2_wins / total) * 100,
        'player1_avg_goals': player1_total_goals / total,
        'player2_avg_goals': player2_total_goals / total,
        'over_0_5_ht_percentage': (over_0_5_ht / total) * 100,
        'over_1_5_ht_percentage': (over_1_5_ht / total) * 100,
        'over_2_5_ht_percentage': (over_2_5_ht / total) * 100,
        'btts_ht_percentage': (btts_ht / total) * 100
    }

def format_message(match, h2h_metrics, strategy, player1, player2):
    league = match.get('leagueName', '')
    timer = match.get('timer') or {}
    minutes = timer.get('minute', 0)
    seconds = timer.get('second', 0)
    game_time = f"{minutes}:{int(seconds):02d}"
    
    home_goals, away_goals = parse_score(match)
    ss = f"{home_goals}-{away_goals}"
    
    msg = f"\n\n<b>🏆 {league}</b>\n\n<b>🎯 {strategy}</b>\n\n⏳ Tempo: {game_time}\n\n"
    msg += f"🎮 {player1} vs {player2}\n"
    msg += f"⚽ Placar: {ss}\n"
    
    if h2h_metrics:
        msg += (
            f"🏅 <i>{h2h_metrics.get('player1_win_percentage', 0):.0f}% vs "
            f"{h2h_metrics.get('player2_win_percentage', 0):.0f}%</i>\n\n"
            f"💠 Média gols: <i>{h2h_metrics.get('player1_avg_goals', 0):.2f}</i> vs <i>{h2h_metrics.get('player2_avg_goals', 0):.2f}</i>\n\n"
            f"<b>📊 H2H HT (últimos 20 jogos):</b>\n\n"
            f"⚽ +0.5: <i>{h2h_metrics.get('over_0_5_ht_percentage', 0):.0f}%</i> | +1.5: <i>{h2h_metrics.get('over_1_5_ht_percentage', 0):.0f}%</i> | +2.5: <i>{h2h_metrics.get('over_2_5_ht_percentage', 0):.0f}%</i>\n\n"
            f"⚽ BTTS HT: <i>{h2h_metrics.get('btts_ht_percentage', 0):.0f}%</i>\n"
        )
    else:
        msg += "📊 H2H: <i>não disponível</i>"
        
    return msg

async def send_message(bot, match_id, message, sent_matches, strategy):
    if match_id in sent_matches:
        return
    try:
        message_obj = await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML", disable_web_page_preview=True)
        sent_matches.add(match_id)
        print(f"[INFO] Enviado match_id={match_id} ({strategy})")
        sent_tips.append({
            'match_id': str(match_id),
            'strategy': strategy,
            'sent_time': datetime.now(MANAUS_TZ),
            'status': 'pending',
            'message_id': message_obj.message_id,
            'message_text': message
        })
    except Exception as e:
        print(f"[ERROR] send_message {match_id}: {e}")

async def periodic_check(bot):
    global last_summary

    while True:
        try:
            print("[INFO] Verificando status das tips...")
            ended = fetch_ended_matches()
            ended_dict = {}
            for m in ended:
                try:
                    ended_dict[str(m['eventID'])] = m
                except Exception:
                    continue
            
            today = datetime.now(MANAUS_TZ).date()
            greens = reds = refunds = 0
            
            for tip in sent_tips:
                if tip['sent_time'].date() != today:
                    continue
                
                if tip['status'] == 'pending':
                    m = ended_dict.get(str(tip['match_id']))
                    if m and m.get('status') == 'ended':
                        print(f"[DEBUG] Tip {tip['match_id']}: Partida finalizada")
                        
                        ht_home = m.get('scoreHT', {}).get('home', 0) or 0
                        ht_away = m.get('scoreHT', {}).get('away', 0) or 0
                        ht_goals = ht_home + ht_away
                        
                        strat = tip['strategy']
                        
                        if 'HT' in strat:
                            if '+0.5' in strat: line = 0.5
                            elif '+1.5' in strat: line = 1.5
                            elif '+2.5' in strat: line = 2.5
                            else: line = 0
                            
                            if 'AMBAS MARCAM' in strat or 'BTTS' in strat:
                                tip['status'] = 'green' if (ht_home > 0 and ht_away > 0) else 'red'
                            else:
                                tip['status'] = 'green' if ht_goals > line else 'red'
                        else:
                            ft_goals = (m.get('score', {}).get('home', 0) or 0) + (m.get('score', {}).get('away', 0) or 0)
                            if '+0.5' in strat: line = 0.5
                            elif '+1.5' in strat: line = 1.5
                            elif '+2.5' in strat: line = 2.5
                            else: line = 0
                            tip['status'] = 'green' if ft_goals > line else 'red'

                        print(f"[DEBUG] Tip {tip['match_id']} ⇒ {tip['status']}")
                        
                        if tip['status'] in ['green', 'red']:
                            emoji = "✅✅✅✅✅" if tip['status'] == 'green' else "❌❌❌❌❌"
                            new_text = tip['message_text'] + f"{emoji}"
                            try:
                                await bot.edit_message_text(chat_id=CHAT_ID, message_id=tip['message_id'], text=new_text,
                                                            parse_mode="HTML", disable_web_page_preview=True)
                            except Exception as edit_e:
                                print(f"[ERROR] Erro ao editar mensagem: {edit_e}")
                
                if tip['status'] == 'green': greens += 1
                if tip['status'] == 'red': reds += 1
                if tip['status'] == 'refund': refunds += 1
            
            total_resolved = greens + reds
            if total_resolved > 0:
                perc = (greens / total_resolved * 100.0)
                current_summary = (
                    f"\n\n<b>👑 ʀᴡ ᴛɪᴘs - ғɪғᴀ 🎮</b>\n\n"
                    f"<b>✅ Green [{greens}]</b>\n"
                    f"<b>❌ Red [{reds}]</b>\n"
                    f"<b>♻️ Push [{refunds}]</b>\n"
                    f"📊 <i>Desempenho: {perc:.2f}%</i>\n\n"
                )
                if current_summary != last_summary:
                    try:
                        await bot.send_message(chat_id=CHAT_ID, text=current_summary, parse_mode="HTML")
                        last_summary = current_summary
                    except Exception as e:
                        print(f"[ERROR] indicador: {e}")

        except Exception as e:
            print(f"[ERROR] periodic_check: {e}")
        await asyncio.sleep(120)

async def main():
    bot = Bot(token=BOT_TOKEN)
    sent_matches = set()
    
    asyncio.create_task(periodic_check(bot))
    
    print("[INFO] Bot iniciado. Monitorando partidas...")
    
    while True:
        try:
            matches = fetch_live_matches()
            
            if matches:
                print(f"\n[INFO] {len(matches)} partidas ao vivo:")
            else:
                print(f"[INFO] 0 partidas ao vivo.")

            for match in matches:
                try:
                    league = match.get('leagueName', 'Unknown')
                    h_name = match.get('homePlayer', 'Home')
                    a_name = match.get('awayPlayer', 'Away')
                    
                    minute = int(get_match_time_in_minutes(match))
                    hg, ag = parse_score(match)
                    
                    print(f"   • {league} | {h_name} vs {a_name} | {minute}' | {hg}-{ag}")
                except Exception:
                    pass

                await analyze_strategies(bot, match, sent_matches)
                
        except Exception as e:
            print(f"[ERROR] Main loop: {e}")
        
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())