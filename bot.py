import os
import time
import requests
import asyncio
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Bot
from telegram.request import HTTPXRequest
import re
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID = "-1001981134607"

# APIs
LIVE_API_URL = "https://app3.caveiratips.com.br/api/live-events/"
RECENT_MATCHES_URL = "https://api.caveiratips.com/api/v1/historico/partidas"
PLAYER_STATS_URL = "https://app3.caveiratips.com.br/app3/api/confronto/"
H2H_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/confronto/{player1}/{player2}?page=1&limit=20"

AUTH_HEADER = "Bearer 444c7677f71663b246a40600ff53a8880240086750fda243735e849cdeba9702"

MANAUS_TZ = timezone(timedelta(hours=-4))

# =============================================================================
# CACHE E ESTADO GLOBAL
# =============================================================================
player_stats_cache = {}  # {player_name: {stats, timestamp}}
CACHE_TTL = 300  # 5 minutos

sent_tips = []
sent_match_ids = set()
last_summary = None
last_league_summary = None
last_league_message_id = None
league_stats = {}

# =============================================================================
# FUNÇÕES DE REQUISIÇÃO
# =============================================================================

def fetch_live_matches():
    """Busca partidas ao vivo da nova API"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(LIVE_API_URL, timeout=15)
            response.raise_for_status()
            data = response.json()
            events = data.get('events', [])
            print(f"[INFO] {len(events)} partidas ao vivo encontradas")
            return events
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout ao buscar partidas ao vivo (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"[ERROR] fetch_live_matches: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return []

def fetch_recent_matches(page=1, page_size=100):
    """Busca partidas recentes finalizadas - Nova API"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            params = {'page': page, 'limit': page_size}
            headers = {'Authorization': AUTH_HEADER}
            
            response = requests.get(RECENT_MATCHES_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            raw_matches = data.get('partidas', [])
            normalized_matches = []
            
            for match in raw_matches:
                normalized_matches.append({
                    'id': match.get('id'),
                    'league_name': match.get('league_name'),
                    'home_player': match.get('home_player'),
                    'away_player': match.get('away_player'),
                    'home_team': match.get('home_team'),
                    'away_team': match.get('away_team'),
                    'data_realizacao': match.get('data_realizacao'),
                    'home_score_ht': match.get('halftime_score_home'),
                    'away_score_ht': match.get('halftime_score_away'),
                    'home_score_ft': match.get('score_home'),
                    'away_score_ft': match.get('score_away')
                })
            
            print(f"[INFO] {len(normalized_matches)} partidas recentes carregadas")
            return normalized_matches
            
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout ao buscar partidas recentes (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"[ERROR] fetch_recent_matches: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return []

def fetch_player_individual_stats(player_name, use_cache=True):
    """Busca estatísticas individuais de um jogador (últimos jogos) - Nova API"""
    
    if use_cache and player_name in player_stats_cache:
        cached = player_stats_cache[player_name]
        if time.time() - cached['timestamp'] < CACHE_TTL:
            print(f"[CACHE] Stats de {player_name} do cache")
            return cached['stats']
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            url = "https://rwtips-r943.onrender.com/api/v1/historico/partidas-assincrono"
            params = {'jogador': player_name, 'limit': 20, 'page': 1}
                        
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data_raw = response.json()
            
            normalized_matches = []
            for match in data_raw.get('partidas', []):
                normalized_match = {
                    'id': match.get('id'),
                    'league_name': match.get('league_name'),
                    'home_player': match.get('home_player'),
                    'away_player': match.get('away_player'),
                    'home_team': match.get('home_team'),
                    'away_team': match.get('away_team'),
                    'data_realizacao': match.get('data_realizacao'),
                    'home_score_ht': match.get('halftime_score_home'),
                    'away_score_ht': match.get('halftime_score_away'),
                    'home_score_ft': match.get('score_home'),
                    'away_score_ft': match.get('score_away')
                }
                normalized_matches.append(normalized_match)
                
            final_data = {
                'matches': normalized_matches,
                'total_count': data_raw.get('paginacao', {}).get('total_partidas', 0)
            }
            
            player_stats_cache[player_name] = {
                'stats': final_data,
                'timestamp': time.time()
            }
            
            print(f"[INFO] Stats de {player_name} carregadas ({final_data['total_count']} jogos)")
            return final_data
            
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout ao buscar stats de {player_name} (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"[ERROR] fetch_player_individual_stats {player_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return None

def fetch_h2h_data(player1, player2):
    """Busca dados H2H entre dois jogadores"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            url = H2H_API_URL.format(player1=player1, player2=player2)
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            print(f"[INFO] H2H {player1} vs {player2}: {data.get('total_matches', 0)} jogos")
            return data
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout ao buscar H2H (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"[ERROR] fetch_h2h_data {player1} vs {player2}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return None

# =============================================================================
# ANÁLISE DE ESTATÍSTICAS
# =============================================================================

def analyze_last_5_games(matches, player_name):
    """Analisa os últimos 5 jogos de um jogador"""
    if not matches:
        print(f"[WARN] {player_name}: Nenhum jogo encontrado")
        return None
    
    if len(matches) < 5:
        print(f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: 5)")
        return None
    
    last_5 = matches[:5]
    print(f"[DEBUG] Analisando últimos 5 jogos de {player_name}")
    
    # Contadores
    ht_over_05 = ht_over_15 = ht_over_25 = ht_over_35 = 0
    ht_scored_05 = ht_scored_15 = ht_scored_25 = 0
    ht_conceded_15 = 0
    
    ft_over_05 = ft_over_15 = ft_over_25 = ft_over_35 = ft_over_45 = 0
    ft_scored_05 = ft_scored_15 = ft_scored_25 = ft_scored_35 = 0
    
    total_goals_scored = total_goals_conceded = 0
    total_goals_scored_ht = total_goals_conceded_ht = 0
    games_scored_3_plus = btts_count = ht_btts_count = 0
    
    for match in last_5:
        is_home = match.get('home_player', '').upper() == player_name.upper()
        
        ht_home = match.get('home_score_ht', 0) or 0
        ht_away = match.get('away_score_ht', 0) or 0
        ht_total = ht_home + ht_away
        
        ft_home = match.get('home_score_ft', 0) or 0
        ft_away = match.get('away_score_ft', 0) or 0
        ft_total = ft_home + ft_away
        
        player_ht_goals = ht_home if is_home else ht_away
        player_ht_conceded = ht_away if is_home else ht_home
        
        player_ft_goals = ft_home if is_home else ft_away
        player_ft_conceded = ft_away if is_home else ft_home
        
        total_goals_scored += player_ft_goals
        total_goals_conceded += player_ft_conceded
        total_goals_scored_ht += player_ht_goals
        total_goals_conceded_ht += player_ht_conceded
        
        if player_ft_goals >= 3: games_scored_3_plus += 1
        if ft_home > 0 and ft_away > 0: btts_count += 1
        if ht_home > 0 and ht_away > 0: ht_btts_count += 1
        
        # HT Overs
        if ht_total > 0: ht_over_05 += 1
        if ht_total > 1: ht_over_15 += 1
        if ht_total > 2: ht_over_25 += 1
        if ht_total > 3: ht_over_35 += 1
        
        # HT Individual
        if player_ht_goals > 0: ht_scored_05 += 1
        if player_ht_goals > 1: ht_scored_15 += 1
        if player_ht_goals > 2: ht_scored_25 += 1
        if player_ht_conceded > 1: ht_conceded_15 += 1
        
        # FT Overs
        if ft_total > 0: ft_over_05 += 1
        if ft_total > 1: ft_over_15 += 1
        if ft_total > 2: ft_over_25 += 1
        if ft_total > 3: ft_over_35 += 1
        if ft_total > 4: ft_over_45 += 1
        
        # FT Individual
        if player_ft_goals > 0: ft_scored_05 += 1
        if player_ft_goals > 1: ft_scored_15 += 1
        if player_ft_goals > 2: ft_scored_25 += 1
        if player_ft_goals > 3: ft_scored_35 += 1
    
    return {
        'ht_over_05_pct': (ht_over_05 / 5) * 100,
        'ht_over_15_pct': (ht_over_15 / 5) * 100,
        'ht_over_25_pct': (ht_over_25 / 5) * 100,
        'ht_over_35_pct': (ht_over_35 / 5) * 100,
        'ht_scored_05_pct': (ht_scored_05 / 5) * 100,
        'ht_scored_15_pct': (ht_scored_15 / 5) * 100,
        'ht_scored_25_pct': (ht_scored_25 / 5) * 100,
        'ht_conceded_15_pct': (ht_conceded_15 / 5) * 100,
        'ft_over_05_pct': (ft_over_05 / 5) * 100,
        'ft_over_15_pct': (ft_over_15 / 5) * 100,
        'ft_over_25_pct': (ft_over_25 / 5) * 100,
        'ft_over_35_pct': (ft_over_35 / 5) * 100,
        'ft_over_45_pct': (ft_over_45 / 5) * 100,
        'ft_scored_05_pct': (ft_scored_05 / 5) * 100,
        'ft_scored_15_pct': (ft_scored_15 / 5) * 100,
        'ft_scored_25_pct': (ft_scored_25 / 5) * 100,
        'ft_scored_35_pct': (ft_scored_35 / 5) * 100,
        'avg_goals_scored_ft': total_goals_scored / 5,
        'avg_goals_conceded_ft': total_goals_conceded / 5,
        'avg_goals_scored_ht': total_goals_scored_ht / 5,
        'avg_goals_conceded_ht': total_goals_conceded_ht / 5,
        'consistency_ft_3_plus_pct': (games_scored_3_plus / 5) * 100,
        'btts_pct': (btts_count / 5) * 100,
        'ht_btts_pct': (ht_btts_count / 5) * 100
    }

# =============================================================================
# LÓGICA DE ESTRATÉGIAS
# =============================================================================

def check_strategies_8mins(event, home_stats, away_stats, all_league_stats):
    """Estratégias para ligas de 8 minutos"""
    strategies = []
    
    league_name = event.get('leagueName', '')
    # Mapeamento reverso para encontrar a chave correta em league_stats
    league_key = None
    if 'H2H GG League - 8 mins' in league_name: league_key = 'H2H 8 MIN'
    elif 'Battle - 8 mins' in league_name: league_key = 'BATTLE 8 MIN'
    
    # Se não tiver dados da liga, não entra nas estratégias
    if not league_key or league_key not in all_league_stats:
        return strategies

    l_stats = all_league_stats[league_key]
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # HT (60s - 180s)
    if 60 <= time_seconds <= 180:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 80 and
                away_stats['ht_over_15_pct'] >= 80):
                strategies.append("⚽ +1.5 GOLS HT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ht']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 75 and
                    home_stats['ht_over_15_pct'] == 100 and
                    away_stats['ht_over_15_pct'] == 100):
                    strategies.append("⚽ +2.5 GOLS HT")
                
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ht_over_05_pct'] == 100 and
                away_stats['ht_over_05_pct'] == 100):
                strategies.append("⚽ BTTS HT")

    # FT (180s - 360s)
    if 180 <= time_seconds <= 360:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 75):
                strategies.append("⚽ +1.5 GOLS FT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o25'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                avg_btts >= 80):
                strategies.append("⚽ +2.5 GOLS FT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ft']['o25'] >= 90): 
                if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                    away_stats['avg_goals_scored_ft'] >= 2.5 and
                    avg_btts >= 80):
                    strategies.append("⚽ +3.5 GOLS FT")
                
    # Estratégias de jogador (90s - 360s)
    if 90 <= time_seconds <= 360:
        # Player 1.5 FT check
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
             if (l_stats['ft']['o15'] >= 95):
                if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.5 and
                    avg_btts <= 70 and
                    home_stats['ft_scored_15_pct'] >= 80 and
                    home_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
             if (l_stats['ft']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 3.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.0 and
                    avg_btts <= 60 and
                    home_stats['ft_scored_25_pct'] >= 80 and
                    home_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
             if (l_stats['ft']['o15'] >= 95):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 2.5 and
                    avg_btts <= 70 and
                    away_stats['ft_scored_15_pct'] >= 80 and
                    away_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (l_stats['ft']['o25'] >= 90):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 3.4 and
                    avg_btts <= 60 and
                    away_stats['ft_scored_25_pct'] >= 80 and
                    away_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

def check_strategies_12mins(event, home_stats, away_stats, all_league_stats):
    """Estratégias para liga de 12 minutos"""
    strategies = []
    
    league_name = event.get('leagueName', '')
    # Mapeamento reverso para encontrar a chave correta em league_stats
    league_key = None
    if 'GT Leagues - 12 mins' in league_name or 'GT Leagues – 12 mins' in league_name: 
        league_key = 'GT LEAGUE 12 MIN'

    if not league_key or league_key not in all_league_stats:
        return strategies

    l_stats = all_league_stats[league_key]
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # HT (90s - 300s)
    if 90 <= time_seconds <= 300:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 90 and
                away_stats['ht_over_15_pct'] >= 90):
                strategies.append("⚽ +1.5 GOLS HT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ht']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 75 and
                    home_stats['ht_over_15_pct'] == 100 and
                    away_stats['ht_over_15_pct'] == 100):
                    strategies.append("⚽ +2.5 GOLS HT")
                
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ht_over_05_pct'] == 100 and
                away_stats['ht_over_05_pct'] == 100):
                strategies.append("⚽ BTTS HT")

    # FT (260s - 510s)
    if 260 <= time_seconds <= 510:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 75):
                strategies.append("⚽ +1.5 GOLS FT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o25'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                avg_btts >= 80):
                strategies.append("⚽ +2.5 GOLS FT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ft']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                    away_stats['avg_goals_scored_ft'] >= 2.5 and
                    avg_btts >= 80):
                    strategies.append("⚽ +3.5 GOLS FT")
                
    # Estratégias de jogador (90s - 510s)
    if 90 <= time_seconds <= 510:
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
             if (l_stats['ft']['o15'] >= 95):
                if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.5 and
                    avg_btts <= 70 and
                    home_stats['ft_scored_15_pct'] >= 80 and
                    home_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
            if (l_stats['ft']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 3.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.0 and
                    avg_btts <= 60 and
                    home_stats['ft_scored_25_pct'] >= 80 and
                    home_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
             if (l_stats['ft']['o15'] >= 95):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 2.5 and
                    avg_btts <= 70 and
                    away_stats['ft_scored_15_pct'] >= 80 and
                    away_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (l_stats['ft']['o25'] >= 90):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 3.4 and
                    avg_btts <= 60 and
                    away_stats['ft_scored_25_pct'] >= 80 and
                    away_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

def check_strategies_volta_6mins(event, home_stats, away_stats, all_league_stats):
    """Estratégias para liga Volta de 6 minutos"""
    strategies = []
    
    league_name = event.get('leagueName', '')
    # Mapeamento reverso para encontrar a chave correta em league_stats
    league_key = None
    if 'Volta - 6 mins' in league_name: 
        league_key = 'VOLTA 6 MIN'

    if not league_key or league_key not in all_league_stats:
        return strategies

    l_stats = all_league_stats[league_key]
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # HT (30s - 120s)
    if 30 <= time_seconds <= 120:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 90 and
                away_stats['ht_over_15_pct'] >= 90):
                strategies.append("⚽ +1.5 GOLS HT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ht']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 75 and
                    home_stats['ht_over_15_pct'] == 100 and
                    away_stats['ht_over_15_pct'] == 100):
                    strategies.append("⚽ +2.5 GOLS HT")
                
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ht_over_05_pct'] == 100 and
                away_stats['ht_over_05_pct'] == 100):
                strategies.append("⚽ BTTS HT")

    # FT (150s - 265s)
    if 150 <= time_seconds <= 265:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 75):
                strategies.append("⚽ +1.5 GOLS FT")
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o25'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                avg_btts >= 80):
                strategies.append("⚽ +2.5 GOLS FT")
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ft']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                    away_stats['avg_goals_scored_ft'] >= 2.5 and
                    avg_btts >= 80):
                    strategies.append("⚽ +3.5 GOLS FT")
                
    # Estratégias de jogador (30s - 265s)
    if 30 <= time_seconds <= 265:
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
             if (l_stats['ft']['o15'] >= 95):
                if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.5 and
                    avg_btts <= 70 and
                    home_stats['ft_scored_15_pct'] >= 80 and
                    home_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
             if (l_stats['ft']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 3.0 and
                    away_stats['avg_goals_scored_ft'] <= 1.0 and
                    avg_btts <= 60 and
                    home_stats['ft_scored_25_pct'] >= 80 and
                    home_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
             if (l_stats['ft']['o15'] >= 95):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 2.5 and
                    avg_btts <= 70 and
                    away_stats['ft_scored_15_pct'] >= 80 and
                    away_stats['ft_scored_25_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (l_stats['ft']['o25'] >= 90):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 3.4 and
                    avg_btts <= 60 and
                    away_stats['ft_scored_25_pct'] >= 80 and
                    away_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

# =============================================================================
# FORMATAÇÃO DE MENSAGENS
# =============================================================================

def format_tip_message(event, strategy, home_stats_summary, away_stats_summary):
    """Formata mensagem da dica"""
    league = event.get('leagueName', 'Desconhecida')
    
    league_mapping = {
        'Esoccer GT Leagues – 12 mins play': 'GT LEAGUE 12 MIN',
        'Esoccer GT Leagues - 12 mins play': 'GT LEAGUE 12 MIN',
        'Esoccer Battle Volta - 6 mins play': 'VOLTA 6 MIN',
        'Esoccer H2H GG League - 8 mins play': 'H2H 8 MIN',
        'Esoccer Battle - 8 mins play': 'BATTLE 8 MIN'
    }
    
    clean_league = league
    for key, value in league_mapping.items():
        if key in league:
            clean_league = value
            break
            
    home_player = event.get('homePlayer', '?')
    away_player = event.get('awayPlayer', '?')
    bet365_event_id = event.get('bet365EventId', '')
    
    timer = event.get('timer', {})
    time_str = timer.get('formatted', '00:00')
    
    scoreboard = event.get('scoreboard', '0-0')
    
    # Cabeçalho com destaque
    msg = "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🎯 <b>OPORTUNIDADE DETECTADA</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Liga e Estratégia
    msg += f"🏆 <b>{clean_league}</b>\n"
    msg += f"💎 <b>{strategy}</b>\n\n"
    
    # Informações do jogo
    msg += f"⏱ <b>Tempo:</b> {time_str} | 📊 <b>Placar:</b> {scoreboard}\n"
    msg += f"🎮 <b>{home_player}</b> vs <b>{away_player}</b>\n\n"
    
    # Estatísticas formatadas
    if home_stats_summary and away_stats_summary:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📈 <b>ANÁLISE - ÚLTIMOS 5 JOGOS</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        avg_btts = (home_stats_summary['btts_pct'] + away_stats_summary['btts_pct']) / 2
        
        msg += f"🏠 <b>{home_player}</b>\n"
        msg += f"├ HT: +0.5 ({home_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({home_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {home_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo\n"
        msg += f"└ Gols +3: {home_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"
        
        msg += f"✈️ <b>{away_player}</b>\n"
        msg += f"├ HT: +0.5 ({away_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({away_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {away_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo\n"
        msg += f"└ Gols +3: {away_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"
        
        msg += f"🔥 <b>BTTS Médio:</b> {avg_btts:.0f}%\n\n"
    
    # Link Bet365
    if bet365_event_id:
        bet365_link = f"https://www.bet365.bet.br/?#/IP/EV{bet365_event_id}"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🎲 <a href='{bet365_link}'><b>APOSTAR NA BET365</b></a>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
    
    return msg

def get_trend_emoji(perc, inverse=False):
    """Retorna emoji baseado na porcentagem"""
    adjusted = 100 - perc if inverse else perc
    
    if adjusted >= 95: return "🟢"
    if adjusted >= 80: return "🟡"
    if adjusted >= 60: return "🟠"
    return "🔴"

# =============================================================================
# ENVIO DE MENSAGENS
# =============================================================================

async def send_tip(bot, event, strategy, home_stats, away_stats):
    """Envia uma dica para o Telegram"""
    event_id = event.get('id')
    
    if event_id in sent_match_ids:
        return
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            msg = format_tip_message(event, strategy, home_stats, away_stats)
            message_obj = await bot.send_message(
                chat_id=CHAT_ID,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            
            sent_match_ids.add(event_id)
            
            sent_tips.append({
                'event_id': event_id,
                'strategy': strategy,
                'sent_time': datetime.now(MANAUS_TZ),
                'status': 'pending',
                'message_id': message_obj.message_id,
                'message_text': msg,
                'home_player': event.get('homePlayer'),
                'away_player': event.get('awayPlayer')
            })
            
            print(f"[✓] Dica enviada: {event_id} - {strategy}")
            break
            
        except Exception as e:
            print(f"[ERROR] send_tip (tentativa {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                print(f"[ERROR] Falha ao enviar dica após {max_retries} tentativas")

# =============================================================================
# VERIFICAÇÃO DE RESULTADOS
# =============================================================================

async def check_results(bot):
    """Verifica resultados das tips e atualiza mensagens"""
    global last_summary, last_league_summary, last_league_message_id
    
    try:
        recent = fetch_recent_matches(page=1, page_size=50)
        
        finished_matches = {}
        for match in recent:
            home = match.get('home_player', '').upper()
            away = match.get('away_player', '').upper()
            key = f"{home}_{away}"
            finished_matches[key] = match
        
        today = datetime.now(MANAUS_TZ).date()
        greens = reds = refunds = 0
        
        for tip in sent_tips:
            if tip['sent_time'].date() != today:
                continue
            
            if tip['status'] == 'pending':
                home = (tip.get('home_player') or '').upper()
                away = (tip.get('away_player') or '').upper()
                key = f"{home}_{away}"
                
                match = finished_matches.get(key)
                if match:
                    ht_home = match.get('home_score_ht', 0) or 0
                    ht_away = match.get('away_score_ht', 0) or 0
                    ht_total = ht_home + ht_away
                    
                    ft_home = match.get('home_score_ft', 0) or 0
                    ft_away = match.get('away_score_ft', 0) or 0
                    ft_total = ft_home + ft_away
                    
                    strategy = tip['strategy']
                    
                    result = None
                    if '+0.5 GOL HT' in strategy:
                        result = 'green' if ht_total >= 1 else 'red'
                    elif '+1.5 GOLS HT' in strategy:
                        result = 'green' if ht_total >= 2 else 'red'
                    elif '+2.5 GOLS HT' in strategy:
                        result = 'green' if ht_total >= 3 else 'red'
                    elif 'BTTS HT' in strategy:
                        result = 'green' if (ht_home > 0 and ht_away > 0) else 'red'
                    elif '+1.5 GOLS FT' in strategy:
                        result = 'green' if ft_total >= 2 else 'red'
                    elif '+2.5 GOLS FT' in strategy:
                        if "⚽ Player" in strategy or "⚽ " in strategy and "GOLS FT" in strategy and not strategy.startswith("⚽ +2.5 GOLS FT"):
                             try:
                                player_name = strategy.replace("⚽ ", "").replace(" +2.5 GOLS FT", "").strip().upper()
                                if player_name == home:
                                    result = 'green' if ft_home >= 3 else 'red'
                                elif player_name == away:
                                    result = 'green' if ft_away >= 3 else 'red'
                             except:
                                 pass
                        else:
                            result = 'green' if ft_total >= 3 else 'red'
                            
                    elif '+3.5 GOLS FT' in strategy:
                        result = 'green' if ft_total >= 4 else 'red'
                    elif '+4.5 GOLS FT' in strategy:
                        result = 'green' if ft_total >= 5 else 'red'
                    
                    elif '+1.5 GOLS FT' in strategy and ("⚽ Player" in strategy or "⚽ " in strategy and not strategy.startswith("⚽ +1.5 GOLS FT")):
                         try:
                            player_name = strategy.replace("⚽ ", "").replace(" +1.5 GOLS FT", "").strip().upper()
                            if player_name == home:
                                result = 'green' if ft_home >= 2 else 'red'
                            elif player_name == away:
                                result = 'green' if ft_away >= 2 else 'red'
                         except:
                             pass
                    
                    if result:
                        tip['status'] = result
                        
                        emoji = "✅✅✅✅✅" if result == 'green' else "❌❌❌❌❌"
                        new_text = tip['message_text'] + f"\n{emoji}"
                        
                        try:
                            await bot.edit_message_text(
                                chat_id=CHAT_ID,
                                message_id=tip['message_id'],
                                text=new_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True
                            )
                            print(f"[✓] Resultado atualizado: {tip['event_id']} - {result}")
                        except Exception as e:
                            print(f"[ERROR] edit_message: {e}")
            
            if tip['status'] == 'green': greens += 1
            if tip['status'] == 'red': reds += 1
            if tip['status'] == 'refund': refunds += 1
        
        total_resolved = greens + reds
        if total_resolved > 0:
            perc = (greens / total_resolved * 100.0)
            summary = (
                f"\n\n<b>👑 RW TIPS - FIFA 🎮</b>\n\n"
                f"<b>✅ Green [{greens}]</b>\n"
                f"<b>❌ Red [{reds}]</b>\n"
                f"<b>♻️ Push [{refunds}]</b>\n"
                f"📊 <i>Taxa de acerto: {perc:.1f}%</i>\n\n"
            )
            
            if summary != last_summary:
                await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
                last_summary = summary
                print("[✓] Resumo do dia enviado")
        
        await update_league_stats(bot, recent)
        
    except Exception as e:
        print(f"[ERROR] check_results: {e}")

async def update_league_stats(bot, recent_matches):
    """Atualiza e envia resumo das estatísticas das ligas"""
    global last_league_summary, last_league_message_id, league_stats
    
    try:
        # Ordenar partidas para garantir estabilidade nos cálculos
        recent_matches.sort(key=lambda x: (x.get('data_realizacao', ''), x.get('id', 0)), reverse=True)

        league_games = defaultdict(list)
        
        league_mapping = {
            'Esoccer GT Leagues – 12 mins play': 'GT LEAGUE 12 MIN',
            'Esoccer GT Leagues - 12 mins play': 'GT LEAGUE 12 MIN',
            'Esoccer Battle Volta - 6 mins play': 'VOLTA 6 MIN',
            'Esoccer H2H GG League - 8 mins play': 'H2H 8 MIN',
            'Esoccer Battle - 8 mins play': 'BATTLE 8 MIN'
        }
        
        for match in recent_matches[:200]:
            league_raw = None
            if 'league_name' in match: league_raw = match['league_name']
            elif 'tournamentName' in match: league_raw = match['tournamentName']
            elif 'leagueName' in match: league_raw = match['leagueName']
            elif 'competition' in match and isinstance(match['competition'], dict): 
                league_raw = match['competition'].get('name')
            
            if not league_raw or league_raw == 'Unknown': continue
            
            league = league_raw
            for key, value in league_mapping.items():
                if key in league_raw:
                    league = value
                    break
            
            ht_home = match.get('home_score_ht', 0) or 0
            ht_away = match.get('away_score_ht', 0) or 0
            ft_home = match.get('home_score_ft', 0) or 0
            ft_away = match.get('away_score_ft', 0) or 0
            
            league_games[league].append({
                'ht_goals': ht_home + ht_away,
                'ft_goals': ft_home + ft_away,
                'ht_btts': 1 if ht_home > 0 and ht_away > 0 else 0,
                'ft_btts': 1 if ft_home > 0 and ft_away > 0 else 0
            })
        
        stats = {}
        for league, games in league_games.items():
            if len(games) < 5: continue
            
            last_n = games[:5]
            total = len(last_n)
            
            def calc_pct(count):
                return int((count / total) * 100)

            stats[league] = {
                'ht': {
                    'o05': calc_pct(sum(1 for g in last_n if g['ht_goals'] > 0)),
                    'o15': calc_pct(sum(1 for g in last_n if g['ht_goals'] > 1)),
                    'o25': calc_pct(sum(1 for g in last_n if g['ht_goals'] > 2)),
                    'btts': calc_pct(sum(1 for g in last_n if g['ht_btts'])),
                    '0x0': calc_pct(sum(1 for g in last_n if g['ht_goals'] == 0))
                },
                'ft': {
                    'o05': calc_pct(sum(1 for g in last_n if g['ft_goals'] > 0)),
                    'o15': calc_pct(sum(1 for g in last_n if g['ft_goals'] > 1)),
                    'o25': calc_pct(sum(1 for g in last_n if g['ft_goals'] > 2)),
                    'btts': calc_pct(sum(1 for g in last_n if g['ft_btts'])),
                    '0x0': calc_pct(sum(1 for g in last_n if g['ft_goals'] == 0))
                },
                'count': total
            }
        
        if not stats: return
        
        # Comparação exata dos dicionários
        if league_stats and league_stats == stats:
            print(f"[INFO] Resumo de ligas idêntico ao anterior. Ignorando envio.")
            return
            
        league_stats = stats
        
        # Nova formatação: limpa, organizada e fácil de ler
        summary = "📊 <b>RESUMO DE LIGAS</b> (Últimos 5 jogos)\n"
        summary += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        def get_emoji(pct):
            """Retorna emoji baseado na porcentagem"""
            if pct >= 95: return "🟢"
            elif pct >= 75: return "🟡"
            elif pct >= 55: return "🟠"
            else: return "🔴"
        
        def format_stat(label, pct):
            """Formata uma estatística com alinhamento"""
            emoji = get_emoji(pct)
            return f"{label}: {pct:>3}% {emoji}"
        
        for league in sorted(stats.keys()):
            s = stats[league]
            h = s['ht']
            f = s['ft']
            
            # Cabeçalho da liga
            summary += f"🏆 <b>{league}</b>\n\n"
            
            # Estatísticas HT
            summary += "⏱ <b>PRIMEIRO TEMPO (HT)</b>\n"
            summary += f"  {format_stat('Over 0.5', h['o05'])}\n"
            summary += f"  {format_stat('Over 1.5', h['o15'])}\n"
            summary += f"  {format_stat('BTTS    ', h['btts'])}\n"
            summary += f"  {format_stat('0x0     ', h['0x0'])}\n\n"
            
            # Estatísticas FT
            summary += "⏰ <b>TEMPO COMPLETO (FT)</b>\n"
            summary += f"  {format_stat('Over 1.5', f['o15'])}\n"
            summary += f"  {format_stat('Over 2.5', f['o25'])}\n"
            summary += f"  {format_stat('BTTS    ', f['btts'])}\n"
            summary += f"  {format_stat('0x0     ', f['0x0'])}\n\n"
            
            summary += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Legenda no final
        summary += "<i>Legenda:</i>\n"
        summary += "🟢 Forte (95%+) | 🟡 Médio (75-94%)\n"
        summary += "🟠 Fraco (55-74%) | 🔴 Baixo (<55%)\n"
        
        if summary != last_league_summary:
            if last_league_message_id:
                try:
                    await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
                except: pass
            
            msg = await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
            last_league_summary = summary
            last_league_message_id = msg.message_id
            print("[✓] Resumo das ligas atualizado com layout melhorado")
    
    except Exception as e:
        print(f"[ERROR] update_league_stats: {e}")

# =============================================================================
# LOOP PRINCIPAL
# =============================================================================

async def main_loop(bot):
    """Loop principal de análise"""
    
    print("[INFO] Iniciando loop principal...")
    
    while True:
        try:
            print(f"\n[CICLO] {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
            
            live_events = fetch_live_matches()
            
            if not live_events:
                print("[INFO] Nenhuma partida ao vivo no momento")
                await asyncio.sleep(10)
                continue
            
            for event in live_events:
                event_id = event.get('id')
                league_name = event.get('leagueName', '')
                home_player = event.get('homePlayer', '')
                away_player = event.get('awayPlayer', '')
                bet365_event_id = event.get('bet365EventId', '')
                
                print(f"\n[EVENTO] {event_id}: {home_player} vs {away_player} ({league_name})")
                print(f"[BET365] Event ID: {bet365_event_id}")
                
                if event_id in sent_match_ids:
                    continue
                
                home_data = fetch_player_individual_stats(home_player)
                away_data = fetch_player_individual_stats(away_player)
                
                if not home_data or not away_data:
                    print(f"[WARN] Sem dados suficientes para {home_player} ou {away_player}")
                    continue
                
                home_matches = home_data.get('matches', [])
                away_matches = away_data.get('matches', [])
                
                if len(home_matches) < 5 or len(away_matches) < 5:
                    print(f"[WARN] Dados insuficientes: {home_player}={len(home_matches)} jogos, {away_player}={len(away_matches)} jogos (mínimo: 5)")
                    continue
                
                home_stats = analyze_last_5_games(home_matches, home_player)
                away_stats = analyze_last_5_games(away_matches, away_player)
                
                if not home_stats or not away_stats:
                    print(f"[WARN] Falha na análise das estatísticas")
                    continue
                
                print(f"[STATS] {home_player} (últimos 5 jogos): HT O0.5={home_stats['ht_over_05_pct']:.0f}% O1.5={home_stats['ht_over_15_pct']:.0f}% O2.5={home_stats['ht_over_25_pct']:.0f}%")
                print(f"[STATS] {away_player} (últimos 5 jogos): HT O0.5={away_stats['ht_over_05_pct']:.0f}% O1.5={away_stats['ht_over_15_pct']:.0f}% O2.5={away_stats['ht_over_25_pct']:.0f}%")
                
                strategies = []
                
                if 'H2H GG League - 8 mins' in league_name or 'Battle - 8 mins' in league_name:
                    strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats)
                
                elif 'GT Leagues - 12 mins' in league_name or 'GT Leagues – 12 mins' in league_name:
                    strategies = check_strategies_12mins(event, home_stats, away_stats, league_stats)
                
                elif 'Volta - 6 mins' in league_name:
                    strategies = check_strategies_volta_6mins(event, home_stats, away_stats, league_stats)
                
                for strategy in strategies:
                    print(f"[✓] OPORTUNIDADE ENCONTRADA: {strategy}")
                    await send_tip(bot, event, strategy, home_stats, away_stats)
                    await asyncio.sleep(1)
            
            print("[INFO] Ciclo concluído, aguardando 10 segundos...")
            await asyncio.sleep(10)
        
        except Exception as e:
            print(f"[ERROR] main_loop: {e}")
            await asyncio.sleep(10)

async def results_checker(bot):
    """Loop de verificação de resultados"""
    
    print("[INFO] Iniciando verificador de resultados...")
    
    await asyncio.sleep(30)
    
    while True:
        try:
            await check_results(bot)
            await asyncio.sleep(180)
        except Exception as e:
            print(f"[ERROR] results_checker: {e}")
            await asyncio.sleep(180)

# =============================================================================
# INICIALIZAÇÃO
# =============================================================================

async def main():
    """Função principal"""
    
    print("="*70)
    print("🤖 RW TIPS - BOT FIFA v2.0")
    print("="*70)
    print(f"Horário: {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')} (Manaus)")
    print("="*70)
    
    request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    
    bot = Bot(token=BOT_TOKEN, request=request)
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"[INFO] Tentando conectar ao Telegram (tentativa {attempt + 1}/{max_retries})...")
            me = await bot.get_me()
            print(f"[✓] Bot conectado: @{me.username}")
            break
        except Exception as e:
            print(f"[ERROR] Tentativa {attempt + 1} falhou: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"[INFO] Aguardando {wait_time} segundos antes de tentar novamente...")
                await asyncio.sleep(wait_time)
            else:
                print("[ERROR] Não foi possível conectar ao Telegram após várias tentativas")
                print("[INFO] Verifique:")
                print("  1. Sua conexão com a internet")
                print("  2. Se o token do bot está correto")
                print("  3. Se não há firewall bloqueando")
                print("  4. Tente usar uma VPN se estiver bloqueado")
                return
    
    await asyncio.gather(
        main_loop(bot),
        results_checker(bot)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Bot encerrado pelo usuário")
    except Exception as e:
        print(f"[ERRO FATAL] {e}")