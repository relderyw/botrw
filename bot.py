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
RECENT_MATCHES_URL = "https://app3.caveiratips.com.br/app3/api/matches/recent/"
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
    """Busca partidas recentes finalizadas"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            params = {'page': page, 'page_size': page_size}
            headers = {'Authorization': AUTH_HEADER}
            response = requests.get(RECENT_MATCHES_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            matches = data.get('results', [])
            print(f"[INFO] {len(matches)} partidas recentes carregadas")
            return matches
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
    """Busca estatísticas individuais de um jogador (últimos jogos)"""
    
    # Verificar cache
    if use_cache and player_name in player_stats_cache:
        cached = player_stats_cache[player_name]
        if time.time() - cached['timestamp'] < CACHE_TTL:
            print(f"[CACHE] Stats de {player_name} do cache")
            return cached['stats']
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            params = {'player1': player_name, 'interval': '9999'}
            headers = {'Authorization': AUTH_HEADER}
            response = requests.get(PLAYER_STATS_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # Salvar no cache
            player_stats_cache[player_name] = {
                'stats': data,
                'timestamp': time.time()
            }
            
            print(f"[INFO] Stats de {player_name} carregadas ({data.get('total_count', 0)} jogos)")
            return data
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
    """
    Analisa os últimos 5 jogos de um jogador
    Retorna métricas para HT e FT
    """
    if not matches:
        print(f"[WARN] {player_name}: Nenhum jogo encontrado")
        return None
    
    if len(matches) < 5:
        print(f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: 5)")
        return None
    
    last_5 = matches[:5]
    print(f"[DEBUG] Analisando últimos 5 jogos de {player_name}")
    
    # Contadores HT
    ht_over_05 = 0
    ht_over_15 = 0
    ht_over_25 = 0
    ht_over_35 = 0
    ht_scored_05 = 0  # Marcou +0.5 no HT
    ht_scored_15 = 0  # Marcou +1.5 no HT
    ht_scored_25 = 0  # Marcou +2.5 no HT
    ht_conceded_15 = 0  # Sofreu +1.5 no HT
    
    # Contadores FT
    ft_over_05 = 0
    ft_over_15 = 0
    ft_over_25 = 0
    ft_over_35 = 0
    ft_over_45 = 0
    
    # Contadores Individuais FT
    ft_scored_05 = 0 # Marcou +0.5 FT
    ft_scored_15 = 0 # Marcou +1.5 FT
    ft_scored_25 = 0 # Marcou +2.5 FT
    ft_scored_35 = 0 # Marcou +3.5 FT
    
    # Métricas Individuais FT
    total_goals_scored = 0
    total_goals_conceded = 0
    total_goals_scored_ht = 0
    total_goals_conceded_ht = 0
    games_scored_3_plus = 0  # Jogos com 3 ou mais gols marcados pelo jogador
    btts_count = 0  # Jogos com BTTS
    
    for match in last_5:
        # Identificar se é home ou away
        is_home = match.get('home_player', '').upper() == player_name.upper()
        
        # Gols HT
        ht_home = match.get('home_score_ht', 0) or 0
        ht_away = match.get('away_score_ht', 0) or 0
        ht_total = ht_home + ht_away
        
        # Gols FT
        ft_home = match.get('home_score_ft', 0) or 0
        ft_away = match.get('away_score_ft', 0) or 0
        ft_total = ft_home + ft_away
        
        # Gols do jogador
        player_ht_goals = ht_home if is_home else ht_away
        player_ht_conceded = ht_away if is_home else ht_home
        
        player_ft_goals = ft_home if is_home else ft_away
        player_ft_conceded = ft_away if is_home else ft_home
        
        total_goals_scored += player_ft_goals
        total_goals_conceded += player_ft_conceded
        
        total_goals_scored_ht += player_ht_goals
        total_goals_conceded_ht += player_ht_conceded
        
        if player_ft_goals >= 3:
            games_scored_3_plus += 1
            
        # BTTS (Ambos Marcam) - FT
        if ft_home > 0 and ft_away > 0:
            btts_count += 1
        
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
        'btts_pct': (btts_count / 5) * 100
    }

# =============================================================================
# LÓGICA DE ESTRATÉGIAS
# =============================================================================

def check_strategies_8mins(event, home_stats, away_stats):
    """
    Verifica estratégias para ligas de 8 minutos
    (E-soccer H2H GG League - 8 mins, E-soccer Battle - 8 mins)
    """
    strategies = []
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    # Calcular média BTTS
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # =========================================================================
    # ESTRATÉGIAS HT (00:01:00 - 00:03:00) -> 60s a 180s
    # =========================================================================
    if 60 <= time_seconds <= 180:
        
        # +0.5 GOL HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts <= 60 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ +0.5 GOL HT")
            
        # +1.5 GOLS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts <= 60 and
            home_stats['ht_scored_15_pct'] >= 90 and
            away_stats['ht_scored_15_pct'] >= 90):
            strategies.append("⚽ +1.5 GOLS HT")
            
        # +2.5 GOLS HT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 1.7 and
                away_stats['avg_goals_scored_ft'] >= 1.7 and
                avg_btts >= 75 and
                home_stats['ht_scored_25_pct'] >= 75 and
                away_stats['ht_scored_25_pct'] >= 75):
                strategies.append("⚽ +2.5 GOLS HT")
                
        # BTTS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts >= 88 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ BTTS HT")

    # =========================================================================
    # ESTRATÉGIAS FT (00:01:00 - 00:05:00) -> 60s a 300s
    # =========================================================================
    if 60 <= time_seconds <= 300:
        
        # +1.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts >= 75):
            strategies.append("⚽ +1.5 GOLS FT")
            
        # +2.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 2.0 and
            away_stats['avg_goals_scored_ft'] >= 2.0 and
            avg_btts >= 80):
            strategies.append("⚽ +2.5 GOLS FT")
            
        # +3.5 GOLS FT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] >= 2.5 and
                avg_btts >= 80):
                strategies.append("⚽ +3.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 1 = HOME)
        # +1.5 Gols Player 1
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_15_pct'] == 100 and
                home_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 1
        # Placar: 0x0, 0x1, 0x2, 1x1, 1x2
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
            if (home_stats['avg_goals_scored_ft'] >= 3.4 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_25_pct'] >= 90 and
                home_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 2 = AWAY)
        # +1.5 Gols Player 2
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 2.5 and
                avg_btts <= 60 and
                away_stats['ft_scored_15_pct'] == 100 and
                away_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 2
        # Placar: 0x0, 1x0, 2x0, 1x1, 2x1
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 3.4 and
                avg_btts <= 60 and
                away_stats['ft_scored_25_pct'] >= 90 and
                away_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

def check_strategies_12mins(event, home_stats, away_stats):
    """
    Verifica estratégias para liga de 12 minutos
    (E-soccer GT Leagues - 12 mins)
    """
    strategies = []
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    # Calcular média BTTS
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # =========================================================================
    # ESTRATÉGIAS HT (00:01:30 - 00:05:00) -> 90s a 300s
    # =========================================================================
    if 90 <= time_seconds <= 300:
        
        # +0.5 GOL HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts <= 60 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ +0.5 GOL HT")
            
        # +1.5 GOLS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts <= 60 and
            home_stats['ht_scored_15_pct'] >= 90 and
            away_stats['ht_scored_15_pct'] >= 90):
            strategies.append("⚽ +1.5 GOLS HT")
            
        # +2.5 GOLS HT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 1.7 and
                away_stats['avg_goals_scored_ft'] >= 1.7 and
                avg_btts >= 75 and
                home_stats['ht_scored_25_pct'] >= 75 and
                away_stats['ht_scored_25_pct'] >= 75):
                strategies.append("⚽ +2.5 GOLS HT")
                
        # BTTS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts >= 88 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ BTTS HT")

    # =========================================================================
    # ESTRATÉGIAS FT (00:01:30 - 00:08:30) -> 90s a 510s
    # =========================================================================
    if 90 <= time_seconds <= 510:
        
        # +1.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts >= 75):
            strategies.append("⚽ +1.5 GOLS FT")
            
        # +2.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 2.0 and
            away_stats['avg_goals_scored_ft'] >= 2.0 and
            avg_btts >= 80):
            strategies.append("⚽ +2.5 GOLS FT")
            
        # +3.5 GOLS FT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] >= 2.5 and
                avg_btts >= 80):
                strategies.append("⚽ +3.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 1 = HOME)
        # +1.5 Gols Player 1
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_15_pct'] == 100 and
                home_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 1
        # Placar: 0x0, 0x1, 0x2, 1x1, 1x2
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
            if (home_stats['avg_goals_scored_ft'] >= 3.4 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_25_pct'] >= 90 and
                home_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 2 = AWAY)
        # +1.5 Gols Player 2
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 2.5 and
                avg_btts <= 60 and
                away_stats['ft_scored_15_pct'] == 100 and
                away_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 2
        # Placar: 0x0, 1x0, 2x0, 1x1, 2x1
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 3.4 and
                avg_btts <= 60 and
                away_stats['ft_scored_25_pct'] >= 90 and
                away_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

def check_strategies_volta_6mins(event, home_stats, away_stats):
    """
    Verifica estratégias para liga Volta de 6 minutos
    (E-soccer Battle Volta - 6 mins)
    """
    strategies = []
    
    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second
    
    score = event.get('score', {})
    home_goals = score.get('home', 0)
    away_goals = score.get('away', 0)
    
    # Calcular média BTTS
    avg_btts = (home_stats['btts_pct'] + away_stats['btts_pct']) / 2
    
    home_player = event.get('homePlayer', 'Player 1')
    away_player = event.get('awayPlayer', 'Player 2')
    
    # =========================================================================
    # ESTRATÉGIAS HT (00:00:30 - 00:02:00) -> 30s a 120s
    # =========================================================================
    if 30 <= time_seconds <= 120:
        
        # +0.5 GOL HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts <= 60 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ +0.5 GOL HT")
            
        # +1.5 GOLS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts <= 60 and
            home_stats['ht_scored_15_pct'] >= 90 and
            away_stats['ht_scored_15_pct'] >= 90):
            strategies.append("⚽ +1.5 GOLS HT")
            
        # +2.5 GOLS HT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 1.7 and
                away_stats['avg_goals_scored_ft'] >= 1.7 and
                avg_btts >= 75 and
                home_stats['ht_scored_25_pct'] >= 75 and
                away_stats['ht_scored_25_pct'] >= 75):
                strategies.append("⚽ +2.5 GOLS HT")
                
        # BTTS HT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 1.3 and
            away_stats['avg_goals_scored_ft'] >= 1.3 and
            avg_btts >= 88 and
            home_stats['ht_scored_05_pct'] == 100 and
            away_stats['ht_scored_05_pct'] == 100):
            strategies.append("⚽ BTTS HT")

    # =========================================================================
    # ESTRATÉGIAS FT (00:00:30 - 00:04:25) -> 30s a 265s
    # =========================================================================
    if 90 <= time_seconds <= 265:
        
        # +1.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 0.7 and
            away_stats['avg_goals_scored_ft'] >= 0.7 and
            avg_btts >= 75):
            strategies.append("⚽ +1.5 GOLS FT")
            
        # +2.5 GOLS FT
        if (home_goals == 0 and away_goals == 0 and
            home_stats['avg_goals_scored_ft'] >= 2.0 and
            away_stats['avg_goals_scored_ft'] >= 2.0 and
            avg_btts >= 80):
            strategies.append("⚽ +2.5 GOLS FT")
            
        # +3.5 GOLS FT
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] >= 2.5 and
                avg_btts >= 80):
                strategies.append("⚽ +3.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 1 = HOME)
        # +1.5 Gols Player 1
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
            if (home_stats['avg_goals_scored_ft'] >= 2.5 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_15_pct'] == 100 and
                home_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {home_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 1
        # Placar: 0x0, 0x1, 0x2, 1x1, 1x2
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
            if (home_stats['avg_goals_scored_ft'] >= 3.4 and
                away_stats['avg_goals_scored_ft'] <= 0.8 and
                avg_btts <= 60 and
                home_stats['ft_scored_25_pct'] >= 90 and
                home_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {home_player} +2.5 GOLS FT")
                
        # ESTRATÉGIAS DE JOGADOR (PLAYER 2 = AWAY)
        # +1.5 Gols Player 2
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 2.5 and
                avg_btts <= 60 and
                away_stats['ft_scored_15_pct'] == 100 and
                away_stats['ft_scored_25_pct'] >= 90):
                strategies.append(f"⚽ {away_player} +1.5 GOLS FT")
                
        # +2.5 Gols Player 2
        # Placar: 0x0, 1x0, 2x0, 1x1, 2x1
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] <= 3.4 and
                avg_btts <= 60 and
                away_stats['ft_scored_25_pct'] >= 90 and
                away_stats['ft_scored_35_pct'] >= 80):
                strategies.append(f"⚽ {away_player} +2.5 GOLS FT")
    
    return strategies

def check_strategies_dominant_player(event, home_stats, away_stats):
    """
    Verifica estratégias de jogador dominante (FT)
    """
    strategies = []
    
    # Esta função foi substituída pelas estratégias de jogador específicas dentro de cada liga
    # Mas mantemos aqui caso queira adicionar algo genérico no futuro
    
    return strategies

# =============================================================================
# FORMATAÇÃO DE MENSAGENS
# =============================================================================

def format_tip_message(event, strategy, home_stats_summary, away_stats_summary):
    """Formata mensagem da dica"""
    league = event.get('leagueName', 'Desconhecida')
    home_player = event.get('homePlayer', '?')
    away_player = event.get('awayPlayer', '?')
    bet365_event_id = event.get('bet365EventId', '')
    
    timer = event.get('timer', {})
    time_str = timer.get('formatted', '00:00')
    
    scoreboard = event.get('scoreboard', '0-0')
    
    msg = f"\n\n<b>🏆 {league}</b>\n\n"
    msg += f"<b>🎯 {strategy}</b>\n\n"
    msg += f"⏳ Tempo: {time_str}\n\n"
    msg += f"🎮 {home_player} vs {away_player}\n"
    msg += f"⚽ Placar: {scoreboard}\n\n"
    
    # Estatísticas resumidas
    if home_stats_summary and away_stats_summary:
        msg += f"<b>📊 Últimos 5 jogos:</b>\n"
        msg += f"🏠 {home_player}:\n"
        msg += f"   HT O0.5: {home_stats_summary['ht_over_05_pct']:.0f}% | O1.5: {home_stats_summary['ht_over_15_pct']:.0f}%\n"
        msg += f"   FT Média: {home_stats_summary['avg_goals_scored_ft']:.1f} gols | Consistência +3: {home_stats_summary['consistency_ft_3_plus_pct']:.0f}%\n\n"
        
        msg += f"✈️ {away_player}:\n"
        msg += f"   HT O0.5: {away_stats_summary['ht_over_05_pct']:.0f}% | O1.5: {away_stats_summary['ht_over_15_pct']:.0f}%\n"
        msg += f"   FT Média: {away_stats_summary['avg_goals_scored_ft']:.1f} gols | Consistência +3: {away_stats_summary['consistency_ft_3_plus_pct']:.0f}%\n"
    
    # Link Bet365 com o formato correto
    if bet365_event_id:
        bet365_link = f"https://www.bet365.bet.br/?#/IP/EV{bet365_event_id}"
        msg += f"\n🌐 <a href='{bet365_link}'>🔗Bet365</a>\n\n"
    
    return msg

def format_thermometer(perc):
    """Formata termômetro visual"""
    bars = 10
    green_count = round(perc / 10)
    bar = '🟩' * green_count + '🟥' * (bars - green_count)
    return f"{bar} {perc:.0f}%"

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
        
        # Criar dicionário de jogos finalizados
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
                    
                    # Avaliar resultado
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
                        # Pode ser geral ou jogador
                        if "⚽ Player" in strategy or "⚽ " in strategy and "GOLS FT" in strategy and not strategy.startswith("⚽ +2.5 GOLS FT"):
                             # Estratégia de jogador
                             try:
                                player_name = strategy.replace("⚽ ", "").replace(" +2.5 GOLS FT", "").strip().upper()
                                if player_name == home:
                                    result = 'green' if ft_home >= 3 else 'red'
                                elif player_name == away:
                                    result = 'green' if ft_away >= 3 else 'red'
                             except:
                                 pass
                        else:
                            # Geral
                            result = 'green' if ft_total >= 3 else 'red'
                            
                    elif '+3.5 GOLS FT' in strategy:
                        result = 'green' if ft_total >= 4 else 'red'
                    elif '+4.5 GOLS FT' in strategy:
                        result = 'green' if ft_total >= 5 else 'red'
                    
                    # Outras estratégias de jogador
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
                        
                        # Editar mensagem
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
            
            # Contar resultados
            if tip['status'] == 'green': greens += 1
            if tip['status'] == 'red': reds += 1
            if tip['status'] == 'refund': refunds += 1
        
        # Enviar resumo do dia
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
        
        # Atualizar estatísticas das ligas
        await update_league_stats(bot, recent)
        
    except Exception as e:
        print(f"[ERROR] check_results: {e}")

async def update_league_stats(bot, recent_matches):
    """Atualiza e envia resumo das estatísticas das ligas"""
    global last_league_summary, last_league_message_id, league_stats
    
    try:
        # Agrupar por liga usando os dados REAIS da API
        league_games = defaultdict(list)
        
        for match in recent_matches[:200]:  # Últimos 200 jogos
            # Extrair nome da liga da API de jogos finalizados
            # A API pode ter diferentes formatos, vamos tentar todos
            league = None
            
            # Tentar diferentes campos que podem conter o nome da liga
            if 'league_name' in match:
                league = match['league_name']
            elif 'tournamentName' in match:
                league = match['tournamentName']
            elif 'leagueName' in match:
                league = match['leagueName']
            elif 'competition' in match and isinstance(match['competition'], dict):
                league = match['competition'].get('name')
            
            # Se não encontrou liga, pular
            if not league or league == 'Unknown':
                continue
            
            ht_home = match.get('home_score_ht', 0) or 0
            ht_away = match.get('away_score_ht', 0) or 0
            ft_home = match.get('home_score_ft', 0) or 0
            ft_away = match.get('away_score_ft', 0) or 0
            
            league_games[league].append({
                'ht_goals': ht_home + ht_away,
                'ft_goals': ft_home + ft_away
            })
        
        # Calcular estatísticas apenas para ligas com dados suficientes
        stats = {}
        for league, games in league_games.items():
            if len(games) < 20:  # Mínimo de 20 jogos para ter estatística válida
                continue
            
            # Pegar últimos 20 jogos (mais recentes)
            last_20 = games[:20]
            
            ht_over = {
                '0.5': sum(1 for g in last_20 if g['ht_goals'] > 0) / len(last_20) * 100,
                '1.5': sum(1 for g in last_20 if g['ht_goals'] > 1) / len(last_20) * 100,
                '2.5': sum(1 for g in last_20 if g['ht_goals'] > 2) / len(last_20) * 100,
            }
            
            ft_over = {
                '0.5': sum(1 for g in last_20 if g['ft_goals'] > 0) / len(last_20) * 100,
                '1.5': sum(1 for g in last_20 if g['ft_goals'] > 1) / len(last_20) * 100,
                '2.5': sum(1 for g in last_20 if g['ft_goals'] > 2) / len(last_20) * 100,
                '3.5': sum(1 for g in last_20 if g['ft_goals'] > 3) / len(last_20) * 100,
                '4.5': sum(1 for g in last_20 if g['ft_goals'] > 4) / len(last_20) * 100,
            }
            
            stats[league] = {'ht': ht_over, 'ft': ft_over, 'games_count': len(last_20)}
        
        # Verificar se houve mudança significativa
        if league_stats == stats:
            print("[INFO] Estatísticas das ligas sem alterações")
            return
        
        league_stats = stats
        
        if not stats:
            print("[INFO] Sem dados suficientes para estatísticas de ligas")
            return
        
        # Construir mensagem apenas se houver pelo menos uma liga
        if len(stats) == 0:
            return
        
        summary = "<b>📊 Resumo das Ligas (últimos 20 jogos)</b>\n\n"
        
        # Ordenar ligas por nome
        for league in sorted(stats.keys()):
            s = stats[league]
            summary += f"<b>{league}</b> ({s['games_count']} jogos)\n"
            
            # Mostrar apenas HT (mais relevante para nossas estratégias)
            summary += f"<b>1º Tempo (HT):</b>\n"
            for line in ['0.5', '1.5', '2.5']:
                p = s['ht'][line]
                thermo = format_thermometer(p)
                summary += f"O{line}: {thermo}\n"
            
            summary += "\n"
        
        # Destacar melhor e pior liga APENAS se houver mais de uma
        if len(stats) > 1:
            # Calcular média HT para cada liga
            league_ht_avg = {}
            for league, s in stats.items():
                avg = sum(s['ht'].values()) / len(s['ht'])
                league_ht_avg[league] = avg
            
            best_league = max(league_ht_avg.items(), key=lambda x: x[1])
            worst_league = min(league_ht_avg.items(), key=lambda x: x[1])
            
            summary += f"<b>🏆 Mais produtiva: {best_league[0]}</b> (Média HT: {best_league[1]:.1f}%)\n"
            summary += f"<b>🚫 Menos produtiva: {worst_league[0]}</b> (Média HT: {worst_league[1]:.1f}%)\n"
        
        # Enviar ou atualizar mensagem
        if summary != last_league_summary:
            if last_league_message_id:
                try:
                    await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
                    print("[INFO] Mensagem anterior de resumo das ligas deletada")
                except Exception as e:
                    print(f"[WARN] Não foi possível deletar mensagem anterior: {e}")
            
            msg = await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
            last_league_summary = summary
            last_league_message_id = msg.message_id
            print("[✓] Resumo das ligas atualizado e enviado")
        else:
            print("[INFO] Resumo das ligas sem mudanças significativas")
    
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
            
            # Buscar partidas ao vivo
            live_events = fetch_live_matches()
            
            if not live_events:
                print("[INFO] Nenhuma partida ao vivo no momento")
                await asyncio.sleep(10)
                continue
            
            # Processar cada evento
            for event in live_events:
                event_id = event.get('id')
                league_name = event.get('leagueName', '')
                home_player = event.get('homePlayer', '')
                away_player = event.get('awayPlayer', '')
                bet365_event_id = event.get('bet365EventId', '')
                
                print(f"\n[EVENTO] {event_id}: {home_player} vs {away_player} ({league_name})")
                print(f"[BET365] Event ID: {bet365_event_id}")
                
                # Pular se já enviamos dica para este evento
                if event_id in sent_match_ids:
                    continue
                
                # Buscar estatísticas individuais dos jogadores
                home_data = fetch_player_individual_stats(home_player)
                away_data = fetch_player_individual_stats(away_player)
                
                if not home_data or not away_data:
                    print(f"[WARN] Sem dados suficientes para {home_player} ou {away_player}")
                    continue
                
                # Pegar últimos jogos
                home_matches = home_data.get('matches', [])
                away_matches = away_data.get('matches', [])
                
                if len(home_matches) < 5 or len(away_matches) < 5:
                    print(f"[WARN] Dados insuficientes: {home_player}={len(home_matches)} jogos, {away_player}={len(away_matches)} jogos (mínimo: 5)")
                    continue
                
                # Analisar últimos 5 jogos
                home_stats = analyze_last_5_games(home_matches, home_player)
                away_stats = analyze_last_5_games(away_matches, away_player)
                
                if not home_stats or not away_stats:
                    print(f"[WARN] Falha na análise das estatísticas")
                    continue
                
                print(f"[STATS] {home_player} (últimos 5 jogos): HT O0.5={home_stats['ht_over_05_pct']:.0f}% O1.5={home_stats['ht_over_15_pct']:.0f}% O2.5={home_stats['ht_over_25_pct']:.0f}%")
                print(f"[STATS] {away_player} (últimos 5 jogos): HT O0.5={away_stats['ht_over_05_pct']:.0f}% O1.5={away_stats['ht_over_15_pct']:.0f}% O2.5={away_stats['ht_over_25_pct']:.0f}%")
                
                # Verificar estratégias com base na liga
                strategies = []
                
                if 'H2H GG League - 8 mins' in league_name or 'Battle - 8 mins' in league_name:
                    strategies = check_strategies_8mins(event, home_stats, away_stats)
                
                elif 'GT Leagues - 12 mins' in league_name or 'GT Leagues – 12 mins' in league_name:
                    strategies = check_strategies_12mins(event, home_stats, away_stats)
                
                elif 'Volta - 6 mins' in league_name:
                    strategies = check_strategies_volta_6mins(event, home_stats, away_stats)
                
                # Nova estratégia de jogador dominante (qualquer liga) - AGORA INTEGRADA NAS FUNÇÕES DE LIGA
                # strategies.extend(check_strategies_dominant_player(event, home_stats, away_stats))
                
                # Enviar dicas
                for strategy in strategies:
                    print(f"[✓] OPORTUNIDADE ENCONTRADA: {strategy}")
                    await send_tip(bot, event, strategy, home_stats, away_stats)
                    await asyncio.sleep(1)  # Delay entre mensagens
            
            print("[INFO] Ciclo concluído, aguardando 10 segundos...")
            await asyncio.sleep(10)
        
        except Exception as e:
            print(f"[ERROR] main_loop: {e}")
            await asyncio.sleep(10)

async def results_checker(bot):
    """Loop de verificação de resultados"""
    
    print("[INFO] Iniciando verificador de resultados...")
    
    # Aguardar inicialização
    await asyncio.sleep(30)
    
    while True:
        try:
            await check_results(bot)
            await asyncio.sleep(180)  # A cada 3 minutos (reduzido de 2)
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
    
    # Configurar request com timeouts maiores
    request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    
    bot = Bot(token=BOT_TOKEN, request=request)
    
    # Testar conexão com retentativas
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
    
    # Iniciar tarefas em paralelo
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