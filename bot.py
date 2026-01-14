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
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO


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

def detect_regime_change(matches):
    """
    Detecta mudança de estado do jogador (hot→cold ou cold→hot)
    Previne situações como Bodyaoo: 2 semanas over → 15 reds seguidos
    """
    if len(matches) < 8:
        return {'regime_change': False}
    
    # Últimos 3 jogos (MOMENTO ATUAL)
    last_3 = matches[:3]
    
    # Jogos 4-10 (HISTÓRICO RECENTE)
    previous_7 = matches[3:10] if len(matches) >= 10 else matches[3:]
    
    def avg_goals_window(window):
        total = 0
        for m in window:
            is_home = m.get('home_player', '').upper() == m.get('home_player', '').upper()
            goals = m.get('home_score_ft', 0) if is_home else m.get('away_score_ft', 0)
            total += goals or 0
        return total / len(window) if window else 0
    
    avg_last_3 = avg_goals_window(last_3)
    avg_previous = avg_goals_window(previous_7)
    
    # DETECÇÃO DE MUDANÇA DE ESTADO
    if avg_previous > 0:
        ratio = avg_last_3 / avg_previous
        
        # COOLING (jogador esfriou) - CASO BODYAOO
        if ratio < 0.5 and avg_last_3 < 1.5:
            return {
                'regime_change': True,
                'direction': 'COOLING',
                'severity': 'HIGH',
                'avg_last_3': avg_last_3,
                'avg_previous': avg_previous,
                'action': 'AVOID',
                'reason': f'Jogador esfriou drasticamente: {avg_last_3:.1f} vs {avg_previous:.1f} anterior'
            }
        
        # HEATING (jogador esquentou)
        elif ratio > 1.8 and avg_last_3 > 2.0:
            return {
                'regime_change': True,
                'direction': 'HEATING',
                'severity': 'MEDIUM',
                'avg_last_3': avg_last_3,
                'avg_previous': avg_previous,
                'action': 'BOOST',
                'reason': f'Jogador em alta: {avg_last_3:.1f} vs {avg_previous:.1f} anterior'
            }
    
    return {'regime_change': False}

def analyze_player_adaptive(matches, player_name):
    """
    Análise ADAPTATIVA - prioriza MOMENTO sobre histórico longo
    
    FILOSOFIA:
    - Últimos 3-5 jogos = 75% do peso (o que importa AGORA)
    - Jogos 6-10 = 25% do peso (contexto)
    - Detecta mudanças de estado (hot→cold) para evitar 15 reds
    - Penaliza fortemente cold streaks recentes
    """
    if not matches:
        print(f"[WARN] {player_name}: Nenhum jogo encontrado")
        return None
    
    # Mínimo de 5 jogos (não 10) - mais ágil
    min_required = 5
    if len(matches) < min_required:
        print(f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: {min_required})")
        return None
    
    # 1. DETECTAR REGIME CHANGE (crítico!)
    regime = detect_regime_change(matches)
    
    if regime['regime_change'] and regime['action'] == 'AVOID':
        print(f"[ALERT] {player_name}: REGIME CHANGE DETECTADO - {regime['reason']}")
        print(f"[ALERT] Bloqueando análise para evitar tips perigosas")
        return None  # VETO! Não analisa jogador que esfriou
    
    # 2. Usar até 5 jogos (conforme solicitado pelo usuário, foco total em recente)
    actual_n = min(len(matches), 5)
    last_n = matches[:actual_n]
    
    # 3. PESOS ADAPTATIVOS - Foco em momento (5 jogos)
    # Últimos 3 jogos = ~75% do peso total
    # Jogos 4-5 = ~25% do peso total
    
    # Pesos para 5 jogos
    weights = [
        1.0, 0.95, 0.85,  # Jogos 1-3: peso total ~2.8 (74%)
        0.50, 0.50        # Jogos 4-5: peso total ~1.0 (26%)
    ][:actual_n]
    
    total_weight = sum(weights)
    
    print(f"[DEBUG] Analisando últimos {actual_n} jogos de {player_name} (MOMENTO > histórico)")
    if regime['regime_change'] and regime['action'] == 'BOOST':
        print(f"[INFO] {player_name} em HOT STREAK: {regime['reason']}")
    
    # Acumuladores ponderados
    ht_over_05 = ht_over_15 = ht_over_25 = ht_over_35 = 0.0
    ht_scored_05 = ht_scored_15 = ht_scored_25 = 0.0
    ht_conceded_15 = 0.0
    
    ft_over_05 = ft_over_15 = ft_over_25 = ft_over_35 = ft_over_45 = 0.0
    ft_scored_05 = ft_scored_15 = ft_scored_25 = ft_scored_35 = 0.0
    
    total_goals_scored = total_goals_conceded = 0.0
    total_goals_scored_ht = total_goals_conceded_ht = 0.0
    games_scored_3_plus = btts_count = ht_btts_count = 0.0
    
    # Métricas adicionais para regime detection
    last_3_goals = []
    
    for idx, match in enumerate(last_n):
        weight = weights[idx]
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
        
        # Rastrear últimos 3 para cold streak detection
        if idx < 3:
            last_3_goals.append(player_ft_goals)
        
        # Aplicar pesos
        total_goals_scored += player_ft_goals * weight
        total_goals_conceded += player_ft_conceded * weight
        total_goals_scored_ht += player_ht_goals * weight
        total_goals_conceded_ht += player_ht_conceded * weight
        
        if player_ft_goals >= 3: games_scored_3_plus += weight
        if ft_home > 0 and ft_away > 0: btts_count += weight
        if ht_home > 0 and ht_away > 0: ht_btts_count += weight
        
        # HT Overs (ponderados)
        if ht_total > 0: ht_over_05 += weight
        if ht_total > 1: ht_over_15 += weight
        if ht_total > 2: ht_over_25 += weight
        if ht_total > 3: ht_over_35 += weight
        
        # HT Individual (ponderados)
        if player_ht_goals > 0: ht_scored_05 += weight
        if player_ht_goals > 1: ht_scored_15 += weight
        if player_ht_goals > 2: ht_scored_25 += weight
        if player_ht_conceded > 1: ht_conceded_15 += weight
        
        # FT Overs (ponderados)
        if ft_total > 0: ft_over_05 += weight
        if ft_total > 1: ft_over_15 += weight
        if ft_total > 2: ft_over_25 += weight
        if ft_total > 3: ft_over_35 += weight
        if ft_total > 4: ft_over_45 += weight
        
        # FT Individual (ponderados)
        if player_ft_goals > 0: ft_scored_05 += weight
        if player_ft_goals > 1: ft_scored_15 += weight
        if player_ft_goals > 2: ft_scored_25 += weight
        if player_ft_goals > 3: ft_scored_35 += weight
    
    # COLD STREAK DETECTION (últimos 3 jogos ruins)
    cold_streak = False
    if len(last_3_goals) == 3:
        # Se 2 dos últimos 3 jogos têm 0 ou 1 gol
        low_scoring = sum(1 for g in last_3_goals if g <= 1)
        if low_scoring >= 2:
            cold_streak = True
            print(f"[WARN] {player_name}: COLD STREAK detectado - últimos 3 jogos: {last_3_goals}")
    
    return {
        'ht_over_05_pct': (ht_over_05 / total_weight) * 100,
        'ht_over_15_pct': (ht_over_15 / total_weight) * 100,
        'ht_over_25_pct': (ht_over_25 / total_weight) * 100,
        'ht_over_35_pct': (ht_over_35 / total_weight) * 100,
        'ht_scored_05_pct': (ht_scored_05 / total_weight) * 100,
        'ht_scored_15_pct': (ht_scored_15 / total_weight) * 100,
        'ht_scored_25_pct': (ht_scored_25 / total_weight) * 100,
        'ht_conceded_15_pct': (ht_conceded_15 / total_weight) * 100,
        'ft_over_05_pct': (ft_over_05 / total_weight) * 100,
        'ft_over_15_pct': (ft_over_15 / total_weight) * 100,
        'ft_over_25_pct': (ft_over_25 / total_weight) * 100,
        'ft_over_35_pct': (ft_over_35 / total_weight) * 100,
        'ft_over_45_pct': (ft_over_45 / total_weight) * 100,
        'ft_scored_05_pct': (ft_scored_05 / total_weight) * 100,
        'ft_scored_15_pct': (ft_scored_15 / total_weight) * 100,
        'ft_scored_25_pct': (ft_scored_25 / total_weight) * 100,
        'ft_scored_35_pct': (ft_scored_35 / total_weight) * 100,
        'avg_goals_scored_ft': total_goals_scored / total_weight,
        'avg_goals_conceded_ft': total_goals_conceded / total_weight,
        'avg_goals_scored_ht': total_goals_scored_ht / total_weight,
        'avg_goals_conceded_ht': total_goals_conceded_ht / total_weight,
        'consistency_ft_3_plus_pct': (games_scored_3_plus / total_weight) * 100,
        'btts_pct': (btts_count / total_weight) * 100,
        'ht_btts_pct': (ht_btts_count / total_weight) * 100,
        'games_analyzed': actual_n,
        'regime_change': regime['regime_change'],
        'regime_direction': regime.get('direction', 'STABLE'),
        'cold_streak': cold_streak,  # Flag de cold streak
        'last_3_goals': last_3_goals  # Para debugging
    }

def calculate_confidence(home_stats, away_stats, league_stats, h2h_data, strategy, league_key):
    """
    Calcula score de confiança de 0-100 para uma tip
    Baseado em: consistência dos jogadores, performance da liga, H2H e sample size
    
    NOVO: Penaliza FORTEMENTE cold streaks e recompensa hot streaks
    """
    confidence = 0.0
    
    # ========== VERIFICAÇÃO CRÍTICA: COLD STREAK ==========
    # Se qualquer jogador está em cold streak, penalizar BRUTALMENTE
    if home_stats.get('cold_streak', False) or away_stats.get('cold_streak', False):
        print(f"[WARN] Cold streak detectado - Penalidade de -25 pontos no confidence")
        confidence -= 25  # Penalidade massiva
    
    # ========== VERIFICAÇÃO: HOT STREAK ==========
    # Se está em hot streak (regime heating), boost significativo
    if home_stats.get('regime_direction') == 'HEATING' or away_stats.get('regime_direction') == 'HEATING':
        print(f"[INFO] Hot streak detectado - Bonus de +10 pontos no confidence")
        confidence += 10  # Recompensa
    
    # ========== FATOR 1: Consistência dos Jogadores (40 pontos) ==========
    # Quanto mais consistentes as estatísticas, maior a confiança
    
    # Métricas relevantes baseadas na estratégia
    if 'HT' in strategy:
        # Para estratégias HT, verificar consistência HT
        home_consistency = (home_stats['ht_over_05_pct'] + home_stats['ht_over_15_pct']) / 2
        away_consistency = (away_stats['ht_over_05_pct'] + away_stats['ht_over_15_pct']) / 2
    else:
        # Para estratégias FT, verificar consistência FT
        home_consistency = (home_stats['ft_over_15_pct'] + home_stats['ft_over_25_pct']) / 2
        away_consistency = (away_stats['ft_over_15_pct'] + away_stats['ft_over_25_pct']) / 2
    
    avg_consistency = (home_consistency + away_consistency) / 2
    confidence += (avg_consistency / 100) * 40
    
    # ========== FATOR 2: Performance da Liga (30 pontos) ==========
    if league_key and league_key in league_stats:
        l_stats = league_stats[league_key]
        
        # Verificar métrica da liga baseado na estratégia
        if '+0.5 GOL HT' in strategy or '+0.5 GOLS HT' in strategy:
            league_metric = l_stats['ht']['o05']
        elif '+1.5 GOLS HT' in strategy:
            league_metric = l_stats['ht']['o15']
        elif '+2.5 GOLS HT' in strategy:
            league_metric = l_stats['ht']['o25']
        elif 'BTTS HT' in strategy:
            league_metric = l_stats['ht']['btts']
        elif '+1.5 GOLS FT' in strategy:
            league_metric = l_stats['ft']['o15']
        elif '+2.5 GOLS FT' in strategy:
            league_metric = l_stats['ft']['o25']
        elif '+3.5 GOLS FT' in strategy:
            league_metric = max(l_stats['ft']['o25'], 70)  # Proxy
        else:
            # Média geral
            league_metric = (l_stats['ft']['o15'] + l_stats['ft']['o25']) / 2
        
        confidence += (league_metric / 100) * 30
        
        # ========== PENALIDADE: LIGA FRACA (Base Rate Fallacy) ==========
        # Se a média da liga é baixa (< 65%), exige performance EXCEPCIONAL dos players
        # Penaliza confidence para evitar entradas "médias" em ligas "ruins"
        if league_metric < 65:
            penalty = 15  # Penalidade severa
            # Se for muito baixa (< 55%), penaliza mais ainda
            if league_metric < 55:
                penalty = 25
            
            print(f"[WARN] Liga com desempenho baixo ({league_metric}%) - Penalidade de -{penalty} no confidence")
            confidence -= penalty

    else:
        # Se não tiver dados da liga, penalizar levemente
        confidence += 15  # 50% do máximo possível
    
    # ========== FATOR 3: H2H Histórico (20 pontos) ==========
    if h2h_data and h2h_data.get('total_matches', 0) >= 3:
        # Se houver pelo menos 3 jogos H2H, usar dados
        h2h_total_goals = h2h_data.get('avg_total_goals', 0)
        h2h_btts_pct = h2h_data.get('btts_pct', 0)
        
        # Verificar se H2H suporta a estratégia
        h2h_support = 50  # baseline
        
        if 'BTTS' in strategy:
            h2h_support = h2h_btts_pct
        elif '+2.5 GOLS' in strategy:
            # 2.5+ gols = média >= 3
            h2h_support = min(100, (h2h_total_goals / 3.0) * 100)
        elif '+1.5 GOLS' in strategy:
            # 1.5+ gols = média >= 2
            h2h_support = min(100, (h2h_total_goals / 2.0) * 100)
        elif '+0.5 GOL' in strategy:
            # 0.5+ gols = média >= 1
            h2h_support = min(100, (h2h_total_goals / 1.0) * 100)
        
        confidence += (h2h_support / 100) * 20
    else:
        # Sem H2H suficiente, usar valor neutro
        confidence += 10  # 50% do máximo possível
    
    # ========== FATOR 4: Tamanho da Amostra (10 pontos) ==========
    min_games = min(
        home_stats.get('games_analyzed', 10),
        away_stats.get('games_analyzed', 10)
    )
    # Quanto mais jogos analisados, maior a confiança (máximo em 10 jogos agora)
    sample_score = min(min_games / 10.0, 1.0)
    confidence += sample_score * 10
    
    # Garantir que confidence está entre 0 e 100
    confidence = max(0, min(100, confidence))
    
    return round(confidence, 1)

# =============================================================================
# LÓGICA DE ESTRATÉGIAS
# =============================================================================

def check_strategies_8mins(event, home_stats, away_stats, all_league_stats, h2h_data=None):
    """Estratégias para ligas de 8 minutos com thresholds otimizados e confidence score"""
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
            l_stats['ht']['o05'] >= 95):  # Relaxado de 100
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 40 and  # Relaxado de 45
                home_stats['ht_over_05_pct'] >= 85 and  # Relaxado de 90
                away_stats['ht_over_05_pct'] >= 85):
                strategy = "⚽ +0.5 GOL HT"
                confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                strategies.append({'strategy': strategy, 'confidence': confidence})
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['o15'] >= 90):  # Relaxado de 95
            if (home_stats['avg_goals_scored_ft'] >= 0.9 and  # Relaxado de 1.0
                away_stats['avg_goals_scored_ft'] >= 0.9 and
                avg_btts >= 40 and  # Relaxado de 45
                home_stats['ht_over_15_pct'] >= 75 and  # Relaxado de 80
                away_stats['ht_over_15_pct'] >= 75):
                strategy = "⚽ +1.5 GOLS HT"
                confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                strategies.append({'strategy': strategy, 'confidence': confidence})
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ht']['o25'] >= 85):  # Relaxado de 90
                if (home_stats['avg_goals_scored_ft'] >= 1.4 and  # Relaxado de 1.5
                    away_stats['avg_goals_scored_ft'] >= 1.4 and
                    avg_btts >= 70 and  # Relaxado de 75
                    home_stats['ht_over_15_pct'] >= 85 and  # Relaxado de == 100
                    away_stats['ht_over_15_pct'] >= 85):
                    strategy = "⚽ +2.5 GOLS HT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
                
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ht']['btts'] >= 85):  # Relaxado de 90
            if (home_stats['avg_goals_scored_ft'] >= 1.2 and  # Relaxado de 1.3
                away_stats['avg_goals_scored_ft'] >= 1.2 and
                avg_btts >= 80 and  # Relaxado de 85
                home_stats['ht_over_05_pct'] >= 90 and  # Relaxado de == 100
                away_stats['ht_over_05_pct'] >= 90):
                strategy = "⚽ BTTS HT"
                confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                strategies.append({'strategy': strategy, 'confidence': confidence})

    # FT (180s - 360s)
    if 180 <= time_seconds <= 360:
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o15'] >= 90):  # Relaxado de 95
            if (home_stats['avg_goals_scored_ft'] >= 0.6 and  # Relaxado de 0.7
                away_stats['avg_goals_scored_ft'] >= 0.6 and
                avg_btts >= 70):  # Relaxado de 75
                strategy = "⚽ +1.5 GOLS FT"
                confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                strategies.append({'strategy': strategy, 'confidence': confidence})
            
        if (home_goals == 0 and away_goals == 0 and
            l_stats['ft']['o25'] >= 85):  # Relaxado de 90
            if (home_stats['avg_goals_scored_ft'] >= 1.8 and  # Relaxado de 2.0
                away_stats['avg_goals_scored_ft'] >= 1.8 and
                avg_btts >= 75):  # Relaxado de 80
                strategy = "⚽ +2.5 GOLS FT"
                confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                strategies.append({'strategy': strategy, 'confidence': confidence})
            
        if ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1)):
            if (l_stats['ft']['o25'] >= 85):  # Relaxado de 90
                if (home_stats['avg_goals_scored_ft'] >= 2.3 and  # Relaxado de 2.5
                    away_stats['avg_goals_scored_ft'] >= 2.3 and
                    avg_btts >= 75):  # Relaxado de 80
                    strategy = "⚽ +3.5 GOLS FT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
                
    # Estratégias de jogador (90s - 360s)
    if 90 <= time_seconds <= 360:
        # Player 1.5 FT check
        if (home_goals == 0 and away_goals == 0) or (home_goals == 0 and away_goals == 1):
             if (l_stats['ft']['o15'] >= 90):  # Relaxado de 95
                if (home_stats['avg_goals_scored_ft'] >= 1.8 and  # Relaxado de 2.0
                    away_stats['avg_goals_scored_ft'] <= 1.7 and  # Relaxado de 1.5
                    avg_btts <= 70 and
                    home_stats['ft_scored_15_pct'] >= 75 and  # Relaxado de 80
                    home_stats['ft_scored_25_pct'] >= 55):  # Relaxado de 60
                    strategy = f"⚽ {home_player} +1.5 GOLS FT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
                
        valid_scores_p1 = [(0,0), (0,1), (0,2), (1,1), (1,2)]
        if (home_goals, away_goals) in valid_scores_p1:
             if (l_stats['ft']['o25'] >= 85):  # Relaxado de 90
                if (home_stats['avg_goals_scored_ft'] >= 2.7 and  # Relaxado de 3.0
                    away_stats['avg_goals_scored_ft'] <= 1.2 and  # Relaxado de 1.0
                    avg_btts <= 60 and
                    home_stats['ft_scored_25_pct'] >= 75 and  # Relaxado de 80
                    home_stats['ft_scored_35_pct'] >= 55):  # Relaxado de 60
                    strategy = f"⚽ {home_player} +2.5 GOLS FT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
                
        if (home_goals == 0 and away_goals == 0) or (home_goals == 1 and away_goals == 0):
             if (l_stats['ft']['o15'] >= 90):  # Relaxado de 95
                if (away_stats['avg_goals_scored_ft'] >= 0.7 and  # Relaxado de 0.8
                    away_stats['avg_goals_scored_ft'] <= 2.7 and  # Relaxado de 2.5
                    avg_btts <= 70 and
                    away_stats['ft_scored_15_pct'] >= 75 and  # Relaxado de 80
                    away_stats['ft_scored_25_pct'] >= 55):  # Relaxado de 60
                    strategy = f"⚽ {away_player} +1.5 GOLS FT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
                
        valid_scores_p2 = [(0,0), (1,0), (2,0), (1,1), (2,1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (l_stats['ft']['o25'] >= 85):  # Relaxado de 90
                if (away_stats['avg_goals_scored_ft'] >= 0.7 and  # Relaxado de 0.8
                    away_stats['avg_goals_scored_ft'] <= 3.6 and  # Relaxado de 3.4
                    avg_btts <= 60 and
                    away_stats['ft_scored_25_pct'] >= 75 and  # Relaxado de 80
                    away_stats['ft_scored_35_pct'] >= 55):  # Relaxado de 60
                    strategy = f"⚽ {away_player} +2.5 GOLS FT"
                    confidence = calculate_confidence(home_stats, away_stats, all_league_stats, h2h_data, strategy, league_key)
                    strategies.append({'strategy': strategy, 'confidence': confidence})
    
    return strategies

def check_strategies_12mins(event, home_stats, away_stats, all_league_stats, h2h_data=None):
    """Estratégias para liga de 12 minutos com thresholds otimizados e confidence score"""
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

def check_strategies_volta_6mins(event, home_stats, away_stats, all_league_stats, h2h_data=None):
    """Estratégias para liga Volta de 6 minutos com thresholds otimizados e confidence score"""
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

def format_tip_message(event, strategy, home_stats_summary, away_stats_summary, confidence=0):
    """Formata mensagem da dica com confidence score"""
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
    msg += f"💎 <b>{strategy}</b>\n"
    
    # Confidence Score (se disponível)
    if confidence > 0:
        # Determinar emoji baseado no confidence
        if confidence >= 85:
            conf_emoji = "🟢"
        elif confidence >= 75:
            conf_emoji = "🟡"
        else:
            conf_emoji = "🟠"
        msg += f"{conf_emoji} <b>Confiança: {confidence:.1f}%</b>\n\n"
    else:
        msg += "\n"
    
    # Informações do jogo
    msg += f"⏱ <b>Tempo:</b> {time_str} | 📊 <b>Placar:</b> {scoreboard}\n"
    msg += f"🎮 <b>{home_player}</b> vs <b>{away_player}</b>\n\n"
    
    # Estatísticas formatadas
    if home_stats_summary and away_stats_summary:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📈 <b>ANÁLISE - ÚLTIMOS {home_stats_summary.get('games_analyzed', 15)} JOGOS</b>\n"
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
def calculate_tip_averages(home_stats, away_stats):
    """Calcula as médias HT e Geral para filtragem"""
    if not home_stats or not away_stats:
        return 0, 0

    # Métricas HT: +0.5, +1.5, +2.5 e BTTS HT
    ht_metrics = [
        home_stats.get('ht_over_05_pct', 0), home_stats.get('ht_over_15_pct', 0),
        home_stats.get('ht_over_25_pct', 0), home_stats.get('ht_btts_pct', 0),
        away_stats.get('ht_over_05_pct', 0), away_stats.get('ht_over_15_pct', 0),
        away_stats.get('ht_over_25_pct', 0), away_stats.get('ht_btts_pct', 0)
    ]
    
    # Métricas FT: +1.5, +2.5 e BTTS FT
    ft_metrics = [
        home_stats.get('ft_over_15_pct', 0), home_stats.get('ft_over_25_pct', 0),
        home_stats.get('btts_pct', 0),
        away_stats.get('ft_over_15_pct', 0), away_stats.get('ft_over_25_pct', 0),
        away_stats.get('btts_pct', 0)
    ]

    avg_ht = sum(ht_metrics) / len(ht_metrics) if ht_metrics else 0
    all_metrics = ht_metrics + ft_metrics
    avg_total = sum(all_metrics) / len(all_metrics) if all_metrics else 0

    return avg_ht, avg_total


async def send_tip(bot, event, strategy_dict, home_stats, away_stats):
    """Envia uma dica para o Telegram com filtro de confidence"""
    event_id = event.get('id')
    
    if event_id in sent_match_ids:
        return
    
    # Extrair strategy e confidence do dict
    strategy = strategy_dict.get('strategy', strategy_dict)  # Fallback para compatibilidade
    confidence = strategy_dict.get('confidence', 0)
    
    # Filtro de confidence mínimo
    if confidence > 0 and confidence < 80:
        print(f"[INFO] Dica ignorada: Confidence ({confidence:.1f}%) abaixo de 80%")
        return

    # Verificação de Médias (Filtro legado - mantido como backup)
    avg_ht, avg_total = calculate_tip_averages(home_stats, away_stats)
    
    # Logs para depuração
    print(f"[DEBUG] {event.get('homePlayer')} vs {event.get('awayPlayer')} | Confidence: {confidence:.1f}% | Média HT: {avg_ht:.1f}% | Média Geral: {avg_total:.1f}%")

    # Se tiver confidence, priorizar ele; senão usar filtro legado
    if confidence > 0:
        # Sistema novo: usa confidence
        if confidence < 80:
            print(f"[INFO] Dica ignorada: Confidence ({confidence:.1f}%) abaixo de 80%")
            return
    else:
        # Sistema legado: usa médias antigas
        if avg_ht < 85:
            print(f"[INFO] Dica ignorada: Média HT ({avg_ht:.1f}%) abaixo de 85%")
            return
        
        if avg_total < 92:
            print(f"[INFO] Dica ignorada: Média Geral ({avg_total:.1f}%) abaixo de 92%")
            return

    max_retries = 3
    for attempt in range(max_retries):
        try:
            msg = format_tip_message(event, strategy, home_stats, away_stats, confidence)
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
                'confidence': confidence,
                'sent_time': datetime.now(MANAUS_TZ),
                'status': 'pending',
                'message_id': message_obj.message_id,
                'message_text': msg,
                'home_player': event.get('homePlayer'),
                'away_player': event.get('awayPlayer'),
                'league': event.get('leagueName', 'Outros')
            })
            
            # Normalizar nome da liga na tip recém enviada
            league_mapping = {
                'Esoccer GT Leagues – 12 mins play': 'GT LEAGUE 12 MIN',
                'Esoccer GT Leagues - 12 mins play': 'GT LEAGUE 12 MIN',
                'Esoccer Battle Volta - 6 mins play': 'VOLTA 6 MIN',
                'Esoccer H2H GG League - 8 mins play': 'H2H 8 MIN',
                'Esoccer Battle - 8 mins play': 'BATTLE 8 MIN'
            }
            raw_league = sent_tips[-1]['league']
            for key, value in league_mapping.items():
                if key in raw_league:
                    sent_tips[-1]['league'] = value
                    break
            
            print(f"[✓] Dica enviada: {event_id} - {strategy} (Confidence: {confidence:.1f}%)")
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
        
        # Agrupar partidas por jogadores, mantendo múltiplas partidas
        finished_matches = defaultdict(list)
        for match in recent:
            home = match.get('home_player', '').upper()
            away = match.get('away_player', '').upper()
            key = f"{home}_{away}"
            finished_matches[key].append(match)
        
        today = datetime.now(MANAUS_TZ).date()
        greens = reds = refunds = 0
        
        for tip in sent_tips:
            if tip['sent_time'].date() != today:
                continue
            
            if tip['status'] == 'pending':
                home = (tip.get('home_player') or '').upper()
                away = (tip.get('away_player') or '').upper()
                key = f"{home}_{away}"
                
                # Buscar a partida correta baseada no horário
                matches_for_players = finished_matches.get(key, [])
                match = None
                
                for m in matches_for_players:
                    match_time_str = m.get('data_realizacao', '')
                    if match_time_str:
                        try:
                            # Parse da data da partida
                            match_time = datetime.fromisoformat(match_time_str.replace('Z', '+00:00'))
                            
                            # Converter para timezone de Manaus
                            if match_time.tzinfo is None:
                                match_time = match_time.replace(tzinfo=timezone.utc)
                            match_time_local = match_time.astimezone(MANAUS_TZ)
                            
                            tip_time = tip['sent_time']
                            
                            # A partida deve ter sido realizada DEPOIS do envio da tip
                            # Margem: 5 min antes (tolerância) até 30 min depois do envio
                            time_diff = (match_time_local - tip_time).total_seconds()
                            
                            # Partida ocorreu entre 5 min antes e 30 min depois do envio
                            if -300 <= time_diff <= 1800:
                                match = m
                                print(f"[DEBUG] Partida encontrada para {key}: {match_time_str} (diff: {time_diff/60:.1f} min)")
                                break
                        except Exception as e:
                            print(f"[WARN] Erro ao parsear data da partida: {e}")
                            continue
                
                # Se não encontrou pelo horário, NÃO usar partida antiga
                if not match:
                    continue
                
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
    """Atualiza e envia resumo das estatísticas das ligas com imagem"""
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
                },
                'ft': {
                    'o15': calc_pct(sum(1 for g in last_n if g['ft_goals'] > 1)),
                    'o25': calc_pct(sum(1 for g in last_n if g['ft_goals'] > 2)),
                    'btts': calc_pct(sum(1 for g in last_n if g['ft_btts'])),
                },
                'count': total
            }
        
        if not stats: return
        
        # Comparação exata dos dicionários
        if league_stats and league_stats == stats:
            print(f"[INFO] Resumo de ligas idêntico ao anterior. Ignorando envio.")
            return
            
        league_stats = stats
        
        # ============ GERAR IMAGEM ============
        img = create_league_stats_image(stats)
        
        # Converter para BytesIO
        bio = BytesIO()
        img.save(bio, 'PNG')
        bio.seek(0)
        
        # Enviar imagem
        if last_league_message_id:
            try:
                await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
            except: pass
        
        msg = await bot.send_photo(
            chat_id=CHAT_ID,
            photo=bio,
            caption="📊 <b>ANÁLISE DE LIGAS</b> (Últimos 5 jogos)\n<i>🔴&lt;48% 🟠48-77% 🟡78-94% 🟢95%+</i>",
            parse_mode="HTML"
        )
        
        last_league_message_id = msg.message_id
        print("[✓] Resumo das ligas atualizado com imagem")
    
    except Exception as e:
        print(f"[ERROR] update_league_stats: {e}")
        import traceback
        traceback.print_exc()


def create_league_stats_image(stats):
    """Cria imagem com heatmap das estatísticas"""
    import os
    
    # Cores - FUNDO PRETO
    bg_color = (0, 0, 0)  # Preto puro
    card_bg = (20, 20, 20)  # Cinza muito escuro
    header_bg = (30, 30, 30)  # Cinza escuro
    text_color = (255, 255, 255)
    header_color = (0, 255, 200)  # Cyan/Verde
    gold_color = (255, 200, 50)  # Dourado
    brand_color = (0, 255, 100)  # Verde para RW TIPS
    
    # Configurações
    sorted_leagues = sorted(stats.keys())
    num_leagues = len(sorted_leagues)
    
    # Dimensões GRANDES
    cell_width = 160
    cell_height = 90
    label_width = 300
    logo_height = 80  # Altura para logo + branding
    header_height = 140  # Aumentado para caber logo
    padding = 40
    
    total_width = label_width + (6 * cell_width) + (2 * padding)
    total_height = header_height + (num_leagues * cell_height) + (2 * padding) + 120
    
    # Criar imagem
    img = Image.new('RGB', (total_width, total_height), bg_color)
    draw = ImageDraw.Draw(img)
    
    # Tamanhos das fontes
    size_title = 30
    size_header = 25
    size_cell = 35
    size_league = 25
    size_brand = 35  # Para RW TIPS
    
    # Lista de fontes para tentar (Windows e Linux)
    font_paths = [
        # Windows
        "C:\\Windows\\Fonts\\arialbd.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
        # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        # Genéricos
        "arial.ttf",
        "DejaVuSans-Bold.ttf",
    ]
    
    font_title = None
    font_header = None
    font_cell = None
    font_league = None
    font_brand = None
    font_loaded = False
    
    for font_path in font_paths:
        try:
            font_title = ImageFont.truetype(font_path, size_title)
            font_header = ImageFont.truetype(font_path, size_header)
            font_cell = ImageFont.truetype(font_path, size_cell)
            font_league = ImageFont.truetype(font_path, size_league)
            font_brand = ImageFont.truetype(font_path, size_brand)
            font_loaded = True
            print(f"[INFO] Fonte carregada: {font_path}")
            break
        except Exception as e:
            continue
    
    # Fallback para fonte padrão com tamanho customizado
    if not font_loaded:
        print("[WARN] Usando fonte padrão do sistema")
        try:
            font_title = ImageFont.load_default(size=size_title)
            font_header = ImageFont.load_default(size=size_header)
            font_cell = ImageFont.load_default(size=size_cell)
            font_league = ImageFont.load_default(size=size_league)
            font_brand = ImageFont.load_default(size=size_brand)
        except:
            font_title = ImageFont.load_default()
            font_header = ImageFont.load_default()
            font_cell = ImageFont.load_default()
            font_league = ImageFont.load_default()
            font_brand = ImageFont.load_default()
    
    # ===== LOGO E BRANDING =====
    logo_size = 50
    brand_text = "RW TIPS"
    
    # Tentar carregar a logo
    logo_path = os.path.join(os.path.dirname(__file__), "app_icon.png")
    logo_loaded = False
    
    try:
        logo = Image.open(logo_path).convert("RGBA")
        logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
        logo_loaded = True
        print(f"[INFO] Logo carregada: {logo_path}")
    except Exception as e:
        print(f"[WARN] Não foi possível carregar logo: {e}")
    
    # Calcular posição centralizada para logo + texto
    brand_bbox = draw.textbbox((0, 0), brand_text, font=font_brand)
    brand_w = brand_bbox[2] - brand_bbox[0]
    
    if logo_loaded:
        total_brand_width = logo_size + 15 + brand_w
        start_x = (total_width - total_brand_width) // 2
        
        # Colar logo
        logo_y = padding
        img.paste(logo, (start_x, logo_y), logo)
        
        # Texto RW TIPS
        text_x = start_x + logo_size + 15
        text_y = padding + (logo_size - size_brand) // 2
        draw.text((text_x, text_y), brand_text, fill=brand_color, font=font_brand)
    else:
        # Só texto se não tiver logo
        draw.text(((total_width - brand_w) // 2, padding), brand_text, fill=brand_color, font=font_brand)
    
    # Título secundário
    title = "ANALISE DE LIGAS (5 jogos)"
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    title_y = padding + logo_size + 10
    draw.text(((total_width - title_width) // 2, title_y), title, fill=header_color, font=font_title)
    
    # Headers das colunas
    headers = ["HT 0.5+", "HT 1.5+", "HT BTTS", "FT 1.5+", "FT 2.5+", "FT BTTS"]
    y_pos = title_y + 100  # Espaço maior após o título
    
    for i, header in enumerate(headers):
        x_pos = label_width + (i * cell_width) + padding
        
        # Background do header
        draw.rectangle(
            [x_pos, y_pos - 35, x_pos + cell_width, y_pos - 5],
            fill=header_bg,
            outline=card_bg,
            width=2
        )
        
        # Texto do header
        header_bbox = draw.textbbox((0, 0), header, font=font_header)
        header_w = header_bbox[2] - header_bbox[0]
        draw.text(
            (x_pos + (cell_width - header_w) // 2, y_pos - 28),
            header,
            fill=header_color,
            font=font_header
        )
    
    # Função para obter cor baseada na porcentagem (novos limites)
    def get_heat_color(pct):
        if pct >= 95:
            return (0, 255, 136)  # Verde
        elif pct >= 78:
            return (255, 238, 68)  # Amarelo
        elif pct >= 48:
            return (255, 136, 68)  # Laranja
        else:
            return (255, 68, 68)  # Vermelho
    
    # Desenhar linhas de ligas
    for idx, league in enumerate(sorted_leagues):
        s = stats[league]
        row_y = y_pos + (idx * cell_height)
        
        # Nome da liga
        draw.rectangle(
            [padding, row_y, label_width + padding - 10, row_y + cell_height],
            fill=header_bg,
            outline=card_bg,
            width=2
        )
        
        league_text = f"{league}"
        draw.text(
            (padding + 10, row_y + 15),
            league_text,
            fill=gold_color,
            font=font_league
        )
        
        # Células de dados
        values = [
            s['ht']['o05'],
            s['ht']['o15'],
            s['ht']['btts'],
            s['ft']['o15'],
            s['ft']['o25'],
            s['ft']['btts']
        ]
        
        for i, val in enumerate(values):
            x_pos = label_width + (i * cell_width) + padding
            
            # Background com cor baseada no valor
            color = get_heat_color(val)
            draw.rectangle(
                [x_pos, row_y, x_pos + cell_width, row_y + cell_height],
                fill=color,
                outline=card_bg,
                width=2
            )
            
            # Texto da porcentagem
            text = f"{val}%"
            text_bbox = draw.textbbox((0, 0), text, font=font_cell)
            text_w = text_bbox[2] - text_bbox[0]
            
            # Cor do texto (branco para escuro, preto para claro)
            text_color_cell = (0, 0, 0) if val >= 60 else (255, 255, 255)
            
            draw.text(
                (x_pos + (cell_width - text_w) // 2, row_y + 15),
                text,
                fill=text_color_cell,
                font=font_cell
            )
    
    # Calcular qual liga é melhor para OVER e UNDER
    league_scores = {}
    for league in sorted_leagues:
        s = stats[league]
        # Média de todos os 6 valores (HT 0.5+, HT 1.5+, HT BTTS, FT 1.5+, FT 2.5+, FT BTTS)
        avg_over = (s['ht']['o05'] + s['ht']['o15'] + s['ht']['btts'] + s['ft']['o15'] + s['ft']['o25'] + s['ft']['btts']) / 6
        league_scores[league] = avg_over
    
    best_over = max(league_scores, key=league_scores.get)
    best_under = min(league_scores, key=league_scores.get)
    
    # Linha de destaque para OVER (sem emoji)
    highlight_y = y_pos + (num_leagues * cell_height) + 15
    over_text = f">> MELHOR OVER: {best_over} ({league_scores[best_over]:.0f}% media)"
    over_bbox = draw.textbbox((0, 0), over_text, font=font_header)
    over_w = over_bbox[2] - over_bbox[0]
    draw.text(
        ((total_width - over_w) // 2, highlight_y),
        over_text,
        fill=(0, 255, 136),  # Verde
        font=font_header
    )
    
    # Linha de destaque para UNDER (sem emoji) - VERMELHO
    under_y = highlight_y + 28
    under_text = f">> LIGA UNDER: {best_under} ({league_scores[best_under]:.0f}% media)"
    under_bbox = draw.textbbox((0, 0), under_text, font=font_header)
    under_w = under_bbox[2] - under_bbox[0]
    draw.text(
        ((total_width - under_w) // 2, under_y),
        under_text,
        fill=(255, 68, 68),  # Vermelho
        font=font_header
    )
    
    return img
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
                
                # Mínimo de 5 jogos (não 10) - sistema adapt adaptivo
                if len(home_matches) < 5 or len(away_matches) < 5:
                    print(f"[WARN] Dados insuficientes: {home_player}={len(home_matches)} jogos, {away_player}={len(away_matches)} jogos (mínimo: 5)")
                    continue
                
                # Análise ADAPTATIVA - prioriza momento (últimos 3-5 jogos)
                home_stats = analyze_player_adaptive(home_matches, home_player)
                away_stats = analyze_player_adaptive(away_matches, away_player)
                
                if not home_stats or not away_stats:
                    print(f"[WARN] Falha na análise das estatísticas")
                    continue
                
                print(f"[STATS] {home_player} (últimos {home_stats.get('games_analyzed', 10)} jogos | Momento prioritário): HT O0.5={home_stats['ht_over_05_pct']:.0f}% O1.5={home_stats['ht_over_15_pct']:.0f}% O2.5={home_stats['ht_over_25_pct']:.0f}%")
                print(f"[STATS] {away_player} (últimos {away_stats.get('games_analyzed', 10)} jogos | Momento prioritário): HT O0.5={away_stats['ht_over_05_pct']:.0f}% O1.5={away_stats['ht_over_15_pct']:.0f}% O2.5={away_stats['ht_over_25_pct']:.0f}%")

                
                # Buscar dados H2H
                print(f"[INFO] Buscando dados H2H...")
                h2h_data = fetch_h2h_data(home_player, away_player)
                
                strategies = []
                
                if 'H2H GG League - 8 mins' in league_name or 'Battle - 8 mins' in league_name:
                    strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats, h2h_data)
                
                elif 'GT Leagues - 12 mins' in league_name or 'GT Leagues – 12 mins' in league_name:
                    # TODO: Aplicar mesmas otimizações da 8min (thresholds relaxados + confidence)
                    strategies = check_strategies_12mins(event, home_stats, away_stats, league_stats, h2h_data)
                
                elif 'Volta - 6 mins' in league_name:
                    # TODO: Aplicar mesmas otimizações da 8min (thresholds relaxados + confidence)
                    strategies = check_strategies_volta_6mins(event, home_stats, away_stats, league_stats, h2h_data)
                
                
                for strategy_dict in strategies:
                    # Lidar com formato antigo (string) e novo (dict)
                    if isinstance(strategy_dict, dict):
                        strategy = strategy_dict['strategy']
                        confidence = strategy_dict.get('confidence', 0)
                        print(f"[✓] OPORTUNIDADE ENCONTRADA: {strategy} (Confidence: {confidence:.1f}%)")
                        await send_tip(bot, event, strategy_dict, home_stats, away_stats)
                    else:
                        # Formato antigo (12min/6min ainda não atualizadas)
                        print(f"[✓] OPORTUNIDADE ENCONTRADA: {strategy_dict}")
                        # Criar dict temporário para compatibilidade
                        temp_dict = {'strategy': strategy_dict, 'confidence': 0}
                        await send_tip(bot, event, temp_dict, home_stats, away_stats)
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

last_league_message_id = None # Initialize global variable

async def send_hourly_summary(bot):
    """Loop para enviar resumo por liga a cada 1 hora"""
    global last_league_message_id
    
    print("[INFO] Iniciando enviador de resumo horário por liga...")
    
    while True:
        try:
            # Esperar até o próximo início de hora
            now = datetime.now(MANAUS_TZ)
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            wait_seconds = (next_hour - now).total_seconds()
            
            print(f"[INFO] Resumo horário agendado para {next_hour.strftime('%H:%M:%S')} (Aguardando {wait_seconds:.0f}s)")
            await asyncio.sleep(wait_seconds)
            
            # Gerar resumo
            today = datetime.now(MANAUS_TZ).date()
            print(f"[DEBUG] Executando resumo horário. Tips em memória: {len(sent_tips)}")
            league_stats_summary = defaultdict(lambda: {'green': 0, 'red': 0, 'total': 0})
            
            for tip in sent_tips:
                if tip['sent_time'].date() != today:
                    continue
                
                league = tip.get('league', 'OUTROS')
                status = tip.get('status')
                
                if status in ['green', 'red']:
                    league_stats_summary[league][status] += 1
                    league_stats_summary[league]['total'] += 1
            
            if not league_stats_summary:
                print("[INFO] Nenhum resultado para o resumo horário")
                continue
                
            msg = "📊 <b>RESUMO POR LIGA (HOJE)</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
            
            has_data = False
            for league, stats in sorted(league_stats_summary.items()):
                total = stats['total']
                if total == 0: continue
                
                has_data = True
                greens = stats['green']
                reds = stats['red']
                perc = (greens / total) * 100
                
                msg += f"🏆 <b>LIGA: {league}</b>\n"
                msg += f"💠 TOTAL: {total} TIPS\n"
                msg += f"✅ GREEN: {greens} ({perc:.0f}%)\n"
                msg += f"❌ RED: {reds}\n\n"
            
            if not has_data:
                continue
                
            msg += "━━━━━━━━━━━━━━━━━━━━"
            
            # DELETAR mensagem anterior antes de enviar nova
            if last_league_message_id:
                try:
                    await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
                    print("[✓] Mensagem anterior do resumo horário deletada")
                except Exception as e:
                    print(f"[WARN] Não foi possível deletar mensagem anterior: {e}")
            
            # Enviar nova mensagem
            sent_message = await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
            last_league_message_id = sent_message.message_id
            print(f"[✓] Resumo horário por liga enviado (message_id: {last_league_message_id})")
            
        except Exception as e:
            print(f"[ERROR] send_hourly_summary: {e}")
            await asyncio.sleep(60)

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
        results_checker(bot),
        send_hourly_summary(bot)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Bot encerrado pelo usuário")
    except Exception as e:
        print(f"[ERRO FATAL] {e}")