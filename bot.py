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
LIVE_API_URL = "https://rwtips.dpdns.org/api/app3/live-events"
RECENT_MATCHES_URL = "https://rwtips-r943.onrender.com/api/rw-matches"
PLAYER_STATS_URL = "https://app3.caveiratips.com.br/app3/api/confronto/"
H2H_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/confronto/{player1}/{player2}?page=1&limit=20"

AUTH_HEADER = "Bearer 444c7677f71663b246a40600ff53a8880240086750fda243735e849cdeba9702"

MANAUS_TZ = timezone(timedelta(hours=-4))

# League Name Mappings
# Live API format → Internal format
LIVE_LEAGUE_MAPPING = {
    "E-Soccer - Battle - 8 minutos de jogo": "BATTLE 8 MIN",
    "Esoccer Battle - 8 mins play": "BATTLE 8 MIN",
    "E-Soccer - H2H GG League - 8 minutos de jogo": "H2H 8 MIN",
    "Esoccer H2H GG League - 8 mins play": "H2H 8 MIN",
    "E-Soccer - GT Leagues - 12 minutos de jogo": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues - 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues – 12 mins play": "GT LEAGUE 12 MIN",
    "E-Soccer - Battle Volta - 6 minutos de jogo": "VOLTA 6 MIN",
    "Esoccer Battle Volta - 6 mins play": "VOLTA 6 MIN",
}

# History API format → Internal format
HISTORY_LEAGUE_MAPPING = {
    "Battle 6m": "VOLTA 6 MIN",
    "Battle 8m": "BATTLE 8 MIN",
    "H2H 8m": "H2H 8 MIN",
    "GT Leagues 12m": "GT LEAGUE 12 MIN",
    "GT League 12m": "GT LEAGUE 12 MIN",
}

# =============================================================================
# CACHE E ESTADO GLOBAL
# =============================================================================
player_stats_cache = {}  # {player_name: {stats, timestamp}}
CACHE_TTL = 300  # 5 minutos

# Cache global de histórico de partidas (compartilhado entre todos os jogadores)
global_history_cache = {
    'matches': [],
    'timestamp': 0
}
HISTORY_CACHE_TTL = 60  # 1 minuto

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
            
            # Normalizar dados da API ao vivo
            normalized_events = []
            for event in events:
                # Mapear nome da liga
                league_name = event.get('leagueName', '')
                mapped_league = LIVE_LEAGUE_MAPPING.get(league_name, league_name)
                
                # Criar evento normalizado mantendo compatibilidade
                normalized_event = event.copy()
                normalized_event['leagueName'] = league_name  # Original para display
                normalized_event['mappedLeague'] = mapped_league  # Mapeado para lógica
                normalized_events.append(normalized_event)
            
            print(f"[INFO] {len(normalized_events)} partidas ao vivo encontradas")
            return normalized_events
        except requests.exceptions.Timeout:
            print(f"[WARN] Timeout ao buscar partidas ao vivo (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"[ERROR] fetch_live_matches: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return []

def fetch_recent_matches(page=1, page_size=500, use_cache=True):
    """Busca partidas recentes finalizadas - Nova API com cache global"""
    global global_history_cache
    
    # Verificar cache global
    if use_cache and global_history_cache['matches']:
        cache_age = time.time() - global_history_cache['timestamp']
        if cache_age < HISTORY_CACHE_TTL:
            print(f"[CACHE] Usando histórico do cache global ({len(global_history_cache['matches'])} partidas)")
            return global_history_cache['matches']
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Nova API retorna array direto, sem paginação
            params = {'limit': page_size}
            
            response = requests.get(RECENT_MATCHES_URL, params=params, timeout=15)
            response.raise_for_status()
            raw_matches = response.json()
            
            # A resposta é um array direto, não um objeto com 'partidas'
            if not isinstance(raw_matches, list):
                print(f"[ERROR] Resposta inesperada da API: {type(raw_matches)}")
                return []
            
            normalized_matches = []
            
            for match in raw_matches:
                # Mapear nome da liga
                league_raw = match.get('league', '')
                league_mapped = HISTORY_LEAGUE_MAPPING.get(league_raw, league_raw)
                
                normalized_matches.append({
                    'id': match.get('id'),
                    'league_name': league_mapped,  # Usar nome mapeado
                    'home_player': match.get('homeTeam'),  # homeTeam é o jogador
                    'away_player': match.get('awayTeam'),  # awayTeam é o jogador
                    'home_team': match.get('homeClub'),    # homeClub é o time
                    'away_team': match.get('awayClub'),    # awayClub é o time
                    'data_realizacao': match.get('matchTime'),
                    'home_score_ht': match.get('homeHT'),
                    'away_score_ht': match.get('awayHT'),
                    'home_score_ft': match.get('homeFT'),
                    'away_score_ft': match.get('awayFT')
                })
            
            # Atualizar cache global
            global_history_cache['matches'] = normalized_matches
            global_history_cache['timestamp'] = time.time()
            
            print(f"[INFO] {len(normalized_matches)} partidas recentes carregadas e cacheadas")
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
    """Busca estatísticas individuais de um jogador - Usa cache global de histórico"""
    
    if use_cache and player_name in player_stats_cache:
        cached = player_stats_cache[player_name]
        if time.time() - cached['timestamp'] < CACHE_TTL:
            print(f"[CACHE] Stats de {player_name} do cache")
            return cached['stats']
    
    # Buscar histórico global (usa cache se disponível)
    all_matches = fetch_recent_matches(page_size=500, use_cache=True)
    
    if not all_matches:
        print(f"[WARN] Nenhum histórico disponível para filtrar {player_name}")
        return None
    
    # Filtrar jogos do jogador específico
    player_matches = []
    for match in all_matches:
        home_player = match.get('home_player', '')
        away_player = match.get('away_player', '')
        
        # Verificar se o jogador participou da partida
        if (home_player.upper() == player_name.upper() or 
            away_player.upper() == player_name.upper()):
            player_matches.append(match)
    
    # Limitar aos últimos 20 jogos do jogador
    player_matches = player_matches[:20]
    
    final_data = {
        'matches': player_matches,
        'total_count': len(player_matches)
    }
    
    player_stats_cache[player_name] = {
        'stats': final_data,
        'timestamp': time.time()
    }
    
    print(f"[INFO] Stats de {player_name} carregadas ({final_data['total_count']} jogos)")
    return final_data

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

def detect_regime_change(matches):
    """
    Detecta mudança de estado do jogador (hot→cold ou cold→hot)
    Previne situações como: 2 semanas over → 15 reds seguidos
    """
    if len(matches) < 8:
        return {'regime_change': False}
    
    # Últimos 3 jogos (MOMENTO ATUAL)
    last_3 = matches[:3]
    
    # Jogos 4-10 (HISTÓRICO RECENTE)
    previous_7 = matches[3:10] if len(matches) >= 10 else matches[3:]
    
    def avg_goals_window(window, player_name=None):
        total = 0
        for m in window:
            # Determinar se é home ou away
            home_player = m.get('home_player', '')
            if player_name:
                is_home = home_player.upper() == player_name.upper()
            else:
                # Fallback: assumir que estamos analisando o primeiro jogador
                is_home = True
            
            goals = m.get('home_score_ft', 0) if is_home else m.get('away_score_ft', 0)
            total += goals or 0
        return total / len(window) if window else 0
    
    avg_last_3 = avg_goals_window(last_3)
    avg_previous = avg_goals_window(previous_7)
    
    # DETECÇÃO DE MUDANÇA DE ESTADO
    if avg_previous > 0:
        ratio = avg_last_3 / avg_previous
        
        # COOLING (jogador esfriou) - BLOQUEIO CRÍTICO
        # Ajustado para ser mais sensível e detectar cooling mais cedo
        if ratio < 0.6 and avg_last_3 < 2.0:
            return {
                'regime_change': True,
                'direction': 'COOLING',
                'severity': 'HIGH',
                'avg_last_3': avg_last_3,
                'avg_previous': avg_previous,
                'action': 'AVOID',
                'reason': f'Jogador esfriou drasticamente: {avg_last_3:.1f} vs {avg_previous:.1f} anterior'
            }
        
        # HEATING (jogador esquentou) - BOOST
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

def analyze_player_with_regime_check(matches, player_name):
    """
    Análise de jogador COM detecção de regime change e cálculo de confidence
    Retorna None se jogador esfriou (para bloquear tips)
    """
    if not matches:
        print(f"[WARN] {player_name}: Nenhum jogo encontrado")
        return None
    
    # Mínimo de 5 jogos
    if len(matches) < 5:
        print(f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: 5)")
        return None
    
    # 1. DETECTAR REGIME CHANGE (crítico!)
    regime = detect_regime_change(matches)
    
    if regime['regime_change'] and regime['action'] == 'AVOID':
        print(f"[ALERT] {player_name}: REGIME CHANGE DETECTADO - {regime['reason']}")
        print(f"[ALERT] Bloqueando análise para evitar tips perigosas")
        return None  # VETO! Não analisa jogador que esfriou
    
    # 2. Análise normal dos últimos 5 jogos
    stats = analyze_last_5_games(matches, player_name)
    
    if not stats:
        return None
    
    # 3. CALCULAR CONFIDENCE SCORE (0-100)
    confidence = calculate_confidence_score(matches[:5], player_name, stats, regime)
    stats['confidence'] = confidence
    
    # 4. Adicionar informações de regime
    stats['regime_change'] = regime['regime_change']
    stats['regime_direction'] = regime.get('direction', 'STABLE')
    
    if regime['regime_change'] and regime['action'] == 'BOOST':
        print(f"[INFO] {player_name} em HOT STREAK: {regime['reason']} | Confidence: {confidence}%")
    else:
        print(f"[INFO] {player_name} Confidence: {confidence}%")
    
    return stats

def calculate_confidence_score(last_5_matches, player_name, stats, regime):
    """
    Calcula score de confidence (0-100) baseado em múltiplos fatores
    """
    score = 0
    
    # FATOR 1: Consistência (40 pontos)
    # Quanto mais consistente, maior o score
    avg_goals = stats['avg_goals_scored_ft']
    
    # Calcular desvio padrão dos gols marcados
    goals_list = []
    for match in last_5_matches:
        is_home = match.get('home_player', '').upper() == player_name.upper()
        goals = match.get('home_score_ft', 0) if is_home else match.get('away_score_ft', 0)
        goals_list.append(goals or 0)
    
    import statistics
    if len(goals_list) >= 2:
        std_dev = statistics.stdev(goals_list)
        # Baixa volatilidade = alta consistência
        if std_dev <= 0.5:
            score += 40  # Muito consistente
        elif std_dev <= 1.0:
            score += 30  # Consistente
        elif std_dev <= 1.5:
            score += 20  # Moderado
        elif std_dev <= 2.0:
            score += 10  # Inconsistente
        # std_dev > 2.0 = 0 pontos (muito volátil)
    
    # FATOR 2: Média de Gols (30 pontos)
    if avg_goals >= 3.5:
        score += 30  # Excelente
    elif avg_goals >= 3.0:
        score += 25  # Muito bom
    elif avg_goals >= 2.5:
        score += 20  # Bom
    elif avg_goals >= 2.0:
        score += 15  # Razoável
    elif avg_goals >= 1.5:
        score += 10  # Fraco
    # < 1.5 = 0 pontos
    
    # FATOR 3: Tendência/Regime (20 pontos)
    if regime['regime_change']:
        if regime['action'] == 'BOOST':
            score += 20  # Jogador esquentando
        elif regime['action'] == 'AVOID':
            score += 0   # Jogador esfriando (já bloqueado antes)
    else:
        score += 10  # Estável (neutro)
    
    # FATOR 4: Consistência em HT (10 pontos)
    # Jogadores que marcam consistentemente no HT são mais confiáveis
    if stats['ht_over_05_pct'] >= 100:
        score += 10
    elif stats['ht_over_05_pct'] >= 80:
        score += 7
    elif stats['ht_over_05_pct'] >= 60:
        score += 5
    elif stats['ht_over_05_pct'] >= 40:
        score += 3
    
    # Garantir que está entre 0-100
    score = max(0, min(100, score))
    
    return score

# =============================================================================
# LÓGICA DE ESTRATÉGIAS
# =============================================================================

def check_strategies_8mins(event, home_stats, away_stats, all_league_stats):
    """Estratégias para ligas de 8 minutos"""
    strategies = []
    
    # Usar o nome mapeado da liga
    league_key = event.get('mappedLeague', '')
    
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
    
    # Usar o nome mapeado da liga
    league_key = event.get('mappedLeague', '')

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
    
    # Usar o nome mapeado da liga
    league_key = event.get('mappedLeague', '')

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
    
    # Calcular confidence médio
    home_confidence = home_stats_summary.get('confidence', 0)
    away_confidence = away_stats_summary.get('confidence', 0)
    avg_confidence = (home_confidence + away_confidence) / 2
    
    # Emoji de confidence
    if avg_confidence >= 90:
        confidence_emoji = "🔥🔥🔥"
    elif avg_confidence >= 80:
        confidence_emoji = "🔥🔥"
    elif avg_confidence >= 70:
        confidence_emoji = "🔥"
    else:
        confidence_emoji = "❄️"
    
    # Regime status
    home_regime = home_stats_summary.get('regime_direction', 'STABLE')
    away_regime = away_stats_summary.get('regime_direction', 'STABLE')
    
    if home_regime == 'HEATING' or away_regime == 'HEATING':
        regime_status = "🔥 HEATING"
    else:
        regime_status = "❄️ STABLE"
    
    # Cabeçalho com destaque
    msg = "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🎯 <b>OPORTUNIDADE DETECTADA</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Confidence e Regime
    msg += f"{confidence_emoji} <b>Confidence: {avg_confidence:.0f}%</b> | {regime_status}\n\n"
    
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
        
        msg += f"🏠 <b>{home_player}</b> (Conf: {home_confidence:.0f}%)\n"
        msg += f"├ HT: +0.5 ({home_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({home_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {home_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo\n"
        msg += f"└ Gols +3: {home_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"
        
        msg += f"✈️ <b>{away_player}</b> (Conf: {away_confidence:.0f}%)\n"
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
        
        for match in recent_matches[:200]:
            # Os dados já vêm normalizados com league_name mapeado
            league = match.get('league_name', '')
            
            if not league or league == 'Unknown': 
                continue
            
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
                
                if len(home_matches) < 5 or len(away_matches) < 5:
                    print(f"[WARN] Dados insuficientes: {home_player}={len(home_matches)} jogos, {away_player}={len(away_matches)} jogos (mínimo: 5)")
                    continue
                
                # Análise COM detecção de regime change
                home_stats = analyze_player_with_regime_check(home_matches, home_player)
                away_stats = analyze_player_with_regime_check(away_matches, away_player)
                
                if not home_stats or not away_stats:
                    print(f"[WARN] Falha na análise das estatísticas (possível regime change detectado)")
                    continue
                
                # FILTRO DE CONFIDENCE MÍNIMO (80%)
                home_confidence = home_stats.get('confidence', 0)
                away_confidence = away_stats.get('confidence', 0)
                avg_confidence = (home_confidence + away_confidence) / 2
                
                if home_confidence < 80 or away_confidence < 80:
                    print(f"[BLOCKED] Confidence insuficiente: {home_player}={home_confidence:.0f}%, {away_player}={away_confidence:.0f}% (mínimo: 80%)")
                    continue
                
                print(f"[STATS] {home_player} (últimos 5 jogos): HT O0.5={home_stats['ht_over_05_pct']:.0f}% O1.5={home_stats['ht_over_15_pct']:.0f}% O2.5={home_stats['ht_over_25_pct']:.0f}% | Confidence: {home_confidence:.0f}%")
                print(f"[STATS] {away_player} (últimos 5 jogos): HT O0.5={away_stats['ht_over_05_pct']:.0f}% O1.5={away_stats['ht_over_15_pct']:.0f}% O2.5={away_stats['ht_over_25_pct']:.0f}% | Confidence: {away_confidence:.0f}%")
                
                strategies = []
                
                # Usar o nome mapeado da liga para selecionar estratégias
                mapped_league = event.get('mappedLeague', '')
                
                if mapped_league in ['BATTLE 8 MIN', 'H2H 8 MIN']:
                    strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats)
                
                elif mapped_league == 'GT LEAGUE 12 MIN':
                    strategies = check_strategies_12mins(event, home_stats, away_stats, league_stats)
                
                elif mapped_league == 'VOLTA 6 MIN':
                    strategies = check_strategies_volta_6mins(event, home_stats, away_stats, league_stats)
                
                for strategy in strategies:
                    print(f"[✓] OPORTUNIDADE ENCONTRADA: {strategy} | Confidence Médio: {avg_confidence:.0f}%")
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