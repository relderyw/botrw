import os
import time
import requests
import asyncio
import concurrent.futures
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Bot
from telegram.request import HTTPXRequest
import re
import logging
from PIL import Image, ImageDraw, ImageFont
import statistics
from io import BytesIO
import json


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
# Altenar Live API (same as frontend) - includes auth parameters
LIVE_API_URL = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66&catIds=2085,1571,1728,1594,2086,1729,2130"
# Internal History API
HISTORY_API_URL = "https://rwtips-r943.onrender.com/api/app3/history"
# Green365 API (for H2H GG and consistent history)
GREEN365_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"
GREEN365_TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImY1MzMwMzNhMTMzYWQyM2EyYzlhZGNmYzE4YzRlM2E3MWFmYWY2MjkiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vZXVncmVlbi0yZTljMCIsImF1ZCI6ImV1Z3JlZW4tMmU5YzAiLCJhdXRoX3RpbWUiOjE3NjM4NzY2NTcsInVzZXJfaWQiOiJwM09CaFI3Wmd3VENwNnFBWFpFZWl0RGt4T0czIiwic3ViIjoicDNPQmhSN1pnd1RDcDZxQVhaRWVpdERreE9HMyIsImlhdCI6MTc3MTI0ODI2NiwiZXhwIjoxNzcxMjUxODI2LCJlbWFpbCI6InJlbGRlcnkxNDIyQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJmaXJlYmFzZSI6eyJpZGVudGl0aWVzIjp7ImVtYWlsIjpbInJlbGRlcnkxNDIyQGdtYWlsLmNvbSJdfSwic2lnbl9pbl9wcm92aWRlciI6InBhc3N3b3JkIn19.yHo_igBbWbi8PUrKpUFpH9yB7mf4E3gW2eg1tVnHDJ7isFiI66Vyde2oCttLXlYLtYZMoU_Epl1Lu_OBAfoaa3IoBO359Cb5cf1gFd-E9wS8pBiZB-QVh0xMHmf29va0CURg3zvlwnpE-MChlmVj2zNzlAhj818VMnsTB3DKPzqIa-n-WklIUYbAWkwVj6qpjAOCWgPUs22mas_-mSbjV6og5OvA-6yKWELWDzAqtjnm0Vpcg92V-YOZ96ymFVqB4t5DlLQmrS53byAYa_uwNRtKB8NdzVVJlm5hjjpfUWYNDnIbZRchroIcpk081R5fqfS6WJ0vDbrCh_E2XCTGgA"

# Legacy URLs (kept for reference/fallback)
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
    "H2H GG LEAGUE - E-FOOTBALL": "H2H 8 MIN",  # NOVO
    "H2H GG LEAGUE": "H2H 8 MIN",  # NOVO
    "E-Soccer - GT Leagues - 12 minutos de jogo": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues - 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues – 12 mins play": "GT LEAGUE 12 MIN",
    "E-Soccer - Battle Volta - 6 minutos de jogo": "VOLTA 6 MIN",
    "Esoccer Battle Volta - 6 mins play": "VOLTA 6 MIN",
    "H2H GG - E-football": "H2H 8 MIN",
    "H2H GG": "H2H 8 MIN",
    # New Altenar leagues
    "Valhalla Cup": "VALHALLA CUP",
    "Valhalla League": "VALHALLA CUP",
    "Valkyrie Cup": "VALKYRIE CUP",
    "CLA": "CLA 10 MIN",
    "Cyber Live Arena": "CLA 10 MIN",
    "Champions League B 2×6": "GT LEAGUE 12 MIN",
    "Champions League B 2x6": "GT LEAGUE 12 MIN",
    "Champions League": "GT LEAGUE 12 MIN",
    "Super Lig": "GT LEAGUE 12 MIN",
    "Super Lig • E-battles": "GT LEAGUE 12 MIN",
    "ESportsBattle. RSL (2x6 mins)": "GT LEAGUE 12 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)": "VOLTA 6 MIN",
    "International": "INT 8 MIN",
    "Europa League": "GT LEAGUE 12 MIN",
    "Bundesliga": "GT LEAGUE 12 MIN",
    "Champions Cyber League": "CLA 10 MIN",
    "Cyber League": "CLA 10 MIN",
    "ESportsBattle. Premier League (2x4 mins)": "BATTLE 8 MIN",
    "Premier League": "GT LEAGUE 12 MIN",
}

# History API / Green365 format → Internal format
HISTORY_LEAGUE_MAPPING = {
    "Battle 6m": "VOLTA 6 MIN",
    "Battle 8m": "BATTLE 8 MIN",
    "H2H 8m": "H2H 8 MIN",
    "GT Leagues 12m": "GT LEAGUE 12 MIN",
    "GT League 12m": "GT LEAGUE 12 MIN",
    # Green365 specific names
    "Esoccer Battle - 8 mins play": "BATTLE 8 MIN",
    "Esoccer Battle Volta - 6 mins play": "VOLTA 6 MIN",
    "Esoccer GT Leagues – 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer H2H GG League - 8 mins play": "H2H 8 MIN",
    # New leagues from internal API
    "Valhalla Cup": "VALHALLA CUP",
    "Valkyrie Cup": "VALKYRIE CUP",
    "CLA League": "CLA 10 MIN",
    "CLA": "CLA 10 MIN",
    "Champions League": "GT LEAGUE 12 MIN",
    "Super Lig": "GT LEAGUE 12 MIN",
    "Champions League B 2x6": "GT LEAGUE 12 MIN",
    "Champions League B 2×6": "GT LEAGUE 12 MIN",
    "ESportsBattle. RSL (2x6 mins)": "GT LEAGUE 12 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)": "VOLTA 6 MIN",
    "ESportsBattle. Premier League (2x4 mins)": "BATTLE 8 MIN",
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
last_league_update_time = 0  # Timestamp do último envio do resumo das ligas


def map_league_name(name):
    """Mapeia o nome da liga de forma consistente (por prefixo)"""
    if not name: return "Unknown"
    
    # Tentar match exato primeiro
    if name in LIVE_LEAGUE_MAPPING:
        return LIVE_LEAGUE_MAPPING[name]
    
    # Tentar match por prefixo
    for key, value in LIVE_LEAGUE_MAPPING.items():
        if name.upper().startswith(key.upper()):
            return value
            
    return name

def save_state():
    """Salva o estado do bot em um arquivo JSON"""
    try:
        state = {
            'last_league_message_id': last_league_message_id,
            'league_stats': league_stats,
            'last_league_update_time': last_league_update_time,
            'last_summary': last_summary,
            'sent_match_ids': list(sent_match_ids),
            'sent_tips': [
                {**tip, 'sent_time': tip['sent_time'].isoformat()}
                for tip in sent_tips
            ]
        }
        with open('bot_state.json', 'w') as f:
            json.dump(state, f, indent=4)
        print("[DEBUG] Estado salvo com sucesso")
    except Exception as e:
        print(f"[ERROR] save_state: {e}")


def load_state():
    """Carrega o estado do bot de um arquivo JSON"""
    global last_league_message_id, league_stats, last_league_update_time, last_summary, sent_match_ids, sent_tips
    try:
        if os.path.exists('bot_state.json'):
            with open('bot_state.json', 'r') as f:
                state = json.load(f)
                last_league_message_id = state.get('last_league_message_id')
                league_stats = state.get('league_stats', {})
                last_league_update_time = state.get('last_league_update_time', 0)
                last_summary = state.get('last_summary')
                sent_match_ids = set(state.get('sent_match_ids', []))
                
                # Carregar sent_tips e converter datas de volta
                raw_tips = state.get('sent_tips', [])
                sent_tips = []
                for tip in raw_tips:
                    try:
                        tip['sent_time'] = datetime.fromisoformat(tip['sent_time'])
                        sent_tips.append(tip)
                    except:
                        continue
            print(f"[DEBUG] Estado carregado com sucesso ({len(sent_tips)} tips pendentes)")
    except Exception as e:
        print(f"[ERROR] load_state: {e}")

# =============================================================================
# UTILITÁRIOS
# =============================================================================


def clean_player_name(name):
    """Extrai nome do jogador de forma inteligente (sem lista estática de times)"""
    if not name or not isinstance(name, str):
        return ""
    
    # Se já parece um nome de jogador (uma palavra curta em maiúsculas ou sem espaços), não mexe
    if name.isupper() and ' ' not in name and 2 <= len(name) <= 15:
        return name

    match = re.search(r'\((.*?)\)', name)
    if not match:
        # Se não tem parênteses, tenta split por " - ", " vs " ou " / "
        for sep in [' - ', ' vs ', ' / ']:
            if sep in name:
                parts = name.split(sep)
                p1, p2 = parts[0].strip(), parts[1].strip()
                if ' ' in p1 and ' ' not in p2: return p2
                if ' ' in p2 and ' ' not in p1: return p1
                return p2 # Fallback
        return name.strip()

    content_match = match.group(1).strip()
    outside = name.replace(f"({content_match})", "").strip()
    
    # Se o conteúdo dos parênteses parecer informação de liga/tempo, ignora
    league_words = ['MINS', 'PLAY', 'LEAGUE', 'GT', 'BATTLE', 'VOLTA', 'ESOCCER', 'INTERNATIONAL', 'PART']
    if any(word in content_match.upper() for word in league_words):
        return outside.strip()

    def get_player_score(s):
        """Atribui uma pontuação de 'probabilidade de ser jogador'"""
        score = 0
        if not s: return -50
        
        # Jogadores costumam ser uma única palavra (sem espaços)
        if ' ' not in s: score += 20
        # Jogadores costumam ser ALL CAPS (ex: ALIBI) ou CamelCase com underscores
        if s.isupper() and len(s) > 1: score += 25
        # Nicks com underscore costumam ser jogadores (ex: Jur_Kr, xoma_1)
        if '_' in s: score += 35
        # Jogadores costumam ter números (ex: D3VA)
        if any(char.isdigit() for char in s): 
            # Mas cuidado com "04", "09" no final (comum em times alemães)
            if re.search(r'\s(04|09|II|III)$', s) or s.endswith('04') or s.endswith('09'):
                score -= 15
            else:
                score += 20
        
        # Nomes de times costumam ser mais longos
        score -= len(s) * 1.0
        
        return score

    score_content = get_player_score(content_match)
    score_outside = get_player_score(outside)

    # Se estiver nos parênteses e for razoável, prioriza (quase sempre é o nick na Altenar)
    if score_content > score_outside - 10:
        return content_match
    return outside



# =============================================================================
# FUNÇÕES DE REQUISIÇÃO
# =============================================================================


def fetch_live_matches():
    """Busca partidas ao vivo da Altenar API"""

    try:
        print(f"[INFO] Buscando partidas ao vivo da Altenar API...")
        response = requests.get(LIVE_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        events = data.get('events', [])
        competitors_list = data.get('competitors', [])
        champs_list = data.get('champs', [])

        # Criar mapa completo de competidores para acessar campos extras se existirem
        competitors_map = {c['id']: c for c in competitors_list}
        champs_map = {c['id']: c['name'] for c in champs_list}

        print(
            f"[DEBUG] Altenar API: {len(events)} eventos, {len(competitors_map)} competidores, {len(champs_map)} campeonatos")

        # Filtrar apenas futebol (sportId 66)
        football_events = [e for e in events if e.get('sportId') == 66]
        print(f"[DEBUG] {len(football_events)} eventos de futebol após filtro")

        def parse_live_time(live_time_str, start_date_str=None):
            """Versão robusta - prioriza liveTime da Altenar"""
            if live_time_str:
                # Tenta extrair "02:45" se existir no texto
                m = re.search(r'(\d{1,2}):(\d{2})', live_time_str)
                if m:
                    return int(m.group(1)), int(m.group(2))
                
                lt = live_time_str.upper()
                if any(x in lt for x in ['1ª PARTE', '1ST HALF', '1°', 'PRIMEIRO TEMPO']):
                    return 2, 30
                if any(x in lt for x in ['INTERVALO', 'HALFTIME', 'MEIO TEMPO']):
                    return 4, 0
                if any(x in lt for x in ['2ª PARTE', '2ND HALF', '2°', 'SEGUNDO TEMPO']):
                    return 7, 0
                if any(x in lt for x in ['FINAL', 'ENDED', 'FIM', 'TERMINADO']):
                    return 9, 0
            
            # Só usa startDate se liveTime estiver vazio
            if start_date_str:
                try:
                    start_dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                    elapsed = (datetime.now(timezone.utc) - start_dt).total_seconds()
                    if elapsed < 0:
                        return 0, 0
                    return int(elapsed // 60), int(elapsed % 60)
                except:
                    pass
            return 0, 0

        normalized_events = []

        for event in football_events:
            try:
                event_id = event.get('id')
                champ_id = event.get('champId')
                competitor_ids = event.get('competitorIds', [])
                score_raw = event.get('score', [0, 0])
                live_time = event.get('liveTime', '')

                # Parse robusto do score — Altenar retorna lista [home, away]
                # mas pode variar: None, string "1:0", dict {home:1, away:0}
                def parse_score_value(s):
                    if s is None:
                        return [0, 0]
                    if isinstance(s, list):
                        h = int(s[0]) if len(s) > 0 else 0
                        a = int(s[1]) if len(s) > 1 else 0
                        return [h, a]
                    if isinstance(s, dict):
                        return [int(s.get('home', 0)), int(s.get('away', 0))]
                    if isinstance(s, str):
                        for sep in [':', '-', ' ']:
                            if sep in s:
                                parts = s.split(sep)
                                try:
                                    return [int(parts[0].strip()), int(parts[1].strip())]
                                except:
                                    pass
                    return [0, 0]

                score = parse_score_value(score_raw)

                # Obter nomes dos competidores
                home_comp = competitors_map.get(competitor_ids[0], {}) if len(competitor_ids) > 0 else {}
                away_comp = competitors_map.get(competitor_ids[1], {}) if len(competitor_ids) > 1 else {}
                
                home_competitor_name = home_comp.get('name', '')
                away_competitor_name = away_comp.get('name', '')

                if not home_competitor_name or not away_competitor_name:
                    print(f"[WARN] Evento {event_id} sem nomes de competidores")
                    continue

                # Priorizar extração dos parênteses se existirem (mais confiável nesta API)
                name_from_full_home = clean_player_name(home_competitor_name)
                check_home_player = (home_comp.get('playerName') or 
                                    home_comp.get('player_name') or 
                                    home_comp.get('player') or 
                                    home_comp.get('nickName') or 
                                    event.get('home_player'))
                
                # Armazenar o texto bruto original usado para a extração
                home_raw_text = home_competitor_name if '(' in home_competitor_name else (check_home_player or home_competitor_name)

                if '(' in home_competitor_name and name_from_full_home != home_competitor_name:
                    home_player = name_from_full_home
                    home_source = "Full-Name-Parentheses"
                else:
                    home_player = clean_player_name(check_home_player or home_competitor_name)
                    home_source = "API-Field" if check_home_player else "Full-Name-Cleaning"
                               
                name_from_full_away = clean_player_name(away_competitor_name)
                check_away_player = (away_comp.get('playerName') or 
                                    away_comp.get('player_name') or 
                                    away_comp.get('player') or 
                                    away_comp.get('nickName') or 
                                    event.get('away_player'))
                
                away_raw_text = away_competitor_name if '(' in away_competitor_name else (check_away_player or away_competitor_name)

                if '(' in away_competitor_name and name_from_full_away != away_competitor_name:
                    away_player = name_from_full_away
                    away_source = "Full-Name-Parentheses"
                else:
                    away_player = clean_player_name(check_away_player or away_competitor_name)
                    away_source = "API-Field" if check_away_player else "Full-Name-Cleaning"
                
                home_team = (home_comp.get('teamName') or 
                             home_comp.get('team_name') or 
                             home_comp.get('team') or 
                             event.get('home_team') or 
                             (home_competitor_name.split('(')[0].strip() if '(' in home_competitor_name else home_competitor_name))
                             
                away_team = (away_comp.get('teamName') or 
                             away_comp.get('team_name') or 
                             away_comp.get('team') or 
                             event.get('away_team') or 
                             (away_competitor_name.split('(')[0].strip() if '(' in away_competitor_name else away_competitor_name))

                # Obter nome da liga
                league_name = champs_map.get(champ_id, 'Unknown League')

                # FILTRO GLOBAL: Remover ligas virtuais indesejadas (ECOMP, etc)
                if "ECOMP" in league_name.upper() or "VIRTUAL" in league_name.upper():
                    continue

                # Mapear nome da liga (com suporte a prefixos/limpeza)
                mapped_league = map_league_name(league_name)

                start_date = event.get('startDate', '')

                # Parsear tempo real usando startDate
                minute, second = parse_live_time(live_time, start_date_str=start_date)
                print(f"[DEBUG] Evento {event_id} | liveTime='{live_time}' | startDate={start_date} | Minuto calculado: {minute}:{second:02d}")

                normalized_event = {
                    'id': str(event_id),
                    'leagueName': league_name,
                    'mappedLeague': mapped_league,
                    'homePlayer': home_player,
                    'awayPlayer': away_player,
                    'homeSource': home_source,
                    'awaySource': away_source,
                    'homeRaw': home_raw_text,
                    'awayRaw': away_raw_text,
                    'homeTeamName': home_team,
                    'awayTeamName': away_team,
                    'timer': {
                        'minute': minute,
                        'second': second,
                        'formatted': f"{minute:02d}:{second:02d}"
                    },
                    'score': {
                        'home': score[0] if len(score) > 0 else 0,
                        'away': score[1] if len(score) > 1 else 0
                    },
                    'scoreboard': f"{score[0] if len(score) > 0 else 0}-{score[1] if len(score) > 1 else 0}",
                    'liveTimeRaw': live_time,      # NOVO
                    'startDateRaw': start_date,    # NOVO
                }

                normalized_events.append(normalized_event)

            except Exception as e:
                print(
                    f"[WARN] Erro ao processar evento {event.get('id')}: {e}")
                continue

        print(
            f"[INFO] {len(normalized_events)} partidas ao vivo normalizadas (Altenar API)")
        return normalized_events

    except Exception as e:
        print(f"[ERROR] Altenar Live API falhou: {e}")
        return []


def fetch_green365_history(num_pages=5):
    """Busca partidas da API Green365 (especialmente para H2H GG)"""
    try:
        print(f"[INFO] Buscando histórico da Green365 ({num_pages} páginas)...")
        all_matches = []

        headers = {"Authorization": GREEN365_TOKEN}

        for page in range(1, num_pages + 1):
            params = {
                "page": page, 
                "limit": 24,
                "sport": "esoccer",
                "status": "ended"
            }
            response = requests.get(
                GREEN365_API_URL, params=params, headers=headers, timeout=12)
            if response.status_code != 200:
                continue

            data = response.json()
            # Green365 retorna os jogos em 'items'
            items = data.get('items', [])
            if not items:
                break

            for item in items:
                try:
                    home_data = item.get('home', {})
                    away_data = item.get('away', {})
                    score = item.get('score', {})
                    score_ht = item.get('scoreHT', {})
                    competition = item.get('competition', {})

                    match_date = item.get('startTime', '')

                    # Normalizar para o formato interno (Green365 já costuma ter campos separados se bem configurado)
                    # Se não tiver, o fallback clean_player_name é usado
                    normalized_home = clean_player_name(home_data.get('player_name') or 
                                                        home_data.get('playerName') or 
                                                        home_data.get('name', ''))
                    normalized_away = clean_player_name(away_data.get('player_name') or 
                                                        away_data.get('playerName') or 
                                                        away_data.get('name', ''))

                    all_matches.append({
                        'id': f"g365_{item.get('id')}",
                        'league_name': competition.get('name', 'Unknown'),
                        'home_player': normalized_home,
                        'away_player': normalized_away,
                        'home_team': home_data.get('team_name') or home_data.get('teamName') or '',
                        'away_team': away_data.get('team_name') or away_data.get('teamName') or '',
                        'home_team_logo': home_data.get('imageUrl', ''),
                        'away_team_logo': away_data.get('imageUrl', ''),
                        'data_realizacao': match_date,
                        'home_score_ht': score_ht.get('home', 0),
                        'away_score_ht': score_ht.get('away', 0),
                        'home_score_ft': score.get('home', 0),
                        'away_score_ft': score.get('away', 0)
                    })
                except Exception as e:
                    print(f"[WARN] Erro ao normalizar item Green365: {e}")
                    continue

        return all_matches
    except Exception as e:
        print(f"[ERROR] Green365 History API falhou: {e}")
        return []


def fetch_recent_matches(num_pages=10, use_cache=True):
    """Busca partidas recentes finalizadas - Unifica Internal API e Green365"""
    global global_history_cache

    # Verificar cache global
    # Se o cache tiver pelo menos 200 jogos, consideramos "quente" o suficiente
    if use_cache and len(global_history_cache['matches']) > 200:
        cache_age = time.time() - global_history_cache['timestamp']
        if cache_age < HISTORY_CACHE_TTL:
            return global_history_cache['matches']

    # Se pediram poucas páginas, aumentamos para 40 para garantir cobertura de jogadores menos frequentes
    fetch_pages = max(num_pages, 40)
    print(f"[INFO] Atualizando histórico completo (Deep Fetch) - {fetch_pages} páginas...")

    def fetch_internal_page(page):
        try:
            params = {'page': page, 'limit': 24}
            response = requests.get(HISTORY_API_URL, params=params, timeout=10)
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get('results', [])
        except:
            return []

    # 1. Buscar da API Interna em paralelo
    internal_matches = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        pages = range(1, fetch_pages + 1)
        results = list(executor.map(fetch_internal_page, pages))
        for r in results:
            internal_matches.extend(r)

    # 2. Buscar da Green365 (historicamente mais estável para H2H GG)
    green_matches = fetch_green365_history(num_pages=5)

    # 3. Processar e unificar
    all_combined = []

    # Processar Internal
    for m in internal_matches:
        league = m.get('league_name', 'Unknown')
        # Priorizar campos separados da API Interna se existirem
        home_player = clean_player_name(m.get('home_player') or 
                                        m.get('player_home') or 
                                        m.get('home_player_name') or 
                                        m.get('home_player_raw') or 
                                        m.get('home_competitor_name', ''))
        
        away_player = clean_player_name(m.get('away_player') or 
                                        m.get('player_away') or 
                                        m.get('away_player_name') or 
                                        m.get('away_player_raw') or 
                                        m.get('away_competitor_name', ''))
        
        all_combined.append({
            'id': f"int_{m.get('id')}",
            'league_name': map_league_name(league),
            'home_player': home_player,
            'away_player': away_player,
            'home_team': m.get('home_team', ''),
            'away_team': m.get('away_team', ''),
            'home_team_logo': m.get('home_team_logo', ''),
            'away_team_logo': m.get('away_team_logo', ''),
            'data_realizacao': f"{m.get('match_date')}T{m.get('match_time')}" if m.get('match_date') else datetime.now().isoformat(),
            'home_score_ht': m.get('home_score_ht', 0) or 0,
            'away_score_ht': m.get('away_score_ht', 0) or 0,
            'home_score_ft': m.get('home_score_ft', 0) or 0,
            'away_score_ft': m.get('away_score_ft', 0) or 0
        })

    # Processar Green365 (já normalizados mas mapear liga)
    for m in green_matches:
        league = m['league_name']
        m['league_name'] = map_league_name(league)
        # Log para depuração de liga H2H GG
        if "H2H" in m['league_name']:
            print(f"[DEBUG G365] Partida unificada: {m['home_player']} vs {m['away_player']} - Liga: {m['league_name']}")
        all_combined.append(m)

    # 4. Remover duplicatas por jogadores + score + data_truncada (se necessário)
    # Por simplicidade e volume, apenas ordenar por data e confiar na unificação
    all_combined.sort(key=lambda x: x['data_realizacao'], reverse=True)

    # Atualizar cache global
    global_history_cache['matches'] = all_combined
    global_history_cache['timestamp'] = time.time()

    print(f"[INFO] Histórico atualizado: {len(all_combined)} partidas unificadas.")
    return all_combined


def fetch_player_individual_stats(player_name, use_cache=True):
    """Busca estatísticas individuais de um jogador - Usa cache global de histórico"""

    name_key = player_name.upper().strip()

    if use_cache and name_key in player_stats_cache:
        cached = player_stats_cache[name_key]
        if time.time() - cached['timestamp'] < CACHE_TTL:
            return cached['stats']

    # Buscar histórico global (usa cache se disponível)
    # 500 jogos ~= 21 páginas de 24 jogos
    all_matches = fetch_recent_matches(num_pages=15, use_cache=True)

    if not all_matches:
        print(f"[WARN] Nenhum histórico disponível para filtrar {player_name}")
        return None

    # Filtrar jogos do jogador específico
    player_matches = []
    for match in all_matches:
        home_player = match.get('home_player', '')
        away_player = match.get('away_player', '')

        # Verificar se o jogador participou da partida (Case Insensitive)
        if (home_player.upper().strip() == name_key or
                away_player.upper().strip() == name_key):
            player_matches.append(match)

    # Limitar aos últimos 20 jogos do jogador
    player_matches = player_matches[:20]

    final_data = {
        'matches': player_matches,
        'total_count': len(player_matches)
    }

    player_stats_cache[name_key] = {
        'stats': final_data,
        'timestamp': time.time()
    }

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
            print(
                f"[INFO] H2H {player1} vs {player2}: {data.get('total_matches', 0)} jogos")
            return data
        except requests.exceptions.Timeout:
            print(
                f"[WARN] Timeout ao buscar H2H (tentativa {attempt + 1}/{max_retries})")
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

    if len(matches) < 4:
        print(
            f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: 4)")
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

        if player_ft_goals >= 3:
            games_scored_3_plus += 1
        if ft_home > 0 and ft_away > 0:
            btts_count += 1
        if ht_home > 0 and ht_away > 0:
            ht_btts_count += 1

        # HT Overs
        if ht_total > 0:
            ht_over_05 += 1
        if ht_total > 1:
            ht_over_15 += 1
        if ht_total > 2:
            ht_over_25 += 1
        if ht_total > 3:
            ht_over_35 += 1

        # HT Individual
        if player_ht_goals > 0:
            ht_scored_05 += 1
        if player_ht_goals > 1:
            ht_scored_15 += 1
        if player_ht_goals > 2:
            ht_scored_25 += 1
        if player_ht_conceded > 1:
            ht_conceded_15 += 1

        # FT Overs
        if ft_total > 0:
            ft_over_05 += 1
        if ft_total > 1:
            ft_over_15 += 1
        if ft_total > 2:
            ft_over_25 += 1
        if ft_total > 3:
            ft_over_35 += 1
        if ft_total > 4:
            ft_over_45 += 1

        # FT Individual
        if player_ft_goals > 0:
            ft_scored_05 += 1
        if player_ft_goals > 1:
            ft_scored_15 += 1
        if player_ft_goals > 2:
            ft_scored_25 += 1
        if player_ft_goals > 3:
            ft_scored_35 += 1

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
    if len(matches) < 6:
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

            goals = m.get('home_score_ft', 0) if is_home else m.get(
                'away_score_ft', 0)
            total += goals or 0
        return total / len(window) if window else 0

    avg_last_3 = avg_goals_window(last_3)
    avg_previous = avg_goals_window(previous_7)

    # DETECÇÃO DE MUDANÇA DE ESTADO
    if avg_previous > 0:
        ratio = avg_last_3 / avg_previous

        # COOLING (jogador esfriou) - BLOQUEIO CRÍTICO
        # Ajustado para ser menos sensível e permitir mais tips (Conforme pedido do Usuário)
        if ratio < 0.45 and avg_last_3 < 1.2:
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

    # Mínimo de 4 jogos
    if len(matches) < 4:
        print(
            f"[WARN] {player_name}: Apenas {len(matches)} jogos encontrados (mínimo: 4)")
        return None

    # 1. DETECTAR REGIME CHANGE (crítico!)
    regime = detect_regime_change(matches)

    if regime['regime_change'] and regime['action'] == 'AVOID':
        print(
            f"[ALERT] {player_name}: REGIME CHANGE DETECTADO - {regime['reason']}")
        print(f"[ALERT] Bloqueando análise para evitar tips perigosas")
        return None  # VETO! Não analisa jogador que esfriou

    # 2. Análise normal dos últimos 5 jogos
    stats = analyze_last_5_games(matches, player_name)

    if not stats:
        return None

    # 3. CALCULAR CONFIDENCE SCORE (0-100)
    confidence = calculate_confidence_score(
        matches[:5], player_name, stats, regime)
    stats['confidence'] = confidence

    # 4. Adicionar informações de regime
    stats['regime_change'] = regime['regime_change']
    stats['regime_direction'] = regime.get('direction', 'STABLE')

    if regime['regime_change'] and regime['action'] == 'BOOST':
        print(
            f"[INFO] {player_name} em HOT STREAK: {regime['reason']} | Confidence: {confidence}%")
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
        goals = match.get('home_score_ft', 0) if is_home else match.get(
            'away_score_ft', 0)
        goals_list.append(goals or 0)

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
        print(f"[BLOCK] Liga {league_key} sem estatísticas no momento.")
        return strategies

    l_stats = all_league_stats[league_key]
    # DEBUG: Mostrar status da liga
    print(f"[DEBUG STRATEGY] Liga {league_key}: HT O0.5={l_stats['ht']['o05']}% (min 85), HT O1.5={l_stats['ht']['o15']}% (min 80), FT O1.5={l_stats['ft']['o15']}% (min 80)")

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

    # HT Strategies
    if 60 <= time_seconds <= 210:
        # +1.5 GOLS HT (Mandada cedo se o jogo for muito over)
        if (60 <= time_seconds <= 180 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o15'] >= 65):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 75 and
                    away_stats['ht_over_15_pct'] >= 75):
                strategies.append("⚽ +1.5 GOLS HT")
            else:
                print(f"[BLOCK] +1.5 HT: Estatísticas insuficientes")

        # +0.5 GOL HT (Aguardar até 02:00 para a odd subir e a linha abrir)
        if (120 <= time_seconds <= 210 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o05'] >= 72):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                    away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")
            else:
                print(f"[BLOCK] +0.5 HT: Estatísticas insuficientes")

        # +2.5 GOLS HT (Se já saiu 1 gol cedo)
        if (60 <= time_seconds <= 180 and ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1))):
            if (l_stats['ht']['o25'] >= 75):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 65 and
                    home_stats['ht_over_15_pct'] >= 90 and
                        away_stats['ht_over_15_pct'] >= 90):
                    strategies.append("⚽ +2.5 GOLS HT")
                else:
                    print(f"[BLOCK] +2.5 HT: Estatísticas insuficientes")

        # BTTS HT (Atrasado para 01:30)
        if (90 <= time_seconds <= 180 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['btts'] >= 75):
            if (home_stats['avg_goals_scored_ft'] >= 1.2 and
                away_stats['avg_goals_scored_ft'] >= 1.2 and
                avg_btts >= 75 and
                home_stats['ht_over_05_pct'] >= 90 and
                    away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ BTTS HT")
            else:
                print(f"[BLOCK] BTTS HT: Estatísticas insuficientes")

    # FT Strategies
    if 180 <= time_seconds <= 360:
        # momentum_factor: Se chegar nos 3 min (180s) em 0-0, aumenta a exigência
        strictness = 0
        if time_seconds >= 180 and home_goals == 0 and away_goals == 0:
            strictness = 5

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o15'] >= (80 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                    avg_btts >= (65 + strictness)):
                strategies.append("⚽ +1.5 GOLS FT")
            else:
                print(f"[BLOCK] +1.5 FT: Estatísticas insuficientes (strictness: {strictness})")

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o25'] >= (90 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                    avg_btts >= (80 + strictness)):
                strategies.append("⚽ +2.5 GOLS FT")

        # BTTS FT (Ambas Marcam Jogo Completo)
        if (180 <= time_seconds <= 300 and home_goals == 0 and away_goals == 0 and
                l_stats['ft']['btts'] >= 75):
            if (home_stats['avg_goals_scored_ft'] >= 1.2 and
                away_stats['avg_goals_scored_ft'] >= 1.2 and
                avg_btts >= 70 and
                home_stats['ft_scored_05_pct'] >= 80 and
                    away_stats['ft_scored_05_pct'] >= 80):
                strategies.append("⚽ BTTS FT")

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

        valid_scores_p1 = [(0, 0), (0, 1), (0, 2), (1, 1), (1, 2)]
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

        valid_scores_p2 = [(0, 0), (1, 0), (2, 0), (1, 1), (2, 1)]
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

    # HT Strategies
    if 90 <= time_seconds <= 360:
        # +1.5 GOLS HT (A partir de 01:30)
        if (90 <= time_seconds <= 300 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 90 and
                    away_stats['ht_over_15_pct'] >= 90):
                strategies.append("⚽ +1.5 GOLS HT")

        # +0.5 GOL HT (Aguardar até 03:00 para a odd subir e a linha abrir)
        if (180 <= time_seconds <= 360 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                    away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")

        # +2.5 GOLS HT (Se já saiu 1 gol cedo)
        if (90 <= time_seconds <= 240 and ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1))):
            if (l_stats['ht']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 75 and
                    home_stats['ht_over_15_pct'] == 100 and
                        away_stats['ht_over_15_pct'] == 100):
                    strategies.append("⚽ +2.5 GOLS HT")

        # BTTS HT (Atrasado para 02:00)
        if (120 <= time_seconds <= 240 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ht_over_05_pct'] == 100 and
                    away_stats['ht_over_05_pct'] == 100):
                strategies.append("⚽ BTTS HT")

    # FT Strategies
    if 260 <= time_seconds <= 510:
        # momentum_factor
        strictness = 0
        if time_seconds >= 360 and home_goals == 0 and away_goals == 0:
            strictness = 5

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o15'] >= (95 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                    avg_btts >= (75 + strictness)):
                strategies.append("⚽ +1.5 GOLS FT")

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o25'] >= (90 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                    avg_btts >= (80 + strictness)):
                strategies.append("⚽ +2.5 GOLS FT")

        # BTTS FT
        if (260 <= time_seconds <= 450 and home_goals == 0 and away_goals == 0 and
                l_stats['ft']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ft_scored_05_pct'] >= 90 and
                    away_stats['ft_scored_05_pct'] >= 90):
                strategies.append("⚽ BTTS FT")

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

        valid_scores_p1 = [(0, 0), (0, 1), (0, 2), (1, 1), (1, 2)]
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

        valid_scores_p2 = [(0, 0), (1, 0), (2, 0), (1, 1), (2, 1)]
        if (home_goals, away_goals) in valid_scores_p2:
            if (l_stats['ft']['o25'] >= 90):
                if (away_stats['avg_goals_scored_ft'] >= 0.8 and
                    away_stats['avg_goals_scored_ft'] <= 3.4 and
                    avg_btts <= 60 and
                    away_stats['ft_scored_25_pct'] >= 80 and
                        away_stats['ft_scored_35_pct'] >= 60):
                    strategies.append(f"⚽ {away_player} +2.5 GOLS FT")

    return strategies


def check_strategies_10mins(event, home_stats, away_stats, all_league_stats):
    """Estratégias para liga de 10 minutos (CLA)"""
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

    # HT Strategies
    if 70 <= time_seconds <= 270:
        # +1.5 GOLS HT (A partir de 01:10)
        if (70 <= time_seconds <= 180 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 90 and
                    away_stats['ht_over_15_pct'] >= 90):
                strategies.append("⚽ +1.5 GOLS HT")

        # +0.5 GOL HT (Aguardar até 02:30 para a odd subir e a linha abrir)
        if (150 <= time_seconds <= 240 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                    away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")

    # FT Strategies
    if 220 <= time_seconds <= 450:
        # momentum_factor
        strictness = 0
        if time_seconds >= 300 and home_goals == 0 and away_goals == 0:
            strictness = 5

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o15'] >= (95 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 0.8 and
                away_stats['avg_goals_scored_ft'] >= 0.8 and
                    avg_btts >= (75 + strictness)):
                strategies.append("⚽ +1.5 GOLS FT")

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o25'] >= (90 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                    avg_btts >= (80 + strictness)):
                strategies.append("⚽ +2.5 GOLS FT")
        
        # BTTS FT
        if (220 <= time_seconds <= 400 and home_goals == 0 and away_goals == 0 and
                l_stats['ft']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ft_scored_05_pct'] >= 90 and
                    away_stats['ft_scored_05_pct'] >= 90):
                strategies.append("⚽ BTTS FT")

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

    # HT Strategies
    if 30 <= time_seconds <= 150:
        # +1.5 GOLS HT (A partir de 30s)
        if (30 <= time_seconds <= 90 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o15'] >= 95):
            if (home_stats['avg_goals_scored_ft'] >= 1.0 and
                away_stats['avg_goals_scored_ft'] >= 1.0 and
                avg_btts >= 45 and
                home_stats['ht_over_15_pct'] >= 90 and
                    away_stats['ht_over_15_pct'] >= 90):
                strategies.append("⚽ +1.5 GOLS HT")

        # +0.5 GOL HT (Aguardar até 01:15 para a odd subir e a linha abrir)
        if (75 <= time_seconds <= 135 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['o05'] >= 100):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                avg_btts >= 45 and
                home_stats['ht_over_05_pct'] >= 90 and
                    away_stats['ht_over_05_pct'] >= 90):
                strategies.append("⚽ +0.5 GOL HT")

        # +2.5 GOLS HT (Se já saiu 1 gol cedo)
        if (30 <= time_seconds <= 90 and ((home_goals == 1 and away_goals == 0) or (home_goals == 0 and away_goals == 1))):
            if (l_stats['ht']['o25'] >= 90):
                if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                    away_stats['avg_goals_scored_ft'] >= 1.5 and
                    avg_btts >= 75 and
                    home_stats['ht_over_15_pct'] == 100 and
                        away_stats['ht_over_15_pct'] == 100):
                    strategies.append("⚽ +2.5 GOLS HT")

        # BTTS HT (Atrasado para 01:00)
        if (60 <= time_seconds <= 120 and home_goals == 0 and away_goals == 0 and
                l_stats['ht']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.3 and
                away_stats['avg_goals_scored_ft'] >= 1.3 and
                avg_btts >= 85 and
                home_stats['ht_over_05_pct'] == 100 and
                    away_stats['ht_over_05_pct'] == 100):
                strategies.append("⚽ BTTS HT")

    # FT Strategies
    if 150 <= time_seconds <= 265:
        # momentum_factor
        strictness = 0
        if time_seconds >= 180 and home_goals == 0 and away_goals == 0:
            strictness = 5

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o15'] >= (95 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 0.7 and
                away_stats['avg_goals_scored_ft'] >= 0.7 and
                    avg_btts >= (75 + strictness)):
                strategies.append("⚽ +1.5 GOLS FT")

        if (home_goals == 0 and away_goals == 0 and
                l_stats['ft']['o25'] >= (90 + strictness)):
            if (home_stats['avg_goals_scored_ft'] >= 2.0 and
                away_stats['avg_goals_scored_ft'] >= 2.0 and
                    avg_btts >= (80 + strictness)):
                strategies.append("⚽ +2.5 GOLS FT")
        
        # BTTS FT
        if (150 <= time_seconds <= 240 and home_goals == 0 and away_goals == 0 and
                l_stats['ft']['btts'] >= 90):
            if (home_stats['avg_goals_scored_ft'] >= 1.5 and
                away_stats['avg_goals_scored_ft'] >= 1.5 and
                avg_btts >= 85 and
                home_stats['ft_scored_05_pct'] >= 95 and
                    away_stats['ft_scored_05_pct'] >= 95):
                strategies.append("⚽ BTTS FT")

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

        valid_scores_p1 = [(0, 0), (0, 1), (0, 2), (1, 1), (1, 2)]
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

        valid_scores_p2 = [(0, 0), (1, 0), (2, 0), (1, 1), (2, 1)]
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
    event_id = event.get('id')
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
    msg += f"⏱️ Tempo: {time_str} | 📊 Placar: {scoreboard}\n"
    msg += f"🎮 <b>{home_player}</b> vs <b>{away_player}</b>\n\n"

    # Estatísticas formatadas
    if home_stats_summary and away_stats_summary:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📈 <b>ANÁLISE - ÚLTIMOS 5 JOGOS</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"

        avg_btts = (home_stats_summary['btts_pct'] +
                    away_stats_summary['btts_pct']) / 2

        msg += f"🏠 <b>{home_player}</b> (Conf: {home_confidence:.0f}%)\n"
        msg += f"├ HT: +0.5 ({home_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({home_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {home_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo\n"
        msg += f"└ Gols +3: {home_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"

        msg += f"✈️ <b>{away_player}</b> (Conf: {away_confidence:.0f}%)\n"
        msg += f"├ HT: +0.5 ({away_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({away_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {away_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo\n"
        msg += f"└ Gols +3: {away_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"

        msg += f"🔥 <b>BTTS Médio:</b> {avg_btts:.0f}%\n\n"

    # Link EstrelaBet
    if event_id:
        estrela_link = f"https://www.estrelabet.bet.br/apostas-ao-vivo?page=liveEvent&eventId={event_id}&sportId=66"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🎲 <a href='{estrela_link}'><b>CONFRONTO</b></a>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"

    return msg


def get_trend_emoji(perc, inverse=False):
    """Retorna emoji baseado na porcentagem"""
    adjusted = 100 - perc if inverse else perc

    if adjusted >= 95:
        return "🟢"
    if adjusted >= 80:
        return "🟡"
    if adjusted >= 60:
        return "🟠"
    return "🔴"

# =============================================================================
# ENVIO DE MENSAGENS
# =============================================================================


async def send_tip(bot, event, strategy, home_stats, away_stats):
    """Envia dica com metadados extras para evitar falsos greens"""
    event_id = event.get('id')
    period = 'HT' if 'HT' in strategy.upper() else 'FT'
    sent_key = f"{event_id}_{period}"
    if sent_key in sent_match_ids:
        return

    timer = event.get('timer', {})
    sent_minute = timer.get('minute', 0)

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
            sent_match_ids.add(sent_key)
            
            sent_tips.append({
                'event_id': event_id,
                'strategy': strategy,
                'sent_time': datetime.now(MANAUS_TZ),
                'status': 'pending',
                'message_id': message_obj.message_id,
                'message_text': msg,
                'home_player': event.get('homePlayer'),
                'away_player': event.get('awayPlayer'),
                'league': event.get('mappedLeague'),
                'tip_period': period,
                'sent_minute': sent_minute,
                'liveTimeRaw': event.get('liveTimeRaw', ''),
                'startDateRaw': event.get('startDateRaw', ''),
                'sent_scoreboard': event.get('scoreboard', '0-0')
            })
            save_state()
            print(f"[✓] Dica enviada: {event_id} - {strategy} ({period}) @ {sent_minute} min")
            break
        except Exception as e:
            print(f"[ERROR] send_tip tentativa {attempt+1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)

# =============================================================================
# VERIFICAÇÃO DE RESULTADOS
# =============================================================================


async def check_results(bot):
    """Versão ultra segura - evita green prematuro e matching errado"""
    global last_summary, last_league_message_id
    try:
        recent = fetch_recent_matches(num_pages=8)
        finished_matches = defaultdict(list)
        for match in recent:
            home = match.get('home_player', '').upper().strip()
            away = match.get('away_player', '').upper().strip()
            key = f"{home}_{away}"
            finished_matches[key].append(match)

        today = datetime.now(MANAUS_TZ).date()
        sent_tips[:] = [t for t in sent_tips if t['sent_time'].date() >= today - timedelta(days=2)]

        greens = reds = 0
        for tip in sent_tips:
            if tip['status'] != 'pending':
                if tip['status'] == 'green': greens += 1
                if tip['status'] == 'red': reds += 1
                continue

            # DELAY MÍNIMO (evita avaliar cedo)
            elapsed = (datetime.now(MANAUS_TZ) - tip['sent_time']).total_seconds()
            min_wait = 240 if tip.get('tip_period') == 'HT' else 480  # 4 min HT / 8 min FT
            if elapsed < min_wait:
                continue

            home = tip.get('home_player', '').upper().strip()
            away = tip.get('away_player', '').upper().strip()
            key = f"{home}_{away}"
            tip_league = tip.get('league', '').upper().strip()

            candidates = finished_matches.get(key, [])
            matched = None
            for m in sorted(candidates, key=lambda x: x.get('data_realizacao',''), reverse=True):
                match_league = m.get('league_name','').upper().strip()
                if tip_league and tip_league not in match_league and match_league not in tip_league:
                    continue
                try:
                    dt_str = m.get('data_realizacao','')
                    match_dt = datetime.fromisoformat(dt_str.replace('Z','+00:00'))
                    if match_dt.tzinfo is None:
                        match_dt = match_dt.replace(tzinfo=timezone.utc)
                    match_local = match_dt.astimezone(MANAUS_TZ)
                    diff = (match_local - tip['sent_time']).total_seconds()
                    if not (-180 <= diff <= 600):   # -3 a +10 minutos
                        continue
                except:
                    continue
                matched = m
                break

            if not matched:
                continue

            ht_total = int(matched.get('home_score_ht',0)) + int(matched.get('away_score_ht',0))
            ft_total = int(matched.get('home_score_ft',0)) + int(matched.get('away_score_ft',0))
            strategy = tip['strategy']
            result = None

            if '+0.5 GOL HT' in strategy: result = 'green' if ht_total >= 1 else 'red'
            elif '+1.5 GOLS HT' in strategy: result = 'green' if ht_total >= 2 else 'red'
            elif 'BTTS HT' in strategy: result = 'green' if (matched.get('home_score_ht',0)>0 and matched.get('away_score_ht',0)>0) else 'red'
            elif '+1.5 GOLS FT' in strategy: result = 'green' if ft_total >= 2 else 'red'
            elif '+2.5 GOLS FT' in strategy: result = 'green' if ft_total >= 3 else 'red'
            elif '+3.5 GOLS FT' in strategy: result = 'green' if ft_total >= 4 else 'red'

            if result:
                tip['status'] = result
                emoji = "✅✅✅✅✅" if result == 'green' else "❌❌❌❌❌"
                new_text = "━━━━━━━━━━━━━━━━━━━━\n📊 RESULTADO DA OPERAÇÃO\n━━━━━━━━━━━━━━━━━━━━\n\n"
                new_text += f"🏆 {tip.get('league','')}\n"
                new_text += f"💎 {strategy}\n"
                new_text += f"🎮 {home} vs {away}\n\n"
                new_text += f"📊 Resultado: HT {matched.get('home_score_ht',0)}-{matched.get('away_score_ht',0)} | FT {matched.get('home_score_ft',0)}-{matched.get('away_score_ft',0)}\n\n"
                new_text += emoji
                await bot.edit_message_text(chat_id=CHAT_ID, message_id=tip['message_id'], text=new_text, parse_mode="HTML")
                print(f"[✓] {result.upper()} aplicado com segurança")

            if tip['status'] == 'green': greens += 1
            if tip['status'] == 'red': reds += 1

        # Resumo diário
        total_resolved = greens + reds
        if total_resolved > 0:
            perc = (greens / total_resolved) * 100
            summary = f"<b>👑 RW TIPS - FIFA 🎮</b>\n✅ Green [{greens}]\n❌ Red [{reds}]\n📊 {perc:.1f}%"
            if summary != last_summary:
                await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
                last_summary = summary

        await update_league_stats(bot, recent)

    except Exception as e:
        print(f"[ERROR check_results] {e}")


async def update_league_stats(bot, recent_matches):
    """Atualiza e envia resumo das estatísticas das ligas com imagem"""
    global last_league_summary, last_league_message_id, league_stats, last_league_update_time

    try:
        # Ordenar partidas para garantir estabilidade nos cálculos
        recent_matches.sort(key=lambda x: (
            x.get('data_realizacao', ''), x.get('id', 0)), reverse=True)

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
            if len(games) < 5:
                continue

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

        if not stats:
            return

        # Comparação exata dos dicionários
        if league_stats and league_stats == stats:
            # print(f"[INFO] Resumo de ligas idêntico ao anterior. Ignorando envio.")
            return

        # VERIFICAR THROTTLE (Apenas 1 vez a cada 30 minutos)
        current_time = time.time()
        time_since_last = current_time - last_league_update_time
        
        if time_since_last < 600: # 10 minutos
            return

        league_stats = stats
        last_league_update_time = current_time

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
            except:
                pass

        msg = await bot.send_photo(
            chat_id=CHAT_ID,
            photo=bio,
            caption="📊 <b>ANÁLISE DE LIGAS</b> (Últimos 5 jogos)\n<i>🔴&lt;48% 🟠48-77% 🟡78-94% 🟢95%+</i>",
            parse_mode="HTML"
        )

        last_league_message_id = msg.message_id
        save_state()  # Salvar estado após atualizar a mensagem
        print("[✓] Resumo das ligas atualizado com imagem")

    except Exception as e:
        if "getaddrinfo failed" in str(e):
            print(f"[WARN] Falha de DNS: api.green365.cc está inacessível no momento.")
        else:
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
    total_height = header_height + \
        (num_leagues * cell_height) + (2 * padding) + 120

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
        draw.text((text_x, text_y), brand_text,
                  fill=brand_color, font=font_brand)
    else:
        # Só texto se não tiver logo
        draw.text(((total_width - brand_w) // 2, padding),
                  brand_text, fill=brand_color, font=font_brand)

    # Título secundário
    title = "ANALISE DE LIGAS (5 jogos)"
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    title_y = padding + logo_size + 10
    draw.text(((total_width - title_width) // 2, title_y),
              title, fill=header_color, font=font_title)

    # Headers das colunas
    headers = ["HT 0.5+", "HT 1.5+", "HT BTTS",
               "FT 1.5+", "FT 2.5+", "FT BTTS"]
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
        avg_over = (s['ht']['o05'] + s['ht']['o15'] + s['ht']['btts'] +
                    s['ft']['o15'] + s['ft']['o25'] + s['ft']['btts']) / 6
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
            print(
                f"\n[CICLO] {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

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

                home_source = event.get('homeSource', '?')
                away_source = event.get('awaySource', '?')
                home_raw = event.get('homeRaw', '?')
                away_raw = event.get('awayRaw', '?')

                print(
                    f"\n[EVENTO] {event_id}: {home_player} vs {away_player} - {league_name}")

                # Skip apenas se já processamos todas as possibilidades para este ID
                # (HT e FT). Se apenas um foi enviado, permitimos re-analisar para o outro.
                if f"{event_id}_HT" in sent_match_ids and f"{event_id}_FT" in sent_match_ids:
                    continue

                home_data = fetch_player_individual_stats(home_player)
                away_data = fetch_player_individual_stats(away_player)

                if not home_data or not away_data:
                    print(
                        f"[WARN] Sem dados suficientes para {home_player} ou {away_player}")
                    continue

                home_matches = home_data.get('matches', [])
                away_matches = away_data.get('matches', [])

                print(f"[INFO] {home_player} ({len(home_matches)} jogos) vs {away_player} ({len(away_matches)} jogos)")

                if len(home_matches) < 4 or len(away_matches) < 4:
                    print(
                        f"[WARN] Dados insuficientes (mínimo: 4)")
                    continue

                # Análise COM detecção de regime change
                home_stats = analyze_player_with_regime_check(
                    home_matches, home_player)
                away_stats = analyze_player_with_regime_check(
                    away_matches, away_player)

                if not home_stats or not away_stats:
                    print(
                        f"[WARN] Falha na análise das estatísticas (possível regime change detectado)")
                    continue

                # FILTRO DE CONFIDENCE MÍNIMO (Média de 60%)
                home_confidence = home_stats.get('confidence', 0)
                away_confidence = away_stats.get('confidence', 0)
                avg_confidence = (home_confidence + away_confidence) / 2

                if avg_confidence < 60:
                    print(
                        f"[BLOCKED] Confidence médio insuficiente: {avg_confidence:.0f}% (Home: {home_confidence:.0f}%, Away: {away_confidence:.0f}%)")
                    continue

                print(f"[✓] Confidence aprovado: Média {avg_confidence:.0f}% | {home_player}: {home_confidence:.0f}% | {away_player}: {away_confidence:.0f}%")

                print(f"[STATS] {home_player} (últimos 5 jogos): HT O0.5={home_stats['ht_over_05_pct']:.0f}% O1.5={home_stats['ht_over_15_pct']:.0f}% O2.5={home_stats['ht_over_25_pct']:.0f}% | Confidence: {home_confidence:.0f}%")
                print(f"[STATS] {away_player} (últimos 5 jogos): HT O0.5={away_stats['ht_over_05_pct']:.0f}% O1.5={away_stats['ht_over_15_pct']:.0f}% O2.5={away_stats['ht_over_25_pct']:.0f}% | Confidence: {away_confidence:.0f}%")

                # DETERMINAR ESTRATÉGIAS PELO TEMPO (Mapeamento ou Auto-detecção)
                strategies = []
                mapped_league = event.get('mappedLeague', '')
                raw_league = league_name.upper()

                # 1. Tentar por mapping explícito (com suporte a prefixo)
                if any(mapped_league.startswith(l) for l in ['BATTLE 8 MIN', 'H2H 8 MIN', 'VALHALLA CUP', 'VALKYRIE CUP']):
                    strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats)
                elif mapped_league.startswith('GT LEAGUE 12 MIN'):
                    strategies = check_strategies_12mins(event, home_stats, away_stats, league_stats)
                elif mapped_league.startswith('VOLTA 6 MIN'):
                    strategies = check_strategies_volta_6mins(event, home_stats, away_stats, league_stats)
                elif mapped_league.startswith('CLA 10 MIN'):
                    strategies = check_strategies_10mins(event, home_stats, away_stats, league_stats)
                
                # 2. Fallback: Auto-detecção por palavras-chave se não houver estratégias
                if not strategies:
                    if '2X6' in raw_league or '12 MIN' in raw_league or 'GT LEAGUE' in raw_league or 'CHAMPIONS LEAGUE' in raw_league:
                        strategies = check_strategies_12mins(event, home_stats, away_stats, league_stats)
                    elif '2X4' in raw_league or '8 MIN' in raw_league or 'BATTLE' in raw_league or 'H2H' in raw_league or 'PREMIER LEAGUE' in raw_league:
                        strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats)
                    elif '2X3' in raw_league or '6 MIN' in raw_league or 'VOLTA' in raw_league:
                        strategies = check_strategies_volta_6mins(event, home_stats, away_stats, league_stats)
                    elif 'CLA' in raw_league or '10 MIN' in raw_league:
                        strategies = check_strategies_10mins(event, home_stats, away_stats, league_stats)
                    elif 'VALHALLA' in raw_league or 'VALKYRIE' in raw_league:
                        strategies = check_strategies_8mins(event, home_stats, away_stats, league_stats)

                for strategy in strategies:
                    print(
                        f"[✓] OPORTUNIDADE ENCONTRADA: {strategy} | Confidence Médio: {avg_confidence:.0f}%")
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
    print(
        f"Horário: {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')} (Manaus)")
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
            print(
                f"[INFO] Tentando conectar ao Telegram (tentativa {attempt + 1}/{max_retries})...")
            me = await bot.get_me()
            print(f"[✓] Bot conectado: @{me.username}")
            break
        except Exception as e:
            print(f"[ERROR] Tentativa {attempt + 1} falhou: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(
                    f"[INFO] Aguardando {wait_time} segundos antes de tentar novamente...")
                await asyncio.sleep(wait_time)
            else:
                print(
                    "[ERROR] Não foi possível conectar ao Telegram após várias tentativas")
                print("[INFO] Verifique:")
                print("  1. Sua conexão com a internet")
                print("  2. Se o token do bot está correto")
                print("  3. Se não há firewall bloqueando")
                print("  4. Tente usar uma VPN se estiver bloqueado")
                return
    
    # Carregar estado persistido
    load_state()

    print("[INFO] Pré-carregando dados para análise imediata...")
    recent = fetch_recent_matches(num_pages=15)
    await update_league_stats(bot, recent)

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
