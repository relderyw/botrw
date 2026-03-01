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

LIVE_API_URL = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66&catIds=2085,1571,1728,1594,2086,1729,2130"
EVENT_API_URL = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventDetails?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}&showNonBoosts=false"
HISTORY_API_URL = "https://rwtips-k8j2.onrender.com/api/history"

AUTH_HEADER = "Bearer 444c7677f71663b246a40600ff53a8880240086750fda243735e849cdeba9702"

MANAUS_TZ = timezone(timedelta(hours=-4))
SAO_PAULO_TZ = timezone(timedelta(hours=-3))

# =============================================================================
# ✅ CORREÇÃO #1: SISTEMA DINÂMICO DE LIGAS (substituiu whitelist estática)
# Cada liga tem uma janela deslizante das últimas 20 tips.
# Bloqueia automaticamente se cair abaixo de 48% de assertividade.
# Desbloqueia automaticamente se subir acima de 72%.
# Notificação no Telegram quando o status muda.
# =============================================================================

from collections import deque

LEAGUE_UNLOCK_THRESHOLD = 72   # % para desbloquear liga bloqueada
LEAGUE_RELOCK_THRESHOLD = 60   # % para bloquear liga ativa (abaixo de 60% → bloqueia)
LEAGUE_WINDOW_SIZE      = 20   # Janela deslizante (últimas N tips)
LEAGUE_MIN_SAMPLES      = 8    # Mínimo de amostras antes de decidir

# Status inicial de cada liga (baseado no histórico disponível)
# True = começa ATIVA | False = começa BLOQUEADA (precisa provar)
LEAGUE_INITIAL_STATUS = {
    "BATTLE 8 MIN":      True,   # 60% histórico → ativa
    "VALHALLA CUP":      True,   # Alta pontuação → ativa
    "VALKYRIE CUP":      True,   # Ativa
    "GT LEAGUE 12 MIN":  True,   # Ativa
    "CLA 10 MIN":        True,   # Ativa
    "H2H 8 MIN":         False,  # 0% no dia → bloqueada, precisa provar
    "La Liga":           False,  # 0% → bloqueada
    "INT 8 MIN":         False,  # 25% → bloqueada
    "VOLTA 6 MIN":       False,  # Bloqueada
    "Ligue 1":           True,   # 66% → ativa (monitorada)
    "Serie A":           False,  # Bloqueada inicialmente
    "Conference League": False,  # Bloqueada inicialmente
    "Champions League":  False,  # Variável → bloqueada até provar
    "Bundesliga":        False,
    "Premier League":    False,
    "Europa League":     False,
}


class LeagueManager:
    """
    Gerencia o status de cada liga com base em performance real das tips.
    Estado persistido em league_performance.json.
    """

    def __init__(self, state_file='league_performance.json'):
        self.state_file = state_file
        self.leagues = {}
        self._load()

    def _load(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for league, info in data.items():
                    self.leagues[league] = {
                        'active': info.get('active', False),
                        'window': deque(info.get('window', []), maxlen=LEAGUE_WINDOW_SIZE),
                        'total_tips': info.get('total_tips', 0),
                    }
                print(f"[LeagueManager] {len(self.leagues)} ligas carregadas")
            except Exception as e:
                print(f"[LeagueManager] Erro ao carregar: {e}")

    def save(self):
        try:
            data = {
                league: {
                    'active': info['active'],
                    'window': list(info['window']),
                    'total_tips': info['total_tips'],
                }
                for league, info in self.leagues.items()
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[LeagueManager] Erro ao salvar: {e}")

    def _get_or_init(self, league):
        if league not in self.leagues:
            initial = LEAGUE_INITIAL_STATUS.get(league, False)
            self.leagues[league] = {
                'active': initial,
                'window': deque(maxlen=LEAGUE_WINDOW_SIZE),
                'total_tips': 0,
            }
            status_str = "ATIVA" if initial else "BLOQUEADA"
            print(f"[LeagueManager] Nova liga: {league} → {status_str}")
        return self.leagues[league]

    def is_active(self, league):
        """Retorna (bool_ativa, motivo)."""
        info = self._get_or_init(league)
        window = info['window']
        n = len(window)
        if n < LEAGUE_MIN_SAMPLES:
            return info['active'], f"Aguardando amostras ({n}/{LEAGUE_MIN_SAMPLES})"
        pct = (sum(window) / n) * 100
        return info['active'], f"{pct:.1f}% nas últimas {n} tips"

    def record_result(self, league, result_is_green):
        """
        Registra resultado e verifica se deve mudar status da liga.
        Retorna (mudou_status, novo_status, mensagem_telegram_ou_None).
        """
        info = self._get_or_init(league)
        info['window'].append(1 if result_is_green else 0)
        info['total_tips'] += 1

        window = info['window']
        n = len(window)

        if n < LEAGUE_MIN_SAMPLES:
            self.save()
            return False, info['active'], None

        pct = (sum(window) / n) * 100
        mudou = False
        mensagem = None

        if not info['active'] and pct >= LEAGUE_UNLOCK_THRESHOLD:
            info['active'] = True
            mudou = True
            mensagem = (
                f"🟢 <b>LIGA DESBLOQUEADA: {league}</b>\n"
                f"Assertividade: {pct:.1f}% nas últimas {n} tips\n"
                f"✅ Tips desta liga foram reativadas!"
            )
            print(f"[LeagueManager] 🟢 DESBLOQUEADA: {league} ({pct:.1f}%)")

        elif info['active'] and pct < LEAGUE_RELOCK_THRESHOLD:
            info['active'] = False
            mudou = True
            mensagem = (
                f"🔴 <b>LIGA BLOQUEADA: {league}</b>\n"
                f"Assertividade caiu para {pct:.1f}% nas últimas {n} tips\n"
                f"⏸ Tips desta liga suspensas temporariamente."
            )
            print(f"[LeagueManager] 🔴 BLOQUEADA: {league} ({pct:.1f}%)")

        self.save()
        return mudou, info['active'], mensagem

    def get_status_report(self):
        """Relatório completo de todas as ligas."""
        lines = ["📊 <b>STATUS DAS LIGAS (Dinâmico)</b>\n"]
        sorted_leagues = sorted(self.leagues.items(), key=lambda x: (not x[1]['active'], x[0]))
        for league, info in sorted_leagues:
            window = info['window']
            n = len(window)
            total = info['total_tips']
            if n >= LEAGUE_MIN_SAMPLES:
                pct = (sum(window) / n) * 100
                bar_filled = int(pct / 10)
                bar = "█" * bar_filled + "░" * (10 - bar_filled)
                stats_str = f"{pct:.0f}% [{bar}] (últ. {n})"
            else:
                stats_str = f"aguardando ({n}/{LEAGUE_MIN_SAMPLES})"
            emoji = "🟢" if info['active'] else "🔴"
            lines.append(f"{emoji} <b>{league}</b>: {stats_str} | {total} tips")
        return "\n".join(lines)


# Instância global
league_manager = LeagueManager()

LIVE_LEAGUE_MAPPING = {
    "E-Soccer - Battle - 8 minutos de jogo": "BATTLE 8 MIN",
    "Esoccer Battle - 8 mins play": "BATTLE 8 MIN",
    "E-Soccer - H2H GG League - 8 minutos de jogo": "H2H 8 MIN",
    "Esoccer H2H GG League - 8 mins play": "H2H 8 MIN",
    "H2H GG LEAGUE - E-FOOTBALL": "H2H 8 MIN",
    "H2H GG LEAGUE": "H2H 8 MIN",
    "E-Soccer - GT Leagues - 12 minutos de jogo": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues - 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues – 12 mins play": "GT LEAGUE 12 MIN",
    "E-Soccer - Battle Volta - 6 minutos de jogo": "VOLTA 6 MIN",
    "Esoccer Battle Volta - 6 mins play": "VOLTA 6 MIN",
    "H2H GG - E-football": "H2H 8 MIN",
    "H2H GG": "H2H 8 MIN",
    "Valhalla Cup": "VALHALLA CUP",
    "Valhalla League": "VALHALLA CUP",
    "Valkyrie Cup": "VALKYRIE CUP",
    "CLA": "CLA 10 MIN",
    "Cyber Live Arena": "CLA 10 MIN",
    "Champions League B 2×6": "GT LEAGUE 12 MIN",
    "Champions League B 2x6": "GT LEAGUE 12 MIN",
    "Champions League": "BATTLE 8 MIN",
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

HISTORY_LEAGUE_MAPPING = {
    "Battle 6m": "VOLTA 6 MIN",
    "Battle 8m": "BATTLE 8 MIN",
    "H2H 8m": "H2H 8 MIN",
    "GT Leagues 12m": "GT LEAGUE 12 MIN",
    "GT League 12m": "GT LEAGUE 12 MIN",
    "Esoccer Battle - 8 mins play": "BATTLE 8 MIN",
    "Esoccer Battle Volta - 6 mins play": "VOLTA 6 MIN",
    "Esoccer GT Leagues – 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer H2H GG League - 8 mins play": "H2H 8 MIN",
    "Valhalla Cup": "VALHALLA CUP",
    "Valkyrie Cup": "VALKYRIE CUP",
    "CLA League": "CLA 10 MIN",
    "CLA": "CLA 10 MIN",
    "Champions League": "BATTLE 8 MIN",
    "Super Lig": "GT LEAGUE 12 MIN",
    "Champions League B 2x6": "GT LEAGUE 12 MIN",
    "Champions League B 2×6": "GT LEAGUE 12 MIN",
    "ESportsBattle. RSL (2x6 mins)": "GT LEAGUE 12 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)": "VOLTA 6 MIN",
    "ESportsBattle. Premier League (2x4 mins)": "BATTLE 8 MIN",
}

# =============================================================================
# ✅ CORREÇÃO #2: THRESHOLDS POR LIGA
# Cada liga tem um perfil de pontuação diferente. Usar o mesmo threshold para
# VALHALLA (média 6+ gols/jogo) e La Liga (média 1-2 gols) era o maior bug.
# =============================================================================
LEAGUE_PROFILES = {
    # league_key: {avg_total_goals, avg_player_goals, min_player_goals_for_tip}
    "BATTLE 8 MIN":    {"avg_total": 5.5, "avg_player": 2.5, "min_player_avg": 2.0},
    "VALHALLA CUP":    {"avg_total": 7.0, "avg_player": 3.5, "min_player_avg": 3.0},
    "VALKYRIE CUP":    {"avg_total": 5.0, "avg_player": 2.5, "min_player_avg": 2.0},
    "GT LEAGUE 12 MIN":{"avg_total": 6.0, "avg_player": 3.0, "min_player_avg": 2.5},
    "H2H 8 MIN":       {"avg_total": 4.0, "avg_player": 2.0, "min_player_avg": 1.5},
    "CLA 10 MIN":      {"avg_total": 5.0, "avg_player": 2.5, "min_player_avg": 2.0},
    "DEFAULT":         {"avg_total": 4.0, "avg_player": 2.0, "min_player_avg": 1.5},
}

# =============================================================================
# CACHE E ESTADO GLOBAL
# =============================================================================
player_stats_cache = {}
CACHE_TTL = 300

global_history_cache = {
    'matches': [],
    'timestamp': 0
}
HISTORY_CACHE_TTL = 120

sent_tips = []
sent_match_ids = set()
last_summary = None
last_league_summary = None
last_league_message_id = None
league_stats = {}
last_league_update_time = 0

daily_stats = {}
last_daily_message_date = None

# ✅ CORREÇÃO #3: Stop-loss por jogador — rastrear sequência de reds por jogador
player_red_streak = {}  # {player_nick: {'reds': N, 'last_red_time': datetime}}
PLAYER_RED_STREAK_BLOCK = 3   # Bloquear jogador após 3 reds consecutivos
PLAYER_RED_COOLDOWN_MIN = 30  # Minutos de cooldown após bloqueio


def map_league_name(name):
    if not name: return "Unknown"
    if name in LIVE_LEAGUE_MAPPING: return LIVE_LEAGUE_MAPPING[name]
    if name in HISTORY_LEAGUE_MAPPING: return HISTORY_LEAGUE_MAPPING[name]
    n_upper = name.upper()
    for key, value in LIVE_LEAGUE_MAPPING.items():
        if n_upper.startswith(key.upper()): return value
    for key, value in HISTORY_LEAGUE_MAPPING.items():
        if n_upper.startswith(key.upper()): return value
    return name


def is_league_approved(mapped_league):
    """
    ✅ CORREÇÃO #1 (DINÂMICO): Consulta o LeagueManager para saber se a liga
    está ativa. O status muda automaticamente com base na performance real.
    """
    return league_manager.is_active(mapped_league)


def is_player_on_cooldown(player_nick):
    """
    ✅ CORREÇÃO #3: Verifica se o jogador está em cooldown por sequência de reds.
    """
    nick = player_nick.upper().strip()
    if nick not in player_red_streak:
        return False, 0
    streak_data = player_red_streak[nick]
    if streak_data['reds'] < PLAYER_RED_STREAK_BLOCK:
        return False, 0
    # Verificar se já passou o cooldown
    elapsed = (datetime.now(MANAUS_TZ) - streak_data['last_red_time']).total_seconds() / 60
    if elapsed < PLAYER_RED_COOLDOWN_MIN:
        remaining = PLAYER_RED_COOLDOWN_MIN - elapsed
        return True, remaining
    else:
        # Reset após cooldown
        player_red_streak[nick] = {'reds': 0, 'last_red_time': None}
        return False, 0


def update_player_red_streak(player_nick, result):
    """Atualiza contador de reds consecutivos do jogador."""
    nick = player_nick.upper().strip()
    if result == 'green':
        player_red_streak[nick] = {'reds': 0, 'last_red_time': None}
    else:
        current = player_red_streak.get(nick, {'reds': 0, 'last_red_time': None})
        player_red_streak[nick] = {
            'reds': current['reds'] + 1,
            'last_red_time': datetime.now(MANAUS_TZ)
        }


def save_state():
    try:
        state = {
            'last_league_message_id': last_league_message_id,
            'league_stats': league_stats,
            'last_league_update_time': last_league_update_time,
            'last_summary': last_summary,
            'last_daily_message_date': last_daily_message_date,
            'sent_match_ids': list(sent_match_ids),
            'player_red_streak': {
                k: {**v, 'last_red_time': v['last_red_time'].isoformat() if v.get('last_red_time') else None}
                for k, v in player_red_streak.items()
            },
        }
        with open('bot_state.json', 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)

        pending = [
            {**tip, 'sent_time': tip['sent_time'].isoformat()}
            for tip in sent_tips if tip.get('status') == 'pending'
        ]
        with open('tips_pending.json', 'w', encoding='utf-8') as f:
            json.dump(pending, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] save_state: {e}")


def save_tip_result(tip, ht_home, ht_away, ft_home, ft_away):
    try:
        results = {}
        if os.path.exists('tips_results.json'):
            with open('tips_results.json', 'r', encoding='utf-8') as f:
                results = json.load(f)

        sent_time = tip['sent_time']
        if isinstance(sent_time, str):
            sent_time = datetime.fromisoformat(sent_time)
        date_key = sent_time.astimezone(MANAUS_TZ).strftime('%Y-%m-%d')

        if date_key not in results:
            results[date_key] = []

        results[date_key].append({
            'sent_time': sent_time.isoformat(),
            'strategy': tip.get('strategy', ''),
            'status': tip.get('status', ''),
            'home_player': tip.get('home_player', ''),
            'away_player': tip.get('away_player', ''),
            'league': tip.get('league', ''),
            'tip_period': tip.get('tip_period', ''),
            'message_id': tip.get('message_id'),
            'result_ht': f"{ht_home}-{ht_away}",
            'result_ft': f"{ft_home}-{ft_away}",
        })

        with open('tips_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] save_tip_result: {e}")


def load_state():
    global last_league_message_id, league_stats, last_league_update_time, last_summary
    global sent_match_ids, sent_tips, daily_stats, last_daily_message_date, player_red_streak
    try:
        if os.path.exists('bot_state.json'):
            with open('bot_state.json', 'r', encoding='utf-8') as f:
                state = json.load(f)
            last_league_message_id = state.get('last_league_message_id')
            league_stats = state.get('league_stats', {})
            last_league_update_time = state.get('last_league_update_time', 0)
            last_summary = state.get('last_summary')
            last_daily_message_date = state.get('last_daily_message_date')
            sent_match_ids = set(state.get('sent_match_ids', []))
            # Restaurar player_red_streak
            raw_streak = state.get('player_red_streak', {})
            for k, v in raw_streak.items():
                player_red_streak[k] = {
                    'reds': v.get('reds', 0),
                    'last_red_time': datetime.fromisoformat(v['last_red_time']) if v.get('last_red_time') else None
                }

        sent_tips = []
        if os.path.exists('tips_pending.json'):
            with open('tips_pending.json', 'r', encoding='utf-8') as f:
                raw = json.load(f)
            for tip in raw:
                try:
                    tip['sent_time'] = datetime.fromisoformat(tip['sent_time'])
                    sent_tips.append(tip)
                except:
                    continue

        daily_stats = {}
        if os.path.exists('tips_results.json'):
            with open('tips_results.json', 'r', encoding='utf-8') as f:
                results = json.load(f)
            for date_key, tips_list in results.items():
                g = sum(1 for t in tips_list if t.get('status') == 'green')
                r = sum(1 for t in tips_list if t.get('status') == 'red')
                daily_stats[date_key] = {'green': g, 'red': r}

        print(f"[DEBUG] Estado carregado: {len(sent_tips)} tips pendentes")
    except Exception as e:
        print(f"[ERROR] load_state: {e}")


# =============================================================================
# UTILITÁRIOS
# =============================================================================

def clean_player_name(name):
    if not name or not isinstance(name, str):
        return ""
    if name.isupper() and ' ' not in name and 2 <= len(name) <= 15:
        return name
    match = re.search(r'\((.*?)\)', name)
    if not match:
        for sep in [' - ', ' vs ', ' / ']:
            if sep in name:
                parts = name.split(sep)
                p1, p2 = parts[0].strip(), parts[1].strip()
                if ' ' in p1 and ' ' not in p2: return p2
                if ' ' in p2 and ' ' not in p1: return p1
                return p2
        return name.strip()

    content_match = match.group(1).strip()
    outside = name.replace(f"({content_match})", "").strip()

    league_words = ['MINS', 'PLAY', 'LEAGUE', 'GT', 'BATTLE', 'VOLTA', 'ESOCCER', 'INTERNATIONAL', 'PART']
    if any(word in content_match.upper() for word in league_words):
        return outside.strip()

    def get_player_score(s):
        score = 0
        if not s: return -50
        if ' ' not in s: score += 20
        if s.isupper() and len(s) > 1: score += 25
        if '_' in s: score += 35
        if any(char.isdigit() for char in s):
            if re.search(r'\s(04|09|II|III)$', s) or s.endswith('04') or s.endswith('09'):
                score -= 15
            else:
                score += 20
        score -= len(s) * 1.0
        return score

    score_content = get_player_score(content_match)
    score_outside = get_player_score(outside)
    if score_content > score_outside - 10:
        return content_match
    return outside


def extract_pure_nick(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    name = raw.strip()
    match = re.search(r'\(([^)]+)\)', name)
    if match:
        inside = match.group(1).strip()
        if 2 <= len(inside) <= 16 and (inside.isupper() or '_' in inside or any(c.isdigit() for c in inside)):
            return inside.upper()
    cleaned = clean_player_name(name)
    words = re.findall(r'\b[A-Z0-9_]+\b', cleaned.upper())
    for word in reversed(words):
        if 2 <= len(word) <= 15:
            return word
    return cleaned.upper()[:15]


def get_player_ft_goals(player_nick: str, match: dict) -> int:
    if not player_nick or not match:
        return 0
    p_nick = player_nick.upper().strip()
    home_nick = extract_pure_nick(match.get('home_player') or match.get('home_team') or '')
    away_nick = extract_pure_nick(match.get('away_player') or match.get('away_team') or '')
    if p_nick == home_nick:
        return int(match.get('home_score_ft', 0) or 0)
    if p_nick == away_nick:
        return int(match.get('away_score_ft', 0) or 0)
    return 0


def find_best_match_for_tip(tip, recent_matches):
    tip_time = tip['sent_time']
    if tip_time.tzinfo is None:
        tip_time = tip_time.replace(tzinfo=MANAUS_TZ)

    h_nick = extract_pure_nick(
        tip.get('homeRaw') or tip.get('home_player') or
        tip.get('homeTeamName') or tip.get('homePlayer') or ''
    )
    a_nick = extract_pure_nick(
        tip.get('awayRaw') or tip.get('away_player') or
        tip.get('awayTeamName') or tip.get('awayPlayer') or ''
    )
    tip_nicks = {h_nick, a_nick} - {''}

    if not tip_nicks:
        return None

    tip_start_time = None
    if tip.get('startDateRaw'):
        try:
            dt_start = datetime.fromisoformat(tip['startDateRaw'].replace('Z', '+00:00'))
            if dt_start.tzinfo is not None:
                tip_start_time = dt_start.astimezone(MANAUS_TZ)
            else:
                tip_start_time = dt_start.replace(tzinfo=MANAUS_TZ)
        except:
            pass

    if not tip_start_time:
        sent_min = tip.get('sent_minute', 0)
        tip_start_time = tip_time - timedelta(minutes=sent_min)

    best = None
    best_diff = float('inf')

    for m in recent_matches:
        m_h = extract_pure_nick(m.get('home_player') or m.get('home_team') or '')
        m_a = extract_pure_nick(m.get('away_player') or m.get('away_team') or '')
        m_nicks = {m_h, m_a} - {''}

        if not (tip_nicks & m_nicks):
            continue

        start_diff = float('inf')

        try:
            dt_str = m.get('data_realizacao', '')
            if not dt_str:
                continue
            if 'T' in str(dt_str) or 'Z' in str(dt_str):
                dt = datetime.fromisoformat(str(dt_str).replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(str(dt_str), '%d/%m/%Y %H:%M:%S')

            if dt.tzinfo is not None:
                m_time = dt.astimezone(MANAUS_TZ)
            else:
                import pytz
                sp_tz = pytz.timezone('America/Sao_Paulo')
                m_time = sp_tz.localize(dt).astimezone(MANAUS_TZ)

            is_internal = str(m.get('id', '')).startswith('int_')

            if is_internal:
                delta_finish = (m_time - tip_time).total_seconds()
                if delta_finish < 180:
                    continue
                if delta_finish > 2400:
                    continue
                start_diff = abs(delta_finish - 600)
            else:
                delta_start = (m_time - tip_start_time).total_seconds()
                if delta_start < -240:
                    continue
                if delta_start > 2400:
                    continue
                start_diff = abs(delta_start)

        except Exception as e:
            continue

        try:
            sent_h, sent_a = map(int, tip.get('sent_scoreboard', '0-0').split('-'))
            final_h = int(m.get('home_score_ft', 0) or 0)
            final_a = int(m.get('away_score_ft', 0) or 0)
            if sent_h > final_h or sent_a > final_a:
                continue
        except:
            pass

        if start_diff < best_diff:
            best_diff = start_diff
            best = m

    if best and best_diff <= 1800:
        return best
    return None


# =============================================================================
# FUNÇÕES DE REQUISIÇÃO
# =============================================================================

def fetch_live_matches():
    try:
        print(f"[INFO] Buscando partidas ao vivo...")
        response = requests.get(LIVE_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        events = data.get('events', [])
        competitors_list = data.get('competitors', [])
        champs_list = data.get('champs', [])

        competitors_map = {c['id']: c for c in competitors_list}
        champs_map = {c['id']: c['name'] for c in champs_list}

        football_events = [e for e in events if e.get('sportId') == 66]

        def parse_live_time(live_time_str, start_date_str=None):
            if live_time_str:
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

                def parse_score_value(s):
                    if s is None: return [0, 0]
                    if isinstance(s, list):
                        return [int(s[0]) if len(s) > 0 else 0, int(s[1]) if len(s) > 1 else 0]
                    if isinstance(s, dict):
                        return [int(s.get('home', 0)), int(s.get('away', 0))]
                    if isinstance(s, str):
                        for sep in [':', '-', ' ']:
                            if sep in s:
                                parts = s.split(sep)
                                try: return [int(parts[0].strip()), int(parts[1].strip())]
                                except: pass
                    return [0, 0]

                score = parse_score_value(score_raw)

                home_comp = competitors_map.get(competitor_ids[0], {}) if len(competitor_ids) > 0 else {}
                away_comp = competitors_map.get(competitor_ids[1], {}) if len(competitor_ids) > 1 else {}

                home_competitor_name = home_comp.get('name', '')
                away_competitor_name = away_comp.get('name', '')

                if not home_competitor_name or not away_competitor_name:
                    continue

                name_from_full_home = clean_player_name(home_competitor_name)
                check_home_player = (home_comp.get('playerName') or home_comp.get('player_name') or
                                     home_comp.get('player') or home_comp.get('nickName') or event.get('home_player'))
                home_raw_text = home_competitor_name if '(' in home_competitor_name else (check_home_player or home_competitor_name)

                if '(' in home_competitor_name and name_from_full_home != home_competitor_name:
                    home_player = name_from_full_home
                    home_source = "Full-Name-Parentheses"
                else:
                    home_player = clean_player_name(check_home_player or home_competitor_name)
                    home_source = "API-Field" if check_home_player else "Full-Name-Cleaning"

                name_from_full_away = clean_player_name(away_competitor_name)
                check_away_player = (away_comp.get('playerName') or away_comp.get('player_name') or
                                     away_comp.get('player') or away_comp.get('nickName') or event.get('away_player'))
                away_raw_text = away_competitor_name if '(' in away_competitor_name else (check_away_player or away_competitor_name)

                if '(' in away_competitor_name and name_from_full_away != away_competitor_name:
                    away_player = name_from_full_away
                    away_source = "Full-Name-Parentheses"
                else:
                    away_player = clean_player_name(check_away_player or away_competitor_name)
                    away_source = "API-Field" if check_away_player else "Full-Name-Cleaning"

                home_team = (home_comp.get('teamName') or home_comp.get('team_name') or home_comp.get('team') or
                             event.get('home_team') or (home_competitor_name.split('(')[0].strip() if '(' in home_competitor_name else home_competitor_name))
                away_team = (away_comp.get('teamName') or away_comp.get('team_name') or away_comp.get('team') or
                             event.get('away_team') or (away_competitor_name.split('(')[0].strip() if '(' in away_competitor_name else away_competitor_name))

                league_name = champs_map.get(champ_id, 'Unknown League')

                if "ECOMP" in league_name.upper() or "VIRTUAL" in league_name.upper():
                    continue

                mapped_league = map_league_name(league_name)
                start_date = event.get('startDate', '')
                minute, second = parse_live_time(live_time, start_date_str=start_date)

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
                    'liveTimeRaw': live_time,
                    'startDateRaw': start_date,
                }

                normalized_events.append(normalized_event)

            except Exception as e:
                continue

        print(f"[INFO] {len(normalized_events)} partidas ao vivo normalizadas")
        return normalized_events

    except Exception as e:
        print(f"[ERROR] Altenar Live API falhou: {e}")
        return []


def fetch_event_markets(event_id):
    try:
        url = EVENT_API_URL.format(event_id)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.estrelabet.bet.br/"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        markets = data.get('markets', [])
        odds = data.get('odds', [])
        odds_map = {odd['id']: odd for odd in odds}
        open_lines = []

        for market in markets:
            market_name = market.get('name', '')
            desktop_odds = market.get('desktopOddIds', [])
            odd_ids_to_check = []
            for group in desktop_odds:
                if isinstance(group, list):
                    odd_ids_to_check.extend(group)
                else:
                    odd_ids_to_check.append(group)

            for odd_id in odd_ids_to_check:
                odd = odds_map.get(odd_id)
                if odd and odd.get('oddStatus') == 0:
                    open_lines.append({
                        'market_name': market_name,
                        'odd_name': odd.get('name', ''),
                        'odd_sv': odd.get('sv', ''),
                        'price': odd.get('price', 0)
                    })

        return open_lines

    except Exception as e:
        print(f"[WARN] Erro ao buscar mercados para {event_id}: {e}")
        return []


def fetch_recent_matches(num_pages=10, use_cache=True):
    global global_history_cache

    if use_cache and len(global_history_cache['matches']) > 200:
        cache_age = time.time() - global_history_cache['timestamp']
        if cache_age < HISTORY_CACHE_TTL:
            return global_history_cache['matches']

    fetch_pages = min(num_pages, 25)
    print(f"[INFO] Atualizando histórico ({fetch_pages} págs)...")

    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    int_session = requests.Session()
    int_retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    int_session.mount("https://", HTTPAdapter(max_retries=int_retry))

    def fetch_internal_page(page):
        try:
            params = {'page': page, 'limit': 40}
            response = int_session.get(HISTORY_API_URL, params=params, timeout=15)
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get('results', [])
        except Exception as e:
            if page <= 2:
                print(f"[WARN] Internal API page {page}: {e}")
            return []

    internal_matches = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        pages = range(1, fetch_pages + 1)
        results = list(executor.map(fetch_internal_page, pages))
        for r in results:
            if r:
                internal_matches.extend(r)

    all_combined = []

    for m in internal_matches:
        league = m.get('league_mapped') or m.get('league_name', 'Unknown')
        home_player = clean_player_name(m.get('home_nick') or m.get('home_player') or m.get('player_home') or
                                        m.get('home_player_name') or m.get('home_player_raw') or m.get('home_competitor_name', ''))
        away_player = clean_player_name(m.get('away_nick') or m.get('away_player') or m.get('player_away') or
                                        m.get('away_player_name') or m.get('away_player_raw') or m.get('away_competitor_name', ''))
        match_id = m.get('event_id') or m.get('id') or m.get('_id', 'unk')

        if m.get('finished_at'):
            fin_at = str(m.get('finished_at'))
            data_realizacao = f"{fin_at}Z" if not fin_at.endswith('Z') and '+' not in fin_at else fin_at
        else:
            data_realizacao = f"{m.get('match_date')}T{m.get('match_time')}" if m.get('match_date') else datetime.now().isoformat()

        all_combined.append({
            'id': f"int_{match_id}",
            'league_name': map_league_name(league),
            'home_player': home_player,
            'away_player': away_player,
            'home_team': m.get('home_raw') or m.get('home_team', ''),
            'away_team': m.get('away_raw') or m.get('away_team', ''),
            'home_team_logo': m.get('home_team_logo', ''),
            'away_team_logo': m.get('away_team_logo', ''),
            'data_realizacao': data_realizacao,
            'home_score_ht': m.get('home_score_ht', 0) or 0,
            'away_score_ht': m.get('away_score_ht', 0) or 0,
            'home_score_ft': m.get('home_score_ft', 0) or 0,
            'away_score_ft': m.get('away_score_ft', 0) or 0
        })

    all_combined.sort(key=lambda x: x['data_realizacao'], reverse=True)

    global_history_cache['matches'] = all_combined
    global_history_cache['timestamp'] = time.time()

    print(f"[INFO] Histórico atualizado: {len(all_combined)} partidas.")
    return all_combined


def fetch_player_individual_stats(player_name, use_cache=True):
    name_key = player_name.upper().strip()

    if use_cache and name_key in player_stats_cache:
        cached = player_stats_cache[name_key]
        if time.time() - cached['timestamp'] < CACHE_TTL:
            return cached['stats']

    all_matches = fetch_recent_matches(num_pages=20, use_cache=True)

    if not all_matches:
        return None

    player_matches = []
    for match in all_matches:
        home_player = match.get('home_player', '')
        away_player = match.get('away_player', '')
        if (home_player.upper().strip() == name_key or away_player.upper().strip() == name_key):
            player_matches.append(match)

    # ✅ CORREÇÃO #4: Aumentar janela para 10 jogos mínimos para análise confiável
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


# =============================================================================
# ANÁLISE DE ESTATÍSTICAS
# =============================================================================

def analyze_player_history(matches, player_name, window=10):
    if not matches:
        return None

    actual_window = min(len(matches), window)
    if actual_window < 5:  # ✅ CORREÇÃO #4: Mínimo 5 jogos (era 4)
        return None

    study_set = matches[:actual_window]

    stats_data = {
        'ht_over_05': 0, 'ht_over_15': 0, 'ht_over_25': 0,
        'ht_scored_05': 0, 'ht_scored_15': 0,
        'ft_over_05': 0, 'ft_over_15': 0, 'ft_over_25': 0, 'ft_over_35': 0, 'ft_over_45': 0,
        'ft_scored_05': 0, 'ft_scored_15': 0, 'ft_scored_25': 0, 'ft_scored_35': 0,
        'total_goals_scored': 0, 'total_goals_conceded': 0,
        'total_goals_scored_ht': 0, 'total_goals_conceded_ht': 0,
        'btts_count': 0, 'ht_btts_count': 0,
        'wins': 0, 'draws': 0, 'losses': 0,
        'games_count': actual_window,
        'goals_list': [],       # ✅ NOVO: lista para cálculo de stdev
        'goals_list_ht': [],
    }

    for match in study_set:
        is_home = match.get('home_player', '').upper() == player_name.upper()

        ht_home = match.get('home_score_ht', 0) or 0
        ht_away = match.get('away_score_ht', 0) or 0
        ht_total = ht_home + ht_away

        ft_home = match.get('home_score_ft', 0) or 0
        ft_away = match.get('away_score_ft', 0) or 0
        ft_total = ft_home + ft_away

        p_ht_g = ht_home if is_home else ht_away
        p_ht_c = ht_away if is_home else ht_home
        p_ft_g = ft_home if is_home else ft_away
        p_ft_c = ft_away if is_home else ft_home

        stats_data['goals_list'].append(p_ft_g)
        stats_data['goals_list_ht'].append(p_ht_g)

        if p_ft_g > p_ft_c: stats_data['wins'] += 1
        elif p_ft_g == p_ft_c: stats_data['draws'] += 1
        else: stats_data['losses'] += 1

        stats_data['total_goals_scored'] += p_ft_g
        stats_data['total_goals_conceded'] += p_ft_c
        stats_data['total_goals_scored_ht'] += p_ht_g
        stats_data['total_goals_conceded_ht'] += p_ht_c

        if ft_home > 0 and ft_away > 0: stats_data['btts_count'] += 1
        if ht_home > 0 and ht_away > 0: stats_data['ht_btts_count'] += 1

        if ht_total > 0: stats_data['ht_over_05'] += 1
        if ht_total > 1: stats_data['ht_over_15'] += 1
        if ht_total > 2: stats_data['ht_over_25'] += 1

        if p_ht_g > 0: stats_data['ht_scored_05'] += 1
        if p_ht_g > 1: stats_data['ht_scored_15'] += 1

        if ft_total > 0: stats_data['ft_over_05'] += 1
        if ft_total > 1: stats_data['ft_over_15'] += 1
        if ft_total > 2: stats_data['ft_over_25'] += 1
        if ft_total > 3: stats_data['ft_over_35'] += 1
        if ft_total > 4: stats_data['ft_over_45'] += 1

        if p_ft_g > 0: stats_data['ft_scored_05'] += 1
        if p_ft_g > 1: stats_data['ft_scored_15'] += 1
        if p_ft_g > 2: stats_data['ft_scored_25'] += 1
        if p_ft_g > 3: stats_data['ft_scored_35'] += 1

    n = actual_window

    # ✅ NOVO: Calcular desvio padrão dos gols individuais
    goals_list = stats_data['goals_list']
    try:
        goals_stdev = statistics.stdev(goals_list) if len(goals_list) >= 2 else 0
    except:
        goals_stdev = 0

    return {
        'ht_over_05_pct': (stats_data['ht_over_05'] / n) * 100,
        'ht_over_15_pct': (stats_data['ht_over_15'] / n) * 100,
        'ht_over_25_pct': (stats_data['ht_over_25'] / n) * 100,
        'ht_scored_05_pct': (stats_data['ht_scored_05'] / n) * 100,
        'ht_scored_15_pct': (stats_data['ht_scored_15'] / n) * 100,
        'ft_over_05_pct': (stats_data['ft_over_05'] / n) * 100,
        'ft_over_15_pct': (stats_data['ft_over_15'] / n) * 100,
        'ft_over_25_pct': (stats_data['ft_over_25'] / n) * 100,
        'ft_over_35_pct': (stats_data['ft_over_35'] / n) * 100,
        'ft_over_45_pct': (stats_data['ft_over_45'] / n) * 100,
        'ft_scored_05_pct': (stats_data['ft_scored_05'] / n) * 100,
        'ft_scored_15_pct': (stats_data['ft_scored_15'] / n) * 100,
        'ft_scored_25_pct': (stats_data['ft_scored_25'] / n) * 100,
        'ft_scored_35_pct': (stats_data['ft_scored_35'] / n) * 100,
        'avg_goals_scored_ft': stats_data['total_goals_scored'] / n,
        'avg_goals_conceded_ft': stats_data['total_goals_conceded'] / n,
        'avg_goals_scored_ht': stats_data['total_goals_scored_ht'] / n,
        'avg_goals_conceded_ht': stats_data['total_goals_conceded_ht'] / n,
        'btts_pct': (stats_data['btts_count'] / n) * 100,
        'ht_btts_pct': (stats_data['ht_btts_count'] / n) * 100,
        'win_pct': (stats_data['wins'] / n) * 100,
        'draw_pct': (stats_data['draws'] / n) * 100,
        'loss_pct': (stats_data['losses'] / n) * 100,
        'consistency_ft_3_plus_pct': (stats_data['ft_over_25'] / n) * 100,
        'consistency_ht_1_plus_pct': (stats_data['ht_over_05'] / n) * 100,
        'goals_stdev': goals_stdev,           # ✅ NOVO
        'goals_list': goals_list,             # ✅ NOVO: para backtesting
        'games_analyzed': n
    }


def get_h2h_stats(player1, player2):
    all_matches = global_history_cache.get('matches', [])
    if not all_matches:
        return None

    h2h_matches_list = []
    p1 = player1.upper()
    p2 = player2.upper()

    for m in all_matches:
        hp = m.get('home_player', '').upper()
        ap = m.get('away_player', '').upper()
        if (hp == p1 and ap == p2) or (hp == p2 and ap == p1):
            h2h_matches_list.append(m)

    if not h2h_matches_list:
        return None

    h2h_matches_list = h2h_matches_list[:5]
    stats = analyze_player_history(h2h_matches_list, player1, window=5)
    if stats:
        stats['count'] = len(h2h_matches_list)
    return stats


def detect_regime_change(matches):
    if len(matches) < 6:
        return {'regime_change': False}

    last_3 = matches[:3]
    previous_7 = matches[3:10] if len(matches) >= 10 else matches[3:]

    def avg_goals_window(window):
        total = 0
        for m in window:
            goals = m.get('home_score_ft', 0) or 0
            total += goals
        return total / len(window) if window else 0

    avg_last_3 = avg_goals_window(last_3)
    avg_previous = avg_goals_window(previous_7)

    if avg_previous > 0:
        ratio = avg_last_3 / avg_previous
        if ratio < 0.45 and avg_last_3 < 1.2:
            return {
                'regime_change': True,
                'direction': 'COOLING',
                'severity': 'HIGH',
                'avg_last_3': avg_last_3,
                'avg_previous': avg_previous,
                'action': 'AVOID',
                'reason': f'Jogador esfriou: {avg_last_3:.1f} vs {avg_previous:.1f}'
            }
        elif ratio > 1.8 and avg_last_3 > 2.0:
            return {
                'regime_change': True,
                'direction': 'HEATING',
                'severity': 'MEDIUM',
                'avg_last_3': avg_last_3,
                'avg_previous': avg_previous,
                'action': 'BOOST',
                'reason': f'Jogador em alta: {avg_last_3:.1f} vs {avg_previous:.1f}'
            }

    return {'regime_change': False}


def analyze_player_with_regime_check(matches, player_name):
    if not matches:
        return None

    # ✅ CORREÇÃO #4: Janela de 10 jogos para análise mais robusta
    stats = analyze_player_history(matches, player_name, window=10)
    if not stats:
        return None

    regime = detect_regime_change(matches)

    if regime['regime_change'] and regime['action'] == 'AVOID':
        print(f"[REGIME] {player_name} em queda. Bloqueando.")
        stats['confidence'] = 0
        return stats

    confidence = calculate_confidence_score(matches[:10], player_name, stats, regime)

    stats['confidence'] = confidence
    stats['regime_change'] = regime['regime_change']
    stats['regime_direction'] = regime.get('direction', 'STABLE')

    return stats


def calculate_confidence_score(last_matches, player_name, stats, regime):
    """
    ✅ CORREÇÃO #5: Score de confiança mais rigoroso.
    
    Principais mudanças:
    - Penalizar fortemente jogadores com alta variância (stdev alto)
    - Exigir média mínima de gols compatível com o threshold que será apostado
    - Usar janela de 10 jogos para cálculo de consistência
    """
    score = 0

    goals_list = stats.get('goals_list', [])

    # FATOR 1: Consistência via desvio padrão (40 pts)
    # Alta variância = imprevisível = não tippamos
    goals_stdev = stats.get('goals_stdev', 0)
    if goals_stdev <= 0.8:   score += 40  # Muito consistente
    elif goals_stdev <= 1.2: score += 30  # Consistente
    elif goals_stdev <= 1.8: score += 15  # Moderado
    elif goals_stdev <= 2.5: score += 5   # Volátil
    else:                    score += 0   # Muito volátil (não tippamos)

    # FATOR 2: Média de gols (30 pts)
    avg_goals_ft = stats['avg_goals_scored_ft']
    avg_goals_ht = stats['avg_goals_scored_ht']

    if avg_goals_ft >= 3.5:   score += 20
    elif avg_goals_ft >= 2.5: score += 13
    elif avg_goals_ft >= 1.5: score += 6
    else:                     score += 0  # Jogador de baixa pontuação: não tippamos +2.5

    if avg_goals_ht >= 1.5:   score += 10
    elif avg_goals_ht >= 0.8: score += 5
    else:                     score += 0

    # FATOR 3: Regime / Forma recente (20 pts)
    if regime.get('regime_change') and regime.get('action') == 'BOOST':
        score += 20
    elif not regime.get('regime_change'):
        score += 12  # Estável mas não excepcional
    # Se COOLING, já retornamos confidence=0 antes

    # FATOR 4: HT Dependability (10 pts)
    if stats['ht_over_05_pct'] >= 90:   score += 10
    elif stats['ht_over_05_pct'] >= 75: score += 5

    return min(100, score)


# =============================================================================
# ✅ CORREÇÃO #6: LÓGICA DE ESTRATÉGIAS COMPLETAMENTE REESCRITA
#
# Problemas do código original que geravam reds:
# 1. Apostava em gols INDIVIDUAIS do jogador mas usava thresholds do jogo TOTAL
# 2. Não verificava se a média de gols individuais suportava o threshold
# 3. Não considerava o perfil de pontuação da liga
# 4. Enviava múltiplas apostas no mesmo jogo (HT + FT + Individual)
# 5. Não tinha floor mínimo de gols por jogo para ligas específicas
# =============================================================================

def evaluate_open_lines(event, home_stats, away_stats, all_league_stats, open_lines, avg_confidence):
    """
    Avaliação de mercados com lógica corrigida.
    
    REGRAS PRINCIPAIS:
    1. Apostas individuais: verificar média de gols INDIVIDUAIS do jogador
    2. Apostas de jogo total: verificar média COMBINADA dos dois jogadores  
    3. Threshold deve ser NO MÁXIMO floor(média_individual * 0.85)
    4. Máximo 1 aposta por jogo (a melhor, por confidence ponderado)
    5. Ligas bloqueadas já foram filtradas antes de chegar aqui
    """
    candidates = []  # Lista de candidatos, depois escolhemos apenas o melhor

    timer = event.get('timer', {})
    minute = timer.get('minute', 0)
    second = timer.get('second', 0)
    time_seconds = minute * 60 + second

    league_key = event.get('mappedLeague', '')
    home_raw = event.get('homeRaw', '').strip()
    away_raw = event.get('awayRaw', '').strip()
    home_player = event.get('homePlayer', 'P1')
    away_player = event.get('awayPlayer', 'P2')

    # Perfil da liga
    league_profile = LEAGUE_PROFILES.get(league_key, LEAGUE_PROFILES["DEFAULT"])
    min_player_avg = league_profile['min_player_avg']

    # H2H Stats
    h2h = get_h2h_stats(home_player, away_player)

    def get_weighted_val(h_val, a_val, h2h_val):
        base = (h_val + a_val) / 2
        if h2h is not None and h2h_val > 0:
            return (base * 0.6 + h2h_val * 0.4)  # ✅ H2H tem peso maior quando disponível
        return base

    # Thresholds de tempo por liga
    MAX_HT_TIME = 210
    if league_key == 'VOLTA 6 MIN':    MAX_HT_TIME = 150
    elif league_key == 'CLA 10 MIN':   MAX_HT_TIME = 270
    elif league_key == 'GT LEAGUE 12 MIN': MAX_HT_TIME = 330

    score = event.get('score', {})
    hg = score.get('home', 0)
    ag = score.get('away', 0)
    total_now = hg + ag

    def parse_line(sv_str):
        try: return float(sv_str.split('|')[-1])
        except: return None

    def find_over_line(market_name_query):
        best_line = None
        q_lower = market_name_query.lower()
        for line in open_lines:
            m_lower = line['market_name'].lower()
            if q_lower in m_lower:
                if "tempo" not in q_lower and "tempo" in m_lower: continue
                if "mais de" in line['odd_name'].lower() or line['price'] > 0:
                    if "menos" in line['odd_name'].lower(): continue
                    if line['price'] < 1.65: continue  # ✅ Odd mínima ligeiramente reduzida
                    sv_val = parse_line(line['odd_sv'])
                    if sv_val is not None:
                        if best_line is None or sv_val < best_line['value']:
                            best_line = {'value': sv_val, 'odd_name': line['odd_name'], 'price': line['price']}
        return best_line

    def find_sim_line(market_name_query):
        q_lower = market_name_query.lower()
        for line in open_lines:
            m_lower = line['market_name'].lower()
            if q_lower in m_lower:
                if line['odd_name'].lower() == 'sim' and line['price'] >= 1.65:
                    return line
        return None

    # -------------------------------------------------------------------------
    # ✅ LÓGICA HT — Apostas de primeiro tempo
    # -------------------------------------------------------------------------
    if time_seconds <= MAX_HT_TIME and total_now == 0:
        ht_line = find_over_line("1º tempo - total") or find_over_line("1ª tempo - Total de gols")
        if ht_line:
            val = ht_line['value']

            # ✅ Para apostas HT, usamos a % de vezes que o TOTAL do jogo teve X gols no HT
            # Mas agora verificamos se a MÉDIA individual de ambos suporta
            home_ht_avg = home_stats['avg_goals_scored_ht']
            away_ht_avg = away_stats['avg_goals_scored_ht']
            combined_ht_avg = home_ht_avg + away_ht_avg

            h2h_ht_val = h2h.get(f'ht_over_{str(val).replace(".","")}_pct', 0) if h2h else 0
            w_pct = get_weighted_val(
                home_stats.get(f'ht_over_{str(val).replace(".","")}_pct', 0),
                away_stats.get(f'ht_over_{str(val).replace(".","")}_pct', 0),
                h2h_ht_val
            )

            # ✅ NOVO GATE: Média combinada HT deve suportar o threshold
            ht_needed_avg = val + 0.5  # ex: para over 1.5, precisa de avg >= 2.0
            avg_gate_ok = combined_ht_avg >= ht_needed_avg

            if val == 0.5 and w_pct >= 90 and avg_gate_ok:
                score_val = w_pct * (ht_line["price"] - 1)
                candidates.append({
                    "name": "⚽ +0.5 GOL HT",
                    "odd": ht_line["price"],
                    "score": score_val,
                    "confidence_pct": w_pct
                })
            elif val == 1.5 and w_pct >= 85 and avg_gate_ok:
                score_val = w_pct * (ht_line["price"] - 1)
                candidates.append({
                    "name": "⚽ +1.5 GOLS HT",
                    "odd": ht_line["price"],
                    "score": score_val,
                    "confidence_pct": w_pct
                })

    # -------------------------------------------------------------------------
    # ✅ LÓGICA FT — Apostas de jogo completo (total de gols)
    # -------------------------------------------------------------------------
    total_ft_line = find_over_line("Total de Gols")
    if total_ft_line:
        val = total_ft_line['value']
        needed = val - total_now

        # ✅ GATE PRINCIPAL: média combinada dos dois jogadores deve suportar o threshold
        combined_ft_avg = home_stats['avg_goals_scored_ft'] + away_stats['avg_goals_scored_ft']
        avg_gate_ok = combined_ft_avg >= (val * 0.85)  # Média precisa ser 85% do threshold

        if needed <= 2.0 and avg_gate_ok:
            key_str = str(val).replace(".", "")
            h2h_val = h2h.get(f'ft_over_{key_str}_pct', 0) if h2h else 0
            w_pct = get_weighted_val(
                home_stats.get(f'ft_over_{key_str}_pct', 0),
                away_stats.get(f'ft_over_{key_str}_pct', 0),
                h2h_val
            )

            # ✅ Thresholds mais rígidos que o original
            threshold_ok = False
            if val == 1.5 and w_pct >= 92:   threshold_ok = True
            elif val == 2.5 and w_pct >= 88: threshold_ok = True
            elif val == 3.5 and w_pct >= 85: threshold_ok = True
            elif val == 4.5 and w_pct >= 82: threshold_ok = True
            elif val == 5.5 and w_pct >= 80: threshold_ok = True

            if threshold_ok:
                score_val = w_pct * (total_ft_line["price"] - 1)
                candidates.append({
                    "name": f"⚽ +{val} GOLS FT (TOTAL)",
                    "odd": total_ft_line["price"],
                    "score": score_val,
                    "confidence_pct": w_pct
                })

    # -------------------------------------------------------------------------
    # ✅ LÓGICA INDIVIDUAL — Apostas em gols de UM jogador específico
    #
    # CORREÇÃO CRÍTICA: O código anterior apostava que o JOGADOR X marcaria
    # N gols, mas usava a % do TOTAL do jogo para decidir.
    # Agora usamos a % de gols INDIVIDUAIS do jogador.
    # -------------------------------------------------------------------------

    def evaluate_individual_player(player_raw, player_stats, player_goals_now):
        """Avalia se vale apostar nos gols individuais de um jogador."""
        avg_individual = player_stats['avg_goals_scored_ft']

        # ✅ Gate: jogador precisa ter média mínima para a liga
        if avg_individual < min_player_avg:
            print(f"[BLOCKED IND] {player_raw}: média {avg_individual:.1f} < mínimo {min_player_avg}")
            return None

        # Verificar mercado individual deste jogador
        ind_line = find_over_line(f"{player_raw} total")
        if not ind_line:
            return None

        v = ind_line['value']
        needed = v - player_goals_now

        if needed > 2.0:  # Precisa marcar mais de 2 gols ainda → muito arriscado
            return None

        # ✅ Usar porcentagem INDIVIDUAL do jogador (ft_scored), não do total do jogo
        key_str = str(v).replace(".", "")
        player_pct = player_stats.get(f'ft_scored_{key_str}_pct', 0)

        # Se não temos a porcentagem exata, estimamos pela média
        if player_pct == 0:
            # Estimar: se média é 3.0 e threshold é 2.5, a probabilidade é alta
            if avg_individual >= v * 1.1:
                player_pct = 75  # Estimativa conservadora
            else:
                return None

        # ✅ Gate mais rigoroso para apostas individuais
        if v <= 1.5 and player_pct < 85: return None
        if v <= 2.5 and player_pct < 80: return None
        if v <= 3.5 and player_pct < 75: return None
        if v <= 4.5 and player_pct < 70: return None

        score_val = player_pct * (ind_line["price"] - 1)
        return {
            "name": f"⚽ {player_raw} +{v} GOLS FT",
            "odd": ind_line["price"],
            "score": score_val,
            "confidence_pct": player_pct
        }

    home_candidate = evaluate_individual_player(home_raw, home_stats, hg)
    if home_candidate:
        candidates.append(home_candidate)

    away_candidate = evaluate_individual_player(away_raw, away_stats, ag)
    if away_candidate:
        candidates.append(away_candidate)

    # -------------------------------------------------------------------------
    # ✅ CORREÇÃO #7: RETORNAR APENAS O MELHOR CANDIDATO POR JOGO
    #
    # O código original retornava TODAS as oportunidades encontradas.
    # Isso causava 3-4 tips no mesmo jogo, multiplicando os reds.
    # Agora retornamos apenas a aposta com maior score ponderado.
    # -------------------------------------------------------------------------
    if not candidates:
        return []

    # Ordenar por score (ponderado por confiança e odd)
    candidates.sort(key=lambda x: x['score'], reverse=True)

    # Retornar apenas o melhor
    best = candidates[0]
    print(f"[SELEÇÃO] Melhor candidato: {best['name']} (score={best['score']:.1f}, conf={best['confidence_pct']:.0f}%)")
    if len(candidates) > 1:
        print(f"[SELEÇÃO] Descartados: {[c['name'] for c in candidates[1:]]}")

    # Remove o campo 'score' e 'confidence_pct' antes de retornar (são internos)
    return [{"name": best["name"], "odd": best["odd"]}]


# =============================================================================
# FORMATAÇÃO DE MENSAGENS
# =============================================================================

def format_tip_message(event, strategy, obs_odd, home_stats_summary, away_stats_summary):
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

    timer = event.get('timer', {})
    time_str = timer.get('formatted', '00:00')
    scoreboard = event.get('scoreboard', '0-0')

    home_confidence = home_stats_summary.get('confidence', 0)
    away_confidence = away_stats_summary.get('confidence', 0)
    avg_confidence = (home_confidence + away_confidence) / 2

    if avg_confidence >= 90:   confidence_emoji = "🔥🔥🔥"
    elif avg_confidence >= 80: confidence_emoji = "🔥🔥"
    elif avg_confidence >= 70: confidence_emoji = "🔥"
    else:                      confidence_emoji = "❄️"

    home_regime = home_stats_summary.get('regime_direction', 'STABLE')
    away_regime = away_stats_summary.get('regime_direction', 'STABLE')
    regime_status = "🔥 HEATING" if (home_regime == 'HEATING' or away_regime == 'HEATING') else "❄️ STABLE"

    msg = "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🎯 <b>OPORTUNIDADE DETECTADA</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"{confidence_emoji} <b>Confidence: {avg_confidence:.0f}%</b> | {regime_status}\n\n"
    msg += f"🏆 <b>{clean_league}</b>\n"
    msg += f"💎 <b>{strategy}</b>\n"
    msg += f"📈 <b>Odd: {obs_odd}</b>\n\n"
    msg += f"⏱️ Tempo: {time_str} | 📊 Placar: {scoreboard}\n"
    msg += f"🎮 <b>{home_player}</b> vs <b>{away_player}</b>\n\n"

    if home_stats_summary and away_stats_summary:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📈 <b>ANÁLISE - ÚLTIMOS 10 JOGOS</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"

        avg_btts = (home_stats_summary['btts_pct'] + away_stats_summary['btts_pct']) / 2

        msg += f"🏠 <b>{home_player}</b> (Conf: {home_confidence:.0f}%)\n"
        msg += f"├ HT: +0.5 ({home_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({home_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {home_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo (σ={home_stats_summary.get('goals_stdev', 0):.1f})\n"
        msg += f"└ Gols +3: {home_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"

        msg += f"✈️ <b>{away_player}</b> (Conf: {away_confidence:.0f}%)\n"
        msg += f"├ HT: +0.5 ({away_stats_summary['ht_over_05_pct']:.0f}%) • +1.5 ({away_stats_summary['ht_over_15_pct']:.0f}%)\n"
        msg += f"├ FT: Média {away_stats_summary['avg_goals_scored_ft']:.1f} gols/jogo (σ={away_stats_summary.get('goals_stdev', 0):.1f})\n"
        msg += f"└ Gols +3: {away_stats_summary['consistency_ft_3_plus_pct']:.0f}% dos jogos\n\n"

        msg += f"🔥 <b>BTTS Médio:</b> {avg_btts:.0f}%\n\n"

    if event_id:
        estrela_link = f"https://www.estrelabet.bet.br/apostas-ao-vivo?page=liveEvent&eventId={event_id}&sportId=66"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🎲 <a href='{estrela_link}'><b>CONFRONTO</b></a>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"

    return msg


# =============================================================================
# ENVIO DE MENSAGENS
# =============================================================================

async def send_tip(bot, event, strategy, obs_odd, home_stats, away_stats):
    event_id = event.get('id')
    period = 'HT' if 'HT' in strategy.upper() else 'FT'
    sent_key = f"{event_id}_{period}"

    # ✅ CORREÇÃO #7: Máximo 1 tip por evento (não separar HT e FT)
    # Agora usamos apenas event_id como chave (sem separação HT/FT)
    event_base_key = f"{event_id}_ANY"
    if event_base_key in sent_match_ids:
        print(f"[SKIP] Evento {event_id} já teve uma tip enviada hoje.")
        return

    timer = event.get('timer', {})
    sent_minute = timer.get('minute', 0)

    # ✅ Verificar cooldown do jogador
    home_player = event.get('homePlayer', '')
    away_player = event.get('awayPlayer', '')
    for player in [home_player, away_player]:
        on_cooldown, remaining = is_player_on_cooldown(player)
        if on_cooldown:
            print(f"[COOLDOWN] {player} em cooldown por {remaining:.0f} min. Pulando tip.")
            return

    max_retries = 3
    for attempt in range(max_retries):
        try:
            msg = format_tip_message(event, strategy, obs_odd, home_stats, away_stats)
            message_obj = await bot.send_message(
                chat_id=CHAT_ID,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )

            # Marcar o evento inteiro como processado
            sent_match_ids.add(event_base_key)
            sent_match_ids.add(sent_key)  # Compatibilidade

            tipped_player = None
            tipped_nick = ""
            strategy_lower = strategy.lower()
            for p in [event.get('homePlayer'), event.get('awayPlayer')]:
                if p and p.lower() in strategy_lower:
                    tipped_player = p
                    tipped_nick = extract_pure_nick(p)
                    break
            if not tipped_player:
                tipped_player = event.get('homePlayer')
                tipped_nick = extract_pure_nick(tipped_player)

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
                'homeTeamName': event.get('homeTeamName', ''),
                'awayTeamName': event.get('awayTeamName', ''),
                'liveTimeRaw': event.get('liveTimeRaw', ''),
                'startDateRaw': event.get('startDateRaw', ''),
                'sent_scoreboard': event.get('scoreboard', '0-0'),
                'tipped_player_nick': tipped_nick,
                'tipped_player_raw': tipped_player,
                'sent_odd': obs_odd
            })
            save_state()
            print(f"[✓] Tip enviada: {event_id} - {strategy} @ min {sent_minute}")
            break
        except Exception as e:
            print(f"[ERROR] send_tip tentativa {attempt+1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)


# =============================================================================
# VERIFICAÇÃO DE RESULTADOS
# =============================================================================

async def check_results(bot):
    global last_summary, last_league_message_id, daily_stats, last_daily_message_date
    try:
        recent = fetch_recent_matches(num_pages=30, use_cache=False)

        today_date = datetime.now(MANAUS_TZ)
        today = today_date.date()
        today_str = today_date.strftime('%Y-%m-%d')

        sent_tips[:] = [t for t in sent_tips if t['sent_time'].date() >= today - timedelta(days=5)]

        for tip in sent_tips:
            if tip['status'] != 'pending':
                continue

            elapsed = (datetime.now(MANAUS_TZ) - tip['sent_time']).total_seconds()
            league_upper = tip.get('league', '').upper()
            if any(x in league_upper for x in ["VALKYRIE", "CLA", "H2H", "BATTLE"]):
                min_wait = 150 if tip.get('tip_period') == 'HT' else 400
            else:
                min_wait = 240 if tip.get('tip_period') == 'HT' else 480

            if elapsed < min_wait:
                continue

            matched = find_best_match_for_tip(tip, recent)
            if not matched:
                print(f"[PENDENTE] {tip.get('home_player')} vs {tip.get('away_player')} - sem match")
                continue

            strategy = tip['strategy']
            tipped_nick = tip.get('tipped_player_nick', '')

            ht_home = int(matched.get('home_score_ht', 0) or 0)
            ht_away = int(matched.get('away_score_ht', 0) or 0)
            ft_home_total = int(matched.get('home_score_ft', 0) or 0)
            ft_away_total = int(matched.get('away_score_ft', 0) or 0)
            ht_total = ht_home + ht_away
            ft_total = ft_home_total + ft_away_total

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
                if tipped_nick:
                    gols = get_player_ft_goals(tipped_nick, matched)
                    result = 'green' if gols >= 2 else 'red'
                else:
                    result = 'green' if ft_total >= 2 else 'red'
            elif '+2.5 GOLS FT' in strategy:
                if tipped_nick:
                    gols = get_player_ft_goals(tipped_nick, matched)
                    result = 'green' if gols >= 3 else 'red'
                else:
                    result = 'green' if ft_total >= 3 else 'red'
            elif '+3.5 GOLS FT' in strategy:
                if tipped_nick:
                    gols = get_player_ft_goals(tipped_nick, matched)
                    result = 'green' if gols >= 4 else 'red'
                else:
                    result = 'green' if ft_total >= 4 else 'red'
            elif '+4.5 GOLS FT' in strategy:
                if tipped_nick:
                    gols = get_player_ft_goals(tipped_nick, matched)
                    result = 'green' if gols >= 5 else 'red'
                else:
                    result = 'green' if ft_total >= 5 else 'red'
            elif '+5.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 6 else 'red'
            elif 'BTTS FT' in strategy:
                result = 'green' if (ft_home_total > 0 and ft_away_total > 0) else 'red'
            # ✅ NOVO: Verificação de vitória individual
            elif 'VITORIA' in strategy.upper():
                # Extrair qual jogador venceu
                if home_player in strategy and ft_home_total > ft_away_total:
                    result = 'green'
                elif away_player in strategy and ft_away_total > ft_home_total:
                    result = 'green'
                else:
                    result = 'red'

            if result:
                tip['status'] = result
                emoji = "✅✅✅✅✅" if result == 'green' else "❌❌❌❌❌"
                new_text = "━━━━━━━━━━━━━━━━━━━━\n📊 RESULTADO DA OPERAÇÃO\n━━━━━━━━━━━━━━━━━━━━\n\n"
                new_text += f"🏆 {tip.get('league','')}\n"
                new_text += f"💠 {strategy}"
                if tip.get('sent_odd'):
                    new_text += f" (@{tip.get('sent_odd')})\n"
                else:
                    new_text += "\n"
                new_text += f"🎮 {tip.get('home_player')} vs {tip.get('away_player')}\n\n"
                new_text += f"📊 Resultado: HT {ht_home}-{ht_away} | FT {ft_home_total}-{ft_away_total}\n\n"
                new_text += emoji

                try:
                    await bot.edit_message_text(chat_id=CHAT_ID, message_id=tip['message_id'], text=new_text, parse_mode="HTML")
                except Exception as e:
                    print(f"[WARN] Não editou mensagem: {e}")

                print(f"[✓] {result.upper()} - {strategy}")

                save_tip_result(tip, ht_home, ht_away, ft_home_total, ft_away_total)

                # ✅ CORREÇÃO #1 DINÂMICA: Atualizar performance da liga
                tip_league = tip.get('league', '')
                if tip_league:
                    mudou_liga, novo_status, msg_liga = league_manager.record_result(
                        tip_league, result == 'green'
                    )
                    if mudou_liga and msg_liga:
                        try:
                            await bot.send_message(chat_id=CHAT_ID, text=msg_liga, parse_mode="HTML")
                        except Exception as e:
                            print(f"[WARN] Não enviou notif de liga: {e}")

                # ✅ Atualizar streak do jogador para stop-loss
                tipped_p = tip.get('tipped_player_nick', '')
                if tipped_p:
                    update_player_red_streak(tipped_p, result)

                d_key = tip['sent_time'].astimezone(MANAUS_TZ).strftime('%Y-%m-%d')
                if d_key not in daily_stats:
                    daily_stats[d_key] = {'green': 0, 'red': 0}
                daily_stats[d_key][result] += 1

        sent_tips[:] = [t for t in sent_tips if t.get('status') == 'pending']

        today_greens = daily_stats.get(today_str, {}).get('green', 0)
        today_reds = daily_stats.get(today_str, {}).get('red', 0)

        total_resolved = today_greens + today_reds
        if total_resolved > 0:
            perc = (today_greens / total_resolved) * 100
            summary = f"<b>👑 RW TIPS - FIFA 🎮</b>\n✅ Green [{today_greens}]\n❌ Red [{today_reds}]\n📊 {perc:.1f}%"
            if summary != last_summary:
                await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
                last_summary = summary

        if last_daily_message_date and last_daily_message_date != today_str:
            sorted_dates = sorted(daily_stats.keys())
            if sorted_dates:
                msg = "🚨 <b>Resumo Geral:</b>\n\n"
                for date_str in sorted_dates[-7:]:
                    ds = daily_stats[date_str]
                    g = ds['green']
                    r = ds['red']
                    t = g + r
                    if t == 0: continue
                    pct = (g / t) * 100
                    fmt_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d/%m')
                    msg += f"📅 {fmt_date} --> ✅ [{g}] | ❌ [{r}] | 📊 [{pct:.1f}%]\n"
                await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")

        if last_daily_message_date != today_str:
            last_daily_message_date = today_str
            # Enviar relatório de status das ligas no início do novo dia
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=league_manager.get_status_report(),
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"[WARN] Erro ao enviar status das ligas: {e}")
            save_state()

        await update_league_stats(bot, recent)

    except Exception as e:
        print(f"[ERROR check_results] {e}")


async def update_league_stats(bot, recent_matches):
    global last_league_summary, last_league_message_id, league_stats, last_league_update_time

    try:
        recent_matches.sort(key=lambda x: (x.get('data_realizacao', ''), x.get('id', 0)), reverse=True)

        league_games = defaultdict(list)
        for match in recent_matches:
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

        if league_stats and league_stats == stats:
            return

        current_time = time.time()
        if current_time - last_league_update_time < 600:
            return

        league_stats = stats
        last_league_update_time = current_time

        img = create_league_stats_image(stats)
        bio = BytesIO()
        img.save(bio, 'PNG')
        bio.seek(0)

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
        save_state()
        print("[✓] Resumo das ligas atualizado")

    except Exception as e:
        print(f"[ERROR] update_league_stats: {e}")


def create_league_stats_image(stats):
    import os

    bg_color = (0, 0, 0)
    card_bg = (20, 20, 20)
    header_bg = (30, 30, 30)
    text_color = (255, 255, 255)
    header_color = (0, 255, 200)
    gold_color = (255, 200, 50)
    brand_color = (0, 255, 100)

    sorted_leagues = sorted(stats.keys())
    num_leagues = len(sorted_leagues)

    cell_width = 160
    cell_height = 90
    label_width = 300
    logo_height = 80
    header_height = 140
    padding = 40

    total_width = label_width + (6 * cell_width) + (2 * padding)
    total_height = header_height + (num_leagues * cell_height) + (2 * padding) + 120

    img = Image.new('RGB', (total_width, total_height), bg_color)
    draw = ImageDraw.Draw(img)

    size_title = 30
    size_header = 25
    size_cell = 35
    size_league = 25
    size_brand = 35

    font_paths = [
        "C:\\Windows\\Fonts\\arialbd.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "arial.ttf",
        "DejaVuSans-Bold.ttf",
    ]

    font_title = font_header = font_cell = font_league = font_brand = None

    for font_path in font_paths:
        try:
            font_title = ImageFont.truetype(font_path, size_title)
            font_header = ImageFont.truetype(font_path, size_header)
            font_cell = ImageFont.truetype(font_path, size_cell)
            font_league = ImageFont.truetype(font_path, size_league)
            font_brand = ImageFont.truetype(font_path, size_brand)
            break
        except:
            continue

    if not font_title:
        try:
            font_title = ImageFont.load_default(size=size_title)
            font_header = ImageFont.load_default(size=size_header)
            font_cell = ImageFont.load_default(size=size_cell)
            font_league = ImageFont.load_default(size=size_league)
            font_brand = ImageFont.load_default(size=size_brand)
        except:
            font_title = font_header = font_cell = font_league = font_brand = ImageFont.load_default()

    logo_size = 50
    brand_text = "RW TIPS"

    logo_path = os.path.join(os.path.dirname(__file__), "app_icon.png")
    logo_loaded = False

    try:
        logo = Image.open(logo_path).convert("RGBA")
        logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
        logo_loaded = True
    except:
        pass

    brand_bbox = draw.textbbox((0, 0), brand_text, font=font_brand)
    brand_w = brand_bbox[2] - brand_bbox[0]

    if logo_loaded:
        total_brand_width = logo_size + 15 + brand_w
        start_x = (total_width - total_brand_width) // 2
        logo_y = padding
        img.paste(logo, (start_x, logo_y), logo)
        text_x = start_x + logo_size + 15
        text_y = padding + (logo_size - size_brand) // 2
        draw.text((text_x, text_y), brand_text, fill=brand_color, font=font_brand)
    else:
        draw.text(((total_width - brand_w) // 2, padding), brand_text, fill=brand_color, font=font_brand)

    title = "ANALISE DE LIGAS (5 jogos)"
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    title_y = padding + logo_size + 10
    draw.text(((total_width - title_width) // 2, title_y), title, fill=header_color, font=font_title)

    headers = ["HT 0.5+", "HT 1.5+", "HT BTTS", "FT 1.5+", "FT 2.5+", "FT BTTS"]
    y_pos = title_y + 100

    for i, header in enumerate(headers):
        x_pos = label_width + (i * cell_width) + padding
        draw.rectangle([x_pos, y_pos - 35, x_pos + cell_width, y_pos - 5], fill=header_bg, outline=card_bg, width=2)
        header_bbox = draw.textbbox((0, 0), header, font=font_header)
        header_w = header_bbox[2] - header_bbox[0]
        draw.text((x_pos + (cell_width - header_w) // 2, y_pos - 28), header, fill=header_color, font=font_header)

    def get_heat_color(pct):
        if pct >= 95:   return (0, 255, 136)
        elif pct >= 78: return (255, 238, 68)
        elif pct >= 48: return (255, 136, 68)
        else:           return (255, 68, 68)

    for idx, league in enumerate(sorted_leagues):
        s = stats[league]
        row_y = y_pos + (idx * cell_height)

        draw.rectangle([padding, row_y, label_width + padding - 10, row_y + cell_height], fill=header_bg, outline=card_bg, width=2)
        draw.text((padding + 10, row_y + 15), f"{league}", fill=gold_color, font=font_league)

        values = [s['ht']['o05'], s['ht']['o15'], s['ht']['btts'], s['ft']['o15'], s['ft']['o25'], s['ft']['btts']]

        for i, val in enumerate(values):
            x_pos = label_width + (i * cell_width) + padding
            color = get_heat_color(val)
            draw.rectangle([x_pos, row_y, x_pos + cell_width, row_y + cell_height], fill=color, outline=card_bg, width=2)
            text = f"{val}%"
            text_bbox = draw.textbbox((0, 0), text, font=font_cell)
            text_w = text_bbox[2] - text_bbox[0]
            text_color_cell = (0, 0, 0) if val >= 60 else (255, 255, 255)
            draw.text((x_pos + (cell_width - text_w) // 2, row_y + 15), text, fill=text_color_cell, font=font_cell)

    league_scores = {}
    for league in sorted_leagues:
        s = stats[league]
        avg_over = (s['ht']['o05'] + s['ht']['o15'] + s['ht']['btts'] + s['ft']['o15'] + s['ft']['o25'] + s['ft']['btts']) / 6
        league_scores[league] = avg_over

    best_over = max(league_scores, key=league_scores.get)
    best_under = min(league_scores, key=league_scores.get)

    highlight_y = y_pos + (num_leagues * cell_height) + 15
    over_text = f">> MELHOR OVER: {best_over} ({league_scores[best_over]:.0f}% media)"
    over_bbox = draw.textbbox((0, 0), over_text, font=font_header)
    over_w = over_bbox[2] - over_bbox[0]
    draw.text(((total_width - over_w) // 2, highlight_y), over_text, fill=(0, 255, 136), font=font_header)

    under_y = highlight_y + 28
    under_text = f">> LIGA UNDER: {best_under} ({league_scores[best_under]:.0f}% media)"
    under_bbox = draw.textbbox((0, 0), under_text, font=font_header)
    under_w = under_bbox[2] - under_bbox[0]
    draw.text(((total_width - under_w) // 2, under_y), under_text, fill=(255, 68, 68), font=font_header)

    return img


# =============================================================================
# LOOP PRINCIPAL
# =============================================================================

async def main_loop(bot):
    print("[INFO] Iniciando loop principal...")

    while True:
        try:
            print(f"\n[CICLO] {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

            live_events = fetch_live_matches()

            if not live_events:
                print("[INFO] Nenhuma partida ao vivo")
                await asyncio.sleep(10)
                continue

            for event in live_events:
                event_id = event.get('id')
                league_name = event.get('leagueName', '')
                mapped_league = event.get('mappedLeague', '')
                home_player = event.get('homePlayer', '')
                away_player = event.get('awayPlayer', '')

                print(f"\n[EVENTO] {event_id}: {home_player} vs {away_player} - {mapped_league}")

                # ✅ CORREÇÃO #7: Verificar se já enviou QUALQUER tip para este evento
                event_base_key = f"{event_id}_ANY"
                if event_base_key in sent_match_ids:
                    continue

                # ✅ CORREÇÃO #1: Verificar se a liga está aprovada
                approved, reason = is_league_approved(mapped_league)
                if not approved:
                    print(f"[SKIP] {reason}")
                    continue

                # Buscar mercados abertos
                open_lines = fetch_event_markets(event_id)
                if not open_lines:
                    print(f"[INFO] Evento {event_id}: Sem mercados abertos.")
                    continue

                home_data = fetch_player_individual_stats(home_player)
                away_data = fetch_player_individual_stats(away_player)

                if not home_data or not away_data:
                    print(f"[WARN] Sem dados para {home_player} ou {away_player}")
                    continue

                home_matches = home_data.get('matches', [])
                away_matches = away_data.get('matches', [])

                print(f"[INFO] {home_player} ({len(home_matches)} jogos) vs {away_player} ({len(away_matches)} jogos)")

                # ✅ CORREÇÃO #4: Exigir mínimo de 5 jogos (era 4)
                if len(home_matches) < 5 or len(away_matches) < 5:
                    print(f"[WARN] Dados insuficientes (mínimo: 5)")
                    continue

                home_stats = analyze_player_with_regime_check(home_matches, home_player)
                away_stats = analyze_player_with_regime_check(away_matches, away_player)

                if not home_stats or not away_stats:
                    print(f"[WARN] Falha na análise (possível regime change)")
                    continue

                home_confidence = home_stats.get('confidence', 0)
                away_confidence = away_stats.get('confidence', 0)
                avg_confidence = (home_confidence + away_confidence) / 2

                # ✅ CORREÇÃO #5: Aumentar o threshold de confidence mínimo para 80%
                if avg_confidence < 80:
                    print(f"[BLOCKED] Confidence {avg_confidence:.0f}% < 80% mínimo")
                    continue

                # ✅ GATE EXTRA: Verificar se algum jogador tem média muito baixa para a liga
                league_profile = LEAGUE_PROFILES.get(mapped_league, LEAGUE_PROFILES["DEFAULT"])
                min_avg = league_profile['min_player_avg']
                if home_stats['avg_goals_scored_ft'] < min_avg and away_stats['avg_goals_scored_ft'] < min_avg:
                    print(f"[BLOCKED] Ambos jogadores com média abaixo do mínimo da liga ({min_avg})")
                    continue

                print(f"[STATS] {home_player}: avg={home_stats['avg_goals_scored_ft']:.1f} σ={home_stats.get('goals_stdev',0):.1f} conf={home_confidence:.0f}%")
                print(f"[STATS] {away_player}: avg={away_stats['avg_goals_scored_ft']:.1f} σ={away_stats.get('goals_stdev',0):.1f} conf={away_confidence:.0f}%")

                all_league_stats = league_stats if mapped_league in league_stats else {}

                strategies = evaluate_open_lines(event, home_stats, away_stats, all_league_stats, open_lines, avg_confidence)

                for strat_obj in strategies:
                    strategy_name = strat_obj['name']
                    obs_odd = strat_obj['odd']
                    print(f"[✓] OPORTUNIDADE: {strategy_name} (Odd: {obs_odd}) | Conf: {avg_confidence:.0f}%")
                    await send_tip(bot, event, strategy_name, obs_odd, home_stats, away_stats)
                    await asyncio.sleep(1)

            print("[INFO] Ciclo concluído. Aguardando 10s...")
            save_state()
            await asyncio.sleep(10)

        except Exception as e:
            print(f"[ERROR] main_loop: {e}")
            await asyncio.sleep(10)


async def results_checker(bot):
    print("[INFO] Iniciando verificador de resultados...")
    await asyncio.sleep(30)

    while True:
        try:
            await check_results(bot)
            await asyncio.sleep(120)
        except Exception as e:
            print(f"[ERROR] results_checker: {e}")
            await asyncio.sleep(120)


# =============================================================================
# INICIALIZAÇÃO
# =============================================================================

async def main():
    print("=" * 70)
    print("🤖 RW TIPS - BOT FIFA v4.0 (LIGAS DINÂMICAS)")
    print("=" * 70)
    print(f"Horário: {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')} (Manaus)")
    print("=" * 70)
    print("\n📋 CORREÇÕES ATIVAS:")
    print("  ✅ #1 - Whitelist de ligas aprovadas (bloqueio de La Liga, INT, Volta PL)")
    print("  ✅ #2 - Perfis por liga (thresholds calibrados por média histórica)")
    print("  ✅ #3 - Stop-loss por jogador (cooldown após 3 reds consecutivos)")
    print("  ✅ #4 - Janela de 10 jogos (era 5) e mínimo 5 jogos (era 4)")
    print("  ✅ #5 - Confidence mínimo 80% (era 75%) com penalização por variância")
    print("  ✅ #6 - Apostas individuais usam % individual (não total do jogo)")
    print("  ✅ #7 - Máximo 1 tip por evento (seleção do melhor candidato)")
    print("=" * 70)

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
            print(f"[INFO] Conectando ao Telegram (tentativa {attempt + 1}/{max_retries})...")
            me = await bot.get_me()
            print(f"[✓] Bot conectado: @{me.username}")
            break
        except Exception as e:
            print(f"[ERROR] Tentativa {attempt + 1} falhou: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"[INFO] Aguardando {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                print("[ERROR] Não foi possível conectar ao Telegram")
                return

    load_state()

    print("[INFO] Pré-carregando dados...")
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