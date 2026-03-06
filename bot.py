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
LEAGUE_MIN_SAMPLES      = 8    # Mínimo de amostras antes de decidir bloqueio estatístico
LEAGUE_CONSECUTIVE_REDS = 2    # Reds consecutivos para pausar liga temporariamente (mesmo sem amostras suficientes)
LEAGUE_PAUSE_MINUTES    = 30   # Minutos de pausa após reds consecutivos

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
            initial = LEAGUE_INITIAL_STATUS.get(league, True)
            self.leagues[league] = {
                "active": initial,
                "window": deque(maxlen=LEAGUE_WINDOW_SIZE),
                "total_tips": 0,
                "consecutive_reds": 0,
                "pause_until": None,
            }
            status_str = "ATIVA" if initial else "BLOQUEADA"
            print(f"[LeagueManager] Nova liga: {league} → {status_str} (inicial)")
        info = self.leagues[league]
        if "consecutive_reds" not in info: info["consecutive_reds"] = 0
        if "pause_until" not in info: info["pause_until"] = None
        return info

    def is_active(self, league):
        """
        Retorna (bool_ativa, motivo).
        Verifica pausa temporaria por reds consecutivos antes da janela estatistica.
        """
        info = self._get_or_init(league)

        # Verificar pausa temporaria por reds consecutivos
        if info.get('pause_until'):
            pause_until = info['pause_until']
            if isinstance(pause_until, str):
                pause_until = datetime.fromisoformat(pause_until)
            now = datetime.now(MANAUS_TZ)
            if pause_until.tzinfo is None:
                pause_until = pause_until.replace(tzinfo=MANAUS_TZ)
            if now < pause_until:
                mins_left = int((pause_until - now).total_seconds() / 60)
                return False, f'Pausada {LEAGUE_CONSECUTIVE_REDS} reds seguidos — retorna em {mins_left}min'
            else:
                info['pause_until'] = None
                info['consecutive_reds'] = 0
                print(f'[LeagueManager] Pausa encerrada: {league} reativada')

        window = info['window']
        n = len(window)
        if n < LEAGUE_MIN_SAMPLES:
            motivo = f'Coletando dados ({n}/{LEAGUE_MIN_SAMPLES} tips)'
            return info['active'], motivo
        pct = (sum(window) / n) * 100
        return info['active'], f'{pct:.1f}% nas ultimas {n} tips'

    def record_result(self, league, result_is_green):
        """
        Registra resultado e verifica se deve mudar status da liga.
        Retorna (mudou_status, novo_status, mensagem_telegram_ou_None).
        """
        info = self._get_or_init(league)
        info['window'].append(1 if result_is_green else 0)
        info['total_tips'] += 1

        # Rastrear reds consecutivos para pausa imediata
        if result_is_green:
            info['consecutive_reds'] = 0
        else:
            info['consecutive_reds'] = info.get('consecutive_reds', 0) + 1

        # Pausa temporaria apos N reds consecutivos
        if info['consecutive_reds'] >= LEAGUE_CONSECUTIVE_REDS:
            pause_until = datetime.now(MANAUS_TZ) + timedelta(minutes=LEAGUE_PAUSE_MINUTES)
            info['pause_until'] = pause_until.isoformat()
            info['consecutive_reds'] = 0
            self.save()
            msg_pausa = (
                f'⏸ <b>LIGA PAUSADA: {league}</b>\n'
                f'{LEAGUE_CONSECUTIVE_REDS} reds consecutivos\n'
                f'Tips suspensas por {LEAGUE_PAUSE_MINUTES} minutos'
            )
            print(f'[LeagueManager] PAUSADA: {league} ({LEAGUE_CONSECUTIVE_REDS} reds seguidos)')
            return True, False, msg_pausa

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

    def register_league(self, league):
        """
        Registra uma liga ao vivo mesmo sem tips ainda.
        Chamado sempre que o bot encontra um evento, garantindo que
        TODAS as ligas apareçam no relatório desde o primeiro ciclo.
        """
        self._get_or_init(league)
        self.save()

    def get_status_report(self):
        """Relatório completo de TODAS as ligas conhecidas, com ou sem tips."""
        if not self.leagues:
            return "📊 <b>STATUS DAS LIGAS</b>\n\nNenhuma liga registrada ainda."

        ativas     = {k: v for k, v in self.leagues.items() if v['active']}
        bloqueadas = {k: v for k, v in self.leagues.items() if not v['active']}

        lines = [
            "📊 <b>STATUS DAS LIGAS (Dinâmico)</b>",
            f"<i>🟢 Ativas: {len(ativas)} | 🔴 Bloqueadas: {len(bloqueadas)} | Total: {len(self.leagues)}</i>\n"
        ]

        for titulo, grupo in [("🟢 ATIVAS", ativas), ("🔴 BLOQUEADAS", bloqueadas)]:
            if not grupo:
                continue
            lines.append(f"<b>{titulo}</b>")
            for league, info in sorted(grupo.items()):
                window = info['window']
                n      = len(window)
                total  = info['total_tips']
                if n >= LEAGUE_MIN_SAMPLES:
                    pct        = (sum(window) / n) * 100
                    bar_filled = int(pct / 10)
                    bar        = "█" * bar_filled + "░" * (10 - bar_filled)
                    stats_str  = f"{pct:.0f}% [{bar}] (últ. {n} tips)"
                elif n > 0:
                    stats_str  = f"coletando... ({n}/{LEAGUE_MIN_SAMPLES} amostras)"
                else:
                    stats_str  = "sem tips ainda"
                emoji = "🟢" if info['active'] else "🔴"
                lines.append(f"  {emoji} <b>{league}</b>: {stats_str} | {total} total")
            lines.append("")

        lines.append(
            f"<i>Bloqueia &lt;{LEAGUE_RELOCK_THRESHOLD}% | "
            f"Desbloqueia &gt;{LEAGUE_UNLOCK_THRESHOLD}% "
            f"(janela {LEAGUE_WINDOW_SIZE} tips)</i>"
        )
        return "\n".join(lines)


# Instância global
league_manager = LeagueManager()

# =============================================================================
# MAPEAMENTO DE LIGAS
# =============================================================================

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
    "Champions Cyber League": "CLA 10 MIN",
    "Cyber League": "CLA 10 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "ESportsBattle. Premier League (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)": "VOLTA 6 MIN",
    "International": "INT 8 MIN",
    # Ligas E-battles independentes (Bundesliga, Europa League, Super Lig, Premier League, etc.)
    # NÃO mapeadas — mantêm seu nome original e são gerenciadas pelo LeagueManager dinamicamente
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
    # Ligas com formato definido — mapeadas para o perfil correto
    "Champions League B 2×6": "GT LEAGUE 12 MIN",
    "Champions League B 2x6": "GT LEAGUE 12 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)": "VOLTA 6 MIN",
    "International": "INT 8 MIN",
    "Champions Cyber League": "CLA 10 MIN",
    "Cyber League": "CLA 10 MIN",
    "ESportsBattle. Premier League (2x4 mins)": "BATTLE 8 MIN",
    # Ligas E-battles independentes — mantêm o próprio nome (gerenciadas dinamicamente)
    # "Bundesliga", "Europa League", "Premier League", "Super Lig", etc.
    # NÃO mapeadas aqui para preservar identidade e estatísticas próprias
}

# =============================================================================
# ✅ CORREÇÃO #2: THRESHOLDS POR LIGA
# =============================================================================
LEAGUE_PROFILES = {
    "BATTLE 8 MIN":    {"avg_total": 5.5, "avg_player": 2.5, "min_player_avg": 2.0, "duration_min": 8},
    "VALHALLA CUP":    {"avg_total": 7.0, "avg_player": 3.5, "min_player_avg": 3.0, "duration_min": 12},
    "VALKYRIE CUP":    {"avg_total": 5.0, "avg_player": 2.5, "min_player_avg": 2.0, "duration_min": 8},
    "GT LEAGUE 12 MIN":{"avg_total": 6.0, "avg_player": 3.0, "min_player_avg": 2.5, "duration_min": 12},
    "H2H 8 MIN":       {"avg_total": 4.0, "avg_player": 2.0, "min_player_avg": 1.5, "duration_min": 8},
    "CLA 10 MIN":      {"avg_total": 5.0, "avg_player": 2.5, "min_player_avg": 2.0, "duration_min": 10},
    "VOLTA 6 MIN":     {"avg_total": 4.0, "avg_player": 2.0, "min_player_avg": 1.5, "duration_min": 6},
    "INT 8 MIN":       {"avg_total": 5.0, "avg_player": 2.5, "min_player_avg": 2.0, "duration_min": 8},
    "DEFAULT":         {"avg_total": 4.0, "avg_player": 2.0, "min_player_avg": 1.5, "duration_min": 8},
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
last_league_message_id = None
league_stats = {}
last_league_update_time = 0

daily_stats = {}
last_daily_message_date = None

# ✅ CORREÇÃO #3: Stop-loss por jogador
player_red_streak = {}
PLAYER_RED_STREAK_BLOCK = 3
PLAYER_RED_COOLDOWN_MIN = 30


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
    return league_manager.is_active(mapped_league)


def is_player_on_cooldown(player_nick):
    nick = player_nick.upper().strip()
    if nick not in player_red_streak:
        return False, 0
    streak_data = player_red_streak[nick]
    if streak_data['reds'] < PLAYER_RED_STREAK_BLOCK:
        return False, 0
    elapsed = (datetime.now(MANAUS_TZ) - streak_data['last_red_time']).total_seconds() / 60
    if elapsed < PLAYER_RED_COOLDOWN_MIN:
        return True, PLAYER_RED_COOLDOWN_MIN - elapsed
    player_red_streak[nick] = {'reds': 0, 'last_red_time': None}
    return False, 0


def update_player_red_streak(player_nick, result):
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

# =============================================================================
# ✅ FILTRO DE QUALIDADE DA LIGA — Cenários favoráveis por tipo
# =============================================================================

SHORT_LEAGUES = {"BATTLE 8 MIN", "H2H 8 MIN", "VOLTA 6 MIN", "VALKYRIE CUP"}
LONG_LEAGUES  = {"GT LEAGUE 12 MIN", "CLA 10 MIN", "VALHALLA CUP"}  # VALHALLA = 2x6 = 12 min

LEAGUE_QUALITY_CRITERIA = {
    "SHORT": {
        "ht_over_05":    100,
        "ht_over_15":     90,
        "ht_over_25":     85,
        "btts_ht":        88,
        "zero_zero_ht":    0,
        "ft_over_25":    100,
        "ft_over_35":     88,
        "ft_over_45":     80,
        "btts_ft":       100,
        "zero_zero_ft":    0,
        "avg_ht_min":     91,
        "avg_ft_min":     92,
    },
    "LONG": {
        "ht_over_05":    100,
        "ht_over_15":    100,
        "ht_over_25":     93,
        "btts_ht":        88,
        "zero_zero_ht":    0,
        "ft_over_25":    100,
        "ft_over_35":    100,
        "ft_over_45":     94,
        "btts_ft":        95,
        "zero_zero_ft":    0,
        "avg_ht_min":     95,
        "avg_ft_min":     97,
    },
}


def check_league_quality(league_key, last_n_matches):
    """
    Verifica se a liga está em condição favorável para receber tips.
    Analisa os últimos 5 jogos da liga.

    LÓGICA: Critérios OBRIGATÓRIOS + Critérios SECUNDÁRIOS ponderados.

    OBRIGATÓRIOS (todos precisam passar — qualquer falha bloqueia):
      - zero_zero_ht = 0%  (nenhum jogo sem gols no HT)
      - zero_zero_ft = 0%  (nenhum jogo sem gols no FT)
      - ht_over_05  = 100% (SHORT) / 100% (LONG)
      - ft_over_25  = 100% (SHORT) / 100% (LONG)
      - btts_ft     = 100% (SHORT) / 95%  (LONG)

    SECUNDÁRIOS (score ponderado >= 80% do total):
      - ht_over_15, ht_over_25, btts_ht
      - ft_over_35, ft_over_45
      - avg_ht_min, avg_ft_min

    Retorna: (aprovada: bool, score: float, detalhes: dict)
    """
    MIN_MATCHES = 5
    SECONDARY_THRESHOLD = 80  # % dos critérios secundários ponderados

    if not last_n_matches or len(last_n_matches) < MIN_MATCHES:
        n_found = len(last_n_matches) if last_n_matches else 0
        return True, 100, {
            "passed": [f"⚠️ Histórico insuficiente ({n_found}/{MIN_MATCHES}) — aprovado por falta de dados"],
            "failed": [],
            "tipo": "SEM DADOS",
            "score": 100
        }

    last_n = last_n_matches[:MIN_MATCHES]
    n = len(last_n)

    def pct(cond):
        return sum(1 for m in last_n if cond(m)) / n * 100

    ht05    = pct(lambda m: (m.get("home_score_ht",0) + m.get("away_score_ht",0)) >= 1)
    ht15    = pct(lambda m: (m.get("home_score_ht",0) + m.get("away_score_ht",0)) >= 2)
    ht25    = pct(lambda m: (m.get("home_score_ht",0) + m.get("away_score_ht",0)) >= 3)
    btts_ht = pct(lambda m: m.get("home_score_ht",0) > 0 and m.get("away_score_ht",0) > 0)
    zz_ht   = pct(lambda m: m.get("home_score_ht",0) == 0 and m.get("away_score_ht",0) == 0)

    ft25    = pct(lambda m: (m.get("home_score_ft",0) + m.get("away_score_ft",0)) >= 3)
    ft35    = pct(lambda m: (m.get("home_score_ft",0) + m.get("away_score_ft",0)) >= 4)
    ft45    = pct(lambda m: (m.get("home_score_ft",0) + m.get("away_score_ft",0)) >= 5)
    btts_ft = pct(lambda m: m.get("home_score_ft",0) > 0 and m.get("away_score_ft",0) > 0)
    zz_ft   = pct(lambda m: m.get("home_score_ft",0) == 0 and m.get("away_score_ft",0) == 0)

    avg_ht = (ht05 + ht15 + ht25 + btts_ht) / 4
    avg_ft = (ft25 + ft35 + ft45 + btts_ft) / 4

    if league_key in SHORT_LEAGUES:
        crit = LEAGUE_QUALITY_CRITERIA["SHORT"]
        tipo = "SHORT (6-8 min)"
    elif league_key in LONG_LEAGUES:
        crit = LEAGUE_QUALITY_CRITERIA["LONG"]
        tipo = "LONG (10-12 min)"
    else:
        crit = LEAGUE_QUALITY_CRITERIA["SHORT"]
        tipo = "SHORT (padrão)"

    real_values = {
        "ht_over_05": ht05, "ht_over_15": ht15, "ht_over_25": ht25,
        "btts_ht": btts_ht, "zero_zero_ht": zz_ht,
        "ft_over_25": ft25, "ft_over_35": ft35, "ft_over_45": ft45,
        "btts_ft": btts_ft, "zero_zero_ft": zz_ft,
        "avg_ht_min": avg_ht, "avg_ft_min": avg_ft,
    }
    labels = {
        "ht_over_05": "+0.5 HT", "ht_over_15": "+1.5 HT", "ht_over_25": "+2.5 HT",
        "btts_ht": "BTTS HT", "zero_zero_ht": "0x0 HT",
        "ft_over_25": "+2.5 FT", "ft_over_35": "+3.5 FT", "ft_over_45": "+4.5 FT",
        "btts_ft": "BTTS FT", "zero_zero_ft": "0x0 FT",
        "avg_ht_min": "Média HT", "avg_ft_min": "Média FT",
    }

    # Critérios OBRIGATÓRIOS — qualquer falha bloqueia imediatamente
    mandatory_keys = {"zero_zero_ht", "zero_zero_ft", "ht_over_05", "ft_over_25", "btts_ft"}

    # Critérios SECUNDÁRIOS FT — determinam aprovação (score >= 80%)
    ft_secondary_weights = {
        "ft_over_35": 3, "ft_over_45": 3, "avg_ft_min": 2,
    }

    # Critérios SECUNDÁRIOS HT — apenas informativo, NÃO bloqueia
    # (HT tem variância natural em ligas longas — 6 min de 1º tempo)
    ht_secondary_weights = {
        "ht_over_15": 2, "ht_over_25": 2, "btts_ht": 2, "avg_ht_min": 1,
    }

    passed, failed = [], []
    mandatory_ok = True

    # Verificar obrigatórios
    for key in mandatory_keys:
        threshold = crit.get(key, 0)
        real      = real_values[key]
        is_zz     = key.startswith("zero_zero")
        ok        = (real == 0) if is_zz else (real >= threshold)
        label     = labels[key]
        if ok:
            passed.append(f"✅ {label}: {real:.0f}%")
        else:
            diff = real if is_zz else (threshold - real)
            failed.append(f"❌ OBRIG. {label}: {real:.0f}% (faltam {diff:.0f}pp)")
            mandatory_ok = False

    # Verificar secundários FT (DETERMINANTES)
    ft_total_w  = 0
    ft_passed_w = 0
    for key, w in ft_secondary_weights.items():
        threshold = crit.get(key, 0)
        real      = real_values[key]
        ok        = real >= threshold
        label     = labels[key]
        ft_total_w += w
        if ok:
            ft_passed_w += w
            passed.append(f"✅ {label}: {real:.0f}%")
        else:
            diff = threshold - real
            failed.append(f"❌ {label}: {real:.0f}% (faltam {diff:.0f}pp)")

    ft_sec_score = (ft_passed_w / ft_total_w * 100) if ft_total_w > 0 else 0

    # Verificar secundários HT (INFORMATIVOS — não bloqueiam)
    ht_total_w  = 0
    ht_passed_w = 0
    for key, w in ht_secondary_weights.items():
        threshold = crit.get(key, 0)
        real      = real_values[key]
        ok        = real >= threshold
        label     = labels[key]
        ht_total_w += w
        if ok:
            ht_passed_w += w
            passed.append(f"✅ {label}: {real:.0f}%")
        else:
            diff = threshold - real
            failed.append(f"⚠️ HT {label}: {real:.0f}% (faltam {diff:.0f}pp)")

    ht_sec_score = (ht_passed_w / ht_total_w * 100) if ht_total_w > 0 else 0

    # Score geral: obrigatórios 50% + FT secundário 40% + HT secundário 10%
    score = (100 if mandatory_ok else 0) * 0.5 + ft_sec_score * 0.4 + ht_sec_score * 0.1

    # APROVAÇÃO: obrigatórios todos passam + FT secundário >= 80%
    # HT secundário NÃO bloqueia (apenas informa qualidade de apostas HT)
    aprovada = mandatory_ok and ft_sec_score >= SECONDARY_THRESHOLD

    return aprovada, score, {
        "tipo": tipo, "passed": passed, "failed": failed, "score": score,
        "avg_ht": avg_ht, "avg_ft": avg_ft, "zz_ht": zz_ht, "zz_ft": zz_ft,
    }



def get_league_last5(league_key, all_matches):
    """Retorna os últimos 8 jogos de uma liga específica do histórico global."""
    filtered = [
        m for m in all_matches
        if m.get('league_name', '') == league_key
        or map_league_name(m.get('league_name', '')) == league_key
    ]
    return filtered[:8]  # ✅ FIX #2: 8 jogos (era 5) — mais estabilidade estatística



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
    """
    Retorna os gols TOTAIS (jogo completo) do jogador.

    CONFIRMADO via multiplos casos reais:
    score_ft = total do jogo completo (inclui o HT)
    score_ht = gols so do 1o tempo

    Exemplos:
      Florie: ht=2, ft=2 -> marcou 2 no HT, 0 no 2T -> total=2
      Olive:  ht=3, ft=4 -> marcou 3 no HT, 1 no 2T -> total=4
      Bradley:ht=1, ft=4 -> marcou 1 no HT, 3 no 2T -> total=4

    NUNCA somar ht + ft.
    """
    if not player_nick or not match:
        return 0
    p_nick = player_nick.upper().strip()
    home_nick = extract_pure_nick(match.get("home_player") or match.get("home_team") or "")
    away_nick = extract_pure_nick(match.get("away_player") or match.get("away_team") or "")
    if p_nick == home_nick:
        return int(match.get("home_score_ft", 0) or 0)
    if p_nick == away_nick:
        return int(match.get("away_score_ft", 0) or 0)
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
            # home_score_ft pode ser apenas o 2T em algumas ligas.
            # Para garantir, comparamos com o total acumulado (ht + ft).
            # Se ht+ft >= placar enviado, o jogo é candidato válido.
            m_ht_h = int(m.get('home_score_ht', 0) or 0)
            m_ht_a = int(m.get('away_score_ht', 0) or 0)
            final_h = m_ht_h + int(m.get('home_score_ft', 0) or 0)
            final_a = m_ht_a + int(m.get('away_score_ft', 0) or 0)
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

                if '(' in home_competitor_name:
                    # "Man City (fantazer)" → extrai "FANTAZER" → normaliza capitalização
                    home_player = extract_pure_nick(home_competitor_name).capitalize()
                    home_source = "Full-Name-Parentheses"
                else:
                    home_player = clean_player_name(check_home_player or home_competitor_name)
                    home_source = "API-Field" if check_home_player else "Full-Name-Cleaning"

                name_from_full_away = clean_player_name(away_competitor_name)
                check_away_player = (away_comp.get('playerName') or away_comp.get('player_name') or
                                     away_comp.get('player') or away_comp.get('nickName') or event.get('away_player'))
                away_raw_text = away_competitor_name if '(' in away_competitor_name else (check_away_player or away_competitor_name)

                if '(' in away_competitor_name:
                    away_player = extract_pure_nick(away_competitor_name).capitalize()
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
    """
    Analisa histórico do jogador.
    Usa janela de 10 jogos para métricas gerais
    e janela de 5 jogos para forma recente (mais sensível).
    """
    if not matches:
        return None

    actual_window = min(len(matches), window)
    if actual_window < 5:
        return None

    study_set    = matches[:actual_window]
    recent_5     = matches[:5]   # Janela curta para forma recente

    def calc_set(match_set, pname):
        n = len(match_set)
        d = {
            'total_scored_ft': 0, 'total_conceded_ft': 0,
            'total_scored_ht': 0, 'total_conceded_ht': 0,
            'wins': 0, 'draws': 0, 'losses': 0,
            'goals_list_ft': [], 'goals_list_ht': [],
            'ht_scored_05': 0, 'ht_scored_15': 0,
            'ft_scored_05': 0, 'ft_scored_15': 0,
            'ft_scored_25': 0, 'ft_scored_35': 0,
            'ht_over_05': 0, 'ht_over_15': 0, 'ht_over_25': 0,
            'ft_over_05': 0, 'ft_over_15': 0, 'ft_over_25': 0,
            'ft_over_35': 0, 'ft_over_45': 0,
            'btts_count': 0, 'ht_btts_count': 0,
        }
        for m in match_set:
            is_home = m.get('home_player', '').upper() == pname.upper()
            ht_h = m.get('home_score_ht', 0) or 0
            ht_a = m.get('away_score_ht', 0) or 0
            ft_h = m.get('home_score_ft', 0) or 0
            ft_a = m.get('away_score_ft', 0) or 0

            pg  = ft_h if is_home else ft_a   # gols marcados FT
            pc  = ft_a if is_home else ft_h   # gols sofridos FT
            pgh = ht_h if is_home else ht_a   # gols marcados HT
            pch = ht_a if is_home else ht_h   # gols sofridos HT

            d['goals_list_ft'].append(pg)
            d['goals_list_ht'].append(pgh)
            d['total_scored_ft']   += pg
            d['total_conceded_ft'] += pc
            d['total_scored_ht']   += pgh
            d['total_conceded_ht'] += pch

            if pg > pc: d['wins']   += 1
            elif pg == pc: d['draws'] += 1
            else: d['losses'] += 1

            if pg > 0: d['ft_scored_05'] += 1
            if pg > 1: d['ft_scored_15'] += 1
            if pg > 2: d['ft_scored_25'] += 1
            if pg > 3: d['ft_scored_35'] += 1
            if pgh > 0: d['ht_scored_05'] += 1
            if pgh > 1: d['ht_scored_15'] += 1

            ht_t = ht_h + ht_a
            ft_t = ft_h + ft_a
            if ht_t > 0: d['ht_over_05'] += 1
            if ht_t > 1: d['ht_over_15'] += 1
            if ht_t > 2: d['ht_over_25'] += 1
            if ft_t > 0: d['ft_over_05'] += 1
            if ft_t > 1: d['ft_over_15'] += 1
            if ft_t > 2: d['ft_over_25'] += 1
            if ft_t > 3: d['ft_over_35'] += 1
            if ft_t > 4: d['ft_over_45'] += 1
            if ft_h > 0 and ft_a > 0: d['btts_count']    += 1
            if ht_h > 0 and ht_a > 0: d['ht_btts_count'] += 1
        return d, n

    d10, n10 = calc_set(study_set, player_name)
    d5,  n5  = calc_set(recent_5,  player_name)

    try:
        stdev_ft = statistics.stdev(d10['goals_list_ft']) if len(d10['goals_list_ft']) >= 2 else 0
    except:
        stdev_ft = 0

    # Forma recente: sequência dos últimos 5 (W=2, D=1, L=0)
    forma_pts = 0
    for m in recent_5:
        is_home = m.get('home_player', '').upper() == player_name.upper()
        pg = (m.get('home_score_ft', 0) or 0) if is_home else (m.get('away_score_ft', 0) or 0)
        pc = (m.get('away_score_ft', 0) or 0) if is_home else (m.get('home_score_ft', 0) or 0)
        if pg > pc:   forma_pts += 2
        elif pg == pc: forma_pts += 1
    forma_pct = (forma_pts / 10) * 100  # 0-100%

    return {
        # Médias gerais (10j)
        'avg_goals_scored_ft':   d10['total_scored_ft']   / n10,
        'avg_goals_conceded_ft': d10['total_conceded_ft'] / n10,
        'avg_goals_scored_ht':   d10['total_scored_ht']   / n10,
        'avg_goals_conceded_ht': d10['total_conceded_ht'] / n10,
        'win_pct':  (d10['wins']   / n10) * 100,
        'draw_pct': (d10['draws']  / n10) * 100,
        'loss_pct': (d10['losses'] / n10) * 100,
        # Médias recentes (5j) — para o novo confidence
        'avg_scored_ft_5j':   d5['total_scored_ft']   / n5,
        'avg_conceded_ft_5j': d5['total_conceded_ft'] / n5,
        'avg_scored_ht_5j':   d5['total_scored_ht']   / n5,
        'avg_conceded_ht_5j': d5['total_conceded_ht'] / n5,
        'win_pct_5j': (d5['wins'] / n5) * 100,
        # Percentuais (10j)
        'ht_over_05_pct':   (d10['ht_over_05'] / n10) * 100,
        'ht_over_15_pct':   (d10['ht_over_15'] / n10) * 100,
        'ht_over_25_pct':   (d10['ht_over_25'] / n10) * 100,
        'ht_scored_05_pct': (d10['ht_scored_05'] / n10) * 100,
        'ht_scored_15_pct': (d10['ht_scored_15'] / n10) * 100,
        'ft_over_05_pct':   (d10['ft_over_05'] / n10) * 100,
        'ft_over_15_pct':   (d10['ft_over_15'] / n10) * 100,
        'ft_over_25_pct':   (d10['ft_over_25'] / n10) * 100,
        'ft_over_35_pct':   (d10['ft_over_35'] / n10) * 100,
        'ft_over_45_pct':   (d10['ft_over_45'] / n10) * 100,
        'ft_scored_05_pct': (d10['ft_scored_05'] / n10) * 100,
        'ft_scored_15_pct': (d10['ft_scored_15'] / n10) * 100,
        'ft_scored_25_pct': (d10['ft_scored_25'] / n10) * 100,
        'ft_scored_35_pct': (d10['ft_scored_35'] / n10) * 100,
        'btts_pct':    (d10['btts_count']    / n10) * 100,
        'ht_btts_pct': (d10['ht_btts_count'] / n10) * 100,
        'consistency_ft_3_plus_pct': (d10['ft_over_25'] / n10) * 100,
        'consistency_ht_1_plus_pct': (d10['ht_over_05'] / n10) * 100,
        # Stdev e listas
        'goals_stdev': stdev_ft,
        'goals_list':    d10['goals_list_ft'],
        'goals_list_ht': d10['goals_list_ht'],
        'games_analyzed': n10,
        # Forma recente (0-100%)
        'forma_recente_pct': forma_pct,
    }


def get_h2h_stats(player1, player2):
    """
    Retorna estatísticas dos confrontos diretos entre player1 e player2.
    Usa os 5 últimos H2H disponíveis.
    Retorna None se menos de 3 confrontos (não penaliza por falta de dados).
    """
    all_matches = global_history_cache.get('matches', [])
    if not all_matches:
        return None

    p1 = player1.upper()
    p2 = player2.upper()
    h2h_list = [
        m for m in all_matches
        if (m.get('home_player', '').upper() == p1 and m.get('away_player', '').upper() == p2)
        or (m.get('home_player', '').upper() == p2 and m.get('away_player', '').upper() == p1)
    ]

    if len(h2h_list) < 3:
        return None   # Dados insuficientes — critério H2H não será aplicado

    h2h_list = h2h_list[:5]
    stats = analyze_player_history(h2h_list, player1, window=5)
    if stats:
        stats['count'] = len(h2h_list)
    return stats


def detect_regime_change(matches):
    """
    Detecta se o jogador está em queda (COOLING) ou em alta (HEATING)
    comparando os últimos 3 jogos com os 4-10 anteriores.
    """
    if len(matches) < 6:
        return {'regime_change': False}

    last_3     = matches[:3]
    previous   = matches[3:10] if len(matches) >= 10 else matches[3:]

    def avg_scored(window):
        if not window: return 0
        return sum(m.get('home_score_ft', 0) or 0 for m in window) / len(window)

    avg_r = avg_scored(last_3)
    avg_p = avg_scored(previous)

    if avg_p > 0:
        ratio = avg_r / avg_p
        if ratio < 0.45 and avg_r < 1.2:
            return {'regime_change': True, 'direction': 'COOLING', 'action': 'AVOID',
                    'reason': f'Esfriou: {avg_r:.1f} vs {avg_p:.1f}'}
        if ratio > 1.8 and avg_r > 2.0:
            return {'regime_change': True, 'direction': 'HEATING', 'action': 'BOOST',
                    'reason': f'Em alta: {avg_r:.1f} vs {avg_p:.1f}'}

    return {'regime_change': False}


def analyze_player_with_regime_check(matches, player_name):
    """
    Análise completa do jogador: histórico + regime + confidence.
    """
    if not matches:
        return None

    stats = analyze_player_history(matches, player_name, window=10)
    if not stats:
        return None

    regime = detect_regime_change(matches)

    # Bloquear imediatamente se em queda
    if regime['regime_change'] and regime['action'] == 'AVOID':
        print(f"[REGIME] {player_name} em queda. Bloqueando.")
        stats['confidence'] = 0
        return stats

    stats['regime_change']    = regime['regime_change']
    stats['regime_direction'] = regime.get('direction', 'STABLE')
    stats['regime_action']    = regime.get('action', '')

    return stats


def calculate_confidence_score(home_stats, away_stats, h2h_stats=None):
    """
    Nova lógica de confidence — baseada em métricas dos últimos 5 jogos
    de CADA jogador individualmente, mais confronto direto.

    Pontuação máxima: 100 pts por jogador
    Threshold de aprovação: média dos dois jogadores >= 70%

    FATOR 1 — HT marcado (15 pts, gradual)
    FATOR 2 — HT sofrido (15 pts, gradual)
    FATOR 3 — FT marcado (15 pts, gradual)
    FATOR 4 — FT sofrido (15 pts, gradual)
    FATOR 5 — Win% (10 pts, gradual)
    FATOR 6 — H2H vantagem (20 pts, com fallback)
    FATOR 7 — Forma recente (10 pts, sequência W/D/L)
    """

    def score_player(stats, label=""):
        s = 0
        bd = []

        ht_marc = stats.get('avg_scored_ht_5j', stats.get('avg_goals_scored_ht', 0))
        ht_sofr = stats.get('avg_conceded_ht_5j', stats.get('avg_goals_conceded_ht', 0))
        ft_marc = stats.get('avg_scored_ft_5j', stats.get('avg_goals_scored_ft', 0))
        ft_sofr = stats.get('avg_conceded_ft_5j', stats.get('avg_goals_conceded_ft', 0))
        win_pct = stats.get('win_pct_5j', stats.get('win_pct', 0))
        forma   = stats.get('forma_recente_pct', 50)

        # F1: HT marcado (≥2.5=15, ≥1.8=10, ≥1.2=5, <1.2=0)
        if ht_marc >= 2.5:   pts = 15
        elif ht_marc >= 1.8: pts = 10
        elif ht_marc >= 1.2: pts = 5
        else:                pts = 0
        s += pts
        bd.append(f"HT marc {ht_marc:.1f}→+{pts}")

        # F2: HT sofrido — threshold dinâmico: artilheiros (ht_marc>=1.8) sofrem mais por natureza
        ht_bonus = 0.5 if ht_marc >= 1.8 else 0
        if ht_sofr <= (1.0 + ht_bonus):   pts = 15
        elif ht_sofr <= (1.5 + ht_bonus): pts = 10
        elif ht_sofr <= (2.0 + ht_bonus): pts = 5
        else:                              pts = 0
        s += pts
        bd.append(f"HT sofr {ht_sofr:.1f}→+{pts}")

        # F3: FT marcado (≥3.5=15, ≥2.8=10, ≥2.0=5, <2.0=0)
        if ft_marc >= 3.5:   pts = 15
        elif ft_marc >= 2.8: pts = 10
        elif ft_marc >= 2.0: pts = 5
        else:                pts = 0
        s += pts
        bd.append(f"FT marc {ft_marc:.1f}→+{pts}")

        # F4: FT sofrido — threshold dinâmico por perfil ofensivo
        # Artilheiros (ft_marc>=3.5) sofrem mais por natureza → limiares tolerantes
        if ft_marc >= 3.5:   ft_lims = (3.0, 3.5, 4.5)  # tolerante
        elif ft_marc >= 2.8: ft_lims = (2.5, 3.0, 3.5)  # moderado
        else:                ft_lims = (2.0, 2.5, 3.0)   # padrão
        if ft_sofr <= ft_lims[0]:   pts = 15
        elif ft_sofr <= ft_lims[1]: pts = 10
        elif ft_sofr <= ft_lims[2]: pts = 5
        else:                       pts = 0
        s += pts
        bd.append(f"FT sofr {ft_sofr:.1f}→+{pts}")

        # F5: Win% para jogadores normais / Ratio ofensivo para artilheiros
        # Artilheiro que perde 7-6 ainda é ótimo para apostas de gols
        # Ratio = ft_marc / (ft_marc + ft_sofr) — mede dominância ofensiva
        if ft_marc >= 3.5:
            ratio = ft_marc / (ft_marc + ft_sofr) if (ft_marc + ft_sofr) > 0 else 0
            if ratio >= 0.55:   pts = 10
            elif ratio >= 0.48: pts = 6
            elif ratio >= 0.42: pts = 3
            else:               pts = 0
            bd.append(f"Ratio {ratio:.2f}→+{pts}")
        else:
            if win_pct >= 65:   pts = 10
            elif win_pct >= 55: pts = 6
            elif win_pct >= 45: pts = 3
            else:               pts = 0
            bd.append(f"Win {win_pct:.0f}%→+{pts}")
        s += pts

        # F6: Forma recente (0-10 pts proporcional)
        pts = round((forma / 100) * 10)
        s += pts
        bd.append(f"Forma {forma:.0f}%→+{pts}")

        return min(100, s), bd

    sc_home, bd_home = score_player(home_stats, "home")
    sc_away, bd_away = score_player(away_stats, "away")

    # F6: H2H — aplica ao JOGADOR com vantagem (+20), desvantagem (0)
    # Se sem dados H2H suficientes, ambos recebem +10 (neutro)
    if h2h_stats is not None:
        # h2h_stats é calculado na perspectiva do home_player
        h2h_win = h2h_stats.get('win_pct', 50)
        if h2h_win >= 60:
            sc_home = min(100, sc_home + 20)
            bd_home.append(f"H2H {h2h_win:.0f}%→+20")
            bd_away.append(f"H2H {100-h2h_win:.0f}%→+0")
        elif h2h_win <= 40:
            sc_away = min(100, sc_away + 20)
            bd_home.append(f"H2H {h2h_win:.0f}%→+0")
            bd_away.append(f"H2H {100-h2h_win:.0f}%→+20")
        else:
            # Paridade: +10 para ambos
            sc_home = min(100, sc_home + 10)
            sc_away = min(100, sc_away + 10)
            bd_home.append(f"H2H paridade→+10")
            bd_away.append(f"H2H paridade→+10")
    else:
        # Sem H2H: +10 neutro para ambos (não penaliza)
        sc_home = min(100, sc_home + 10)
        sc_away = min(100, sc_away + 10)

    avg_conf = (sc_home + sc_away) / 2

    return {
        'home_confidence': sc_home,
        'away_confidence': sc_away,
        'avg_confidence':  avg_conf,
        'breakdown_home':  bd_home,
        'breakdown_away':  bd_away,
    }


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

def evaluate_open_lines(event, home_stats, away_stats, all_league_stats, open_lines,
                        avg_confidence,
                        gate_individual=True, gate_total=True,
                        home_confidence=0, away_confidence=0):
    """
    Avaliação com 6 filtros para atingir 75-85% de assertividade:
    1. Gate duplo individual vs total
    2. Projeção de ritmo ao vivo (tempo restante vs gols necessários)
    3. Sem fallback artificial de 75% — sem dados históricos = sem aposta
    4. Odd mínima 1.70
    5. needed reduzido para 1.5 em jogos tardios (>60% do tempo)
    6. Gate de confiança correlacionado com nível da aposta
    """
    candidates = []

    timer   = event.get("timer", {})
    minute  = timer.get("minute", 0)
    second  = timer.get("second", 0)

    league_key    = event.get("mappedLeague", "")
    home_raw      = event.get("homeRaw", "").strip()
    away_raw      = event.get("awayRaw", "").strip()
    home_player   = event.get("homePlayer", "P1")
    away_player   = event.get("awayPlayer", "P2")

    league_profile  = LEAGUE_PROFILES.get(league_key, LEAGUE_PROFILES["DEFAULT"])
    min_player_avg  = league_profile["min_player_avg"]
    duration_min    = league_profile.get("duration_min", 8)

    h2h = get_h2h_stats(home_player, away_player)

    def get_weighted_val(h_val, a_val, h2h_val):
        base = (h_val + a_val) / 2
        if h2h is not None and h2h_val > 0:
            return base * 0.6 + h2h_val * 0.4
        return base

    MAX_HT_TIME    = int(duration_min * 60 * 0.55)
    score          = event.get("score", {})
    hg             = score.get("home", 0)
    ag             = score.get("away", 0)
    total_now      = hg + ag
    pct_elapsed    = minute / max(1, duration_min)
    mins_restantes = max(0.5, duration_min - minute - (second / 60))

    combined_ft_avg      = home_stats["avg_goals_scored_ft"] + away_stats["avg_goals_scored_ft"]
    taxa_historica_jogo  = combined_ft_avg / duration_min  # gols/min esperados para o jogo

    def ritmo_viavel(gols_faltando, margem=1.15):
        if gols_faltando <= 0:
            return True
        taxa_necessaria = gols_faltando / mins_restantes
        ok = taxa_necessaria <= taxa_historica_jogo * margem
        if not ok:
            print(f"[BLOCKED RITMO] precisa {taxa_necessaria:.2f}g/min, histórico {taxa_historica_jogo:.2f}g/min (margem {margem}x)")
        return ok

    def parse_line(sv_str):
        try: return float(sv_str.split("|")[-1])
        except: return None

    def find_over_line(market_name_query):
        best_line = None
        q_lower = market_name_query.lower()
        for line in open_lines:
            m_lower = line["market_name"].lower()
            if q_lower in m_lower:
                if "tempo" not in q_lower and "tempo" in m_lower: continue
                if "mais de" in line["odd_name"].lower() or line["price"] > 0:
                    if "menos" in line["odd_name"].lower(): continue
                    if line["price"] < 1.70: continue
                    sv_val = parse_line(line["odd_sv"])
                    if sv_val is not None:
                        if best_line is None or sv_val < best_line["value"]:
                            best_line = {"value": sv_val, "odd_name": line["odd_name"], "price": line["price"]}
        return best_line

    # =========================================================================
    # HT — Apostas de primeiro tempo
    # Critérios do JOGADOR: ht_scored_05_pct/ht_scored_15_pct/ht_btts_pct
    # =========================================================================
    if gate_total and (minute * 60 + second) <= MAX_HT_TIME and total_now == 0:
        ht_line = find_over_line("1º tempo - total") or find_over_line("1ª tempo - Total de gols")
        if ht_line:
            val = ht_line["value"]
            combined_ht_avg = home_stats["avg_goals_scored_ht"] + away_stats["avg_goals_scored_ht"]
            gols_faltando_ht = max(0, val + 0.5 - total_now)
            ritmo_ok = ritmo_viavel(gols_faltando_ht)
            avg_gate_ok = combined_ht_avg >= (val + 0.5)

            # w_pct baseado em % histórica do JOGO (ambos os jogadores)
            h2h_ht_val = h2h.get(f"ht_over_{str(val).replace('.','')}_pct", 0) if h2h else 0
            w_pct = get_weighted_val(
                home_stats.get(f"ht_over_{str(val).replace('.','')}_pct", 0),
                away_stats.get(f"ht_over_{str(val).replace('.','')}_pct", 0),
                h2h_ht_val
            )
            # BTTS HT médio dos jogadores
            btts_ht_avg = (home_stats.get("ht_btts_pct", 0) + away_stats.get("ht_btts_pct", 0)) / 2

            if val == 0.5 and w_pct >= 100 and btts_ht_avg >= 88 and avg_gate_ok and ritmo_ok:
                candidates.append({"name": "⚽ +0.5 GOL HT",  "odd": ht_line["price"], "score": w_pct * (ht_line["price"] - 1), "confidence_pct": w_pct})
            elif val == 1.5 and w_pct >= 90 and btts_ht_avg >= 88 and avg_gate_ok and ritmo_ok:
                candidates.append({"name": "⚽ +1.5 GOLS HT", "odd": ht_line["price"], "score": w_pct * (ht_line["price"] - 1), "confidence_pct": w_pct})
            elif w_pct > 0:
                print(f"[BLOCKED HT] +{val}: w_pct={w_pct:.0f}% btts_ht={btts_ht_avg:.0f}% avg_gate={'✅' if avg_gate_ok else '❌'} ritmo={'✅' if ritmo_ok else '❌'}")

    # =========================================================================
    # FT TOTAL — Apostas de total do jogo
    # Critérios: ft_over_X5_pct (média dos dois), ritmo, confiança >= 70%
    # =========================================================================
    total_ft_line = find_over_line("Total de Gols") if gate_total else None
    if total_ft_line:
        val    = total_ft_line["value"]
        needed = val - total_now
        needed_limit = 1.5 if pct_elapsed > 0.60 else 2.0
        avg_gate_ok  = combined_ft_avg >= (val * 0.85)
        ritmo_ok     = ritmo_viavel(needed)

        if needed <= needed_limit and avg_gate_ok and ritmo_ok:
            key_str = str(val).replace(".", "")
            h2h_val = h2h.get(f"ft_over_{key_str}_pct", 0) if h2h else 0
            w_pct = get_weighted_val(
                home_stats.get(f"ft_over_{key_str}_pct", 0),
                away_stats.get(f"ft_over_{key_str}_pct", 0),
                h2h_val
            )
            # Thresholds históricos por linha (SHORT/LONG já filtrados na liga)
            threshold_map = {1.5: 92, 2.5: 88, 3.5: 85, 4.5: 83, 5.5: 81}
            min_pct = threshold_map.get(val, 85)
            # Confiança mínima sobe com nível da aposta (todos >= 70%)
            conf_gate_map = {1.5: 70, 2.5: 70, 3.5: 72, 4.5: 75, 5.5: 78}
            min_conf = conf_gate_map.get(val, 70)

            if w_pct >= min_pct and avg_confidence >= min_conf:
                candidates.append({
                    "name": f"⚽ +{val} GOLS FT (TOTAL)",
                    "odd": total_ft_line["price"],
                    "score": w_pct * (total_ft_line["price"] - 1),
                    "confidence_pct": w_pct
                })
            elif w_pct > 0:
                print(f"[BLOCKED FT] +{val}: w_pct={w_pct:.0f}% (min={min_pct}%) conf={avg_confidence:.0f}% (min={min_conf}%)")

    # =========================================================================
    # INDIVIDUAL — Gols de 1 jogador
    # NOVO: avalia MARCADOS do jogador + SOFRIDOS do adversário
    # =========================================================================
    def evaluate_individual_player(player_raw, player_stats, opponent_stats, player_goals_now, player_conf):
        # Gate de confiança global 70%
        if player_conf < 70:
            return None
        avg_individual = player_stats["avg_goals_scored_ft"]
        if avg_individual < min_player_avg:
            print(f"[BLOCKED IND] {player_raw}: média {avg_individual:.1f} < mínimo {min_player_avg}")
            return None
        ind_line = find_over_line(f"{player_raw} total")
        if not ind_line:
            return None
        v         = ind_line["value"]
        needed_v  = v - player_goals_now
        limit_v   = 1.5 if pct_elapsed > 0.60 else 2.0
        if needed_v > limit_v:
            return None
        # Ritmo individual
        taxa_ind = avg_individual / duration_min
        if needed_v > 0:
            taxa_need = needed_v / mins_restantes
            if taxa_need > taxa_ind * 1.20:
                print(f"[BLOCKED IND RITMO] {player_raw}: {taxa_need:.2f}g/min > histórico {taxa_ind:.2f}g/min")
                return None
        key_str    = str(v).replace(".", "")
        player_pct = player_stats.get(f"ft_scored_{key_str}_pct", 0)
        # Sem dados históricos = sem aposta
        if player_pct == 0:
            print(f"[BLOCKED IND PCT] {player_raw}: sem dados para +{v} FT")
            return None
        # Gate de confiança correlacionado com linha
        conf_ind_map = {1.5: 70, 2.5: 72, 3.5: 75, 4.5: 78}
        if player_conf < conf_ind_map.get(v, 72):
            print(f"[BLOCKED IND CONF] {player_raw}: conf={player_conf:.0f}% < {conf_ind_map.get(v,72)}% para +{v}")
            return None
        # Threshold histórico mínimo por linha
        min_pct_ind = {1.5: 85, 2.5: 80, 3.5: 75, 4.5: 70}
        if player_pct < min_pct_ind.get(v, 75):
            return None

        # ── ANÁLISE DE GOLS SOFRIDOS DO ADVERSÁRIO ──────────────────────────
        # Se o adversário concede muitos gols, favorece nossa aposta.
        # Se concede poucos, penaliza.
        # Gols sofridos do adversário = gols_concedidos por jogo (avg_goals_conceded_ft)
        opp_conceded_avg = opponent_stats.get("avg_goals_conceded_ft", 0)
        opp_conceded_5j  = opponent_stats.get("avg_conceded_ft_5j", opp_conceded_avg)

        # Threshold de gols concedidos: adversário precisa conceder >= min_player_avg
        # (se ele é muito fechado, dificilmente nosso jogador vai atingir o threshold)
        if opp_conceded_5j < min_player_avg * 0.8:
            print(f"[BLOCKED IND OPP] {player_raw}: adversário concede apenas {opp_conceded_5j:.1f}g/j (mín. {min_player_avg*0.8:.1f})")
            return None

        # Boost no score se o adversário é muito vazado
        opp_boost = 1.0
        if opp_conceded_5j >= avg_individual * 1.2:
            opp_boost = 1.10  # +10% no score
        elif opp_conceded_5j < avg_individual * 0.8:
            opp_boost = 0.90  # -10% no score (adversário fecha bem)

        score_val = player_pct * (ind_line["price"] - 1) * opp_boost
        print(f"[IND OK] {player_raw} +{v}: marcados={player_pct:.0f}% opp_concede={opp_conceded_5j:.1f}g boost={opp_boost}")
        return {
            "name": f"⚽ {player_raw} +{v} GOLS FT",
            "odd": ind_line["price"],
            "score": score_val,
            "confidence_pct": player_pct
        }

    if gate_individual:
        home_cand = evaluate_individual_player(home_raw, home_stats, away_stats, hg, home_confidence)
        if home_cand: candidates.append(home_cand)
        away_cand = evaluate_individual_player(away_raw, away_stats, home_stats, ag, away_confidence)
        if away_cand: candidates.append(away_cand)

    if not candidates:
        return []

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    print(f"[SELEÇÃO] {best['name']} (score={best['score']:.1f}, conf={best['confidence_pct']:.0f}%)")
    if len(candidates) > 1:
        print(f"[SELEÇÃO] Descartados: {[c['name'] for c in candidates[1:]]}")
    return [{"name": best["name"], "odd": best["odd"]}]



# =============================================================================
# FORMATAÇÃO DE MENSAGENS
# =============================================================================

def make_player_bar(avg_goals, max_goals=6):
    """Gera barra visual de média de gols. Ex: ████████░░ 3.8g/j"""
    filled = min(10, round((avg_goals / max_goals) * 10))
    bar = "█" * filled + "░" * (10 - filled)
    return f"{bar} {avg_goals:.1f}g/j"


def make_confidence_semaphore(confidence):
    """Semáforo de confiança: verde/amarelo/vermelho."""
    if confidence >= 85:
        return "🟢"
    elif confidence >= 75:
        return "🟡"
    else:
        return "🔴"


def format_tip_message(event, strategy, obs_odd, home_stats_summary, away_stats_summary, liga_det=None, avg_confidence=None):
    """
    ✅ FORMATO OTIMIZADO — Decisão em 2 segundos, sem scroll.

    Hierarquia:
      1. Liga + Aposta + Odd  (topo — o que importa)
      2. Semáforo + Placar + Tempo
      3. Barras visuais dos jogadores
      4. Rodapé: liga validada + BTTS + link
    """
    event_id   = event.get('id', '')
    home_player = event.get('homePlayer', '?')
    away_player = event.get('awayPlayer', '?')

    timer      = event.get('timer', {})
    time_str   = timer.get('formatted', '00:00')
    scoreboard = event.get('scoreboard', '0-0')

    # Limpar nome da liga
    league_raw = event.get('leagueName', '')
    mapped     = event.get('mappedLeague', league_raw)
    league_display = mapped if mapped and mapped != 'Unknown' else league_raw

    # Confidence e semáforo — usa valor passado diretamente ou fallback nos stats
    if avg_confidence is not None:
        avg_conf = avg_confidence
    else:
        home_conf = home_stats_summary.get('confidence', 0)
        away_conf = away_stats_summary.get('confidence', 0)
        avg_conf  = (home_conf + away_conf) / 2
    semaphore = make_confidence_semaphore(avg_conf)

    # Barras visuais dos jogadores
    home_avg  = home_stats_summary.get('avg_goals_scored_ft', 0)
    away_avg  = away_stats_summary.get('avg_goals_scored_ft', 0)
    max_g     = max(home_avg, away_avg, 4.0)
    home_bar  = make_player_bar(home_avg, max_g)
    away_bar  = make_player_bar(away_avg, max_g)

    # Padding para alinhar barras
    h_len = len(home_player)
    a_len = len(away_player)
    pad   = max(h_len, a_len)
    home_padded = home_player.ljust(pad)
    away_padded = away_player.ljust(pad)

    # Informações da liga (do filtro de qualidade)
    if liga_det and liga_det.get('score', 0) >= 100:
        liga_status = "Liga ✅"
    elif liga_det:
        pct = liga_det.get('score', 0)
        liga_status = f"Liga {pct:.0f}%"
    else:
        liga_status = "Liga ✅"

    # BTTS médio
    btts = (home_stats_summary.get('btts_pct', 0) + away_stats_summary.get('btts_pct', 0)) / 2

    # HT relevante
    ht_pct = (home_stats_summary.get('ht_over_15_pct', 0) + away_stats_summary.get('ht_over_15_pct', 0)) / 2

    # Link do jogo
    link_str = ""
    if event_id:
        url = f"https://www.estrelabet.bet.br/apostas-ao-vivo?page=liveEvent&eventId={event_id}&sportId=66"
        link_str = f'🎲 <a href="{url}">VER JOGO AO VIVO</a>'

    # ── MONTAR MENSAGEM ──────────────────────────────────────
    msg  = f"{semaphore} <b>{league_display} — {strategy}</b>\n"
    msg += f"<b>@ {obs_odd}</b>  |  Conf: {avg_conf:.0f}%\n"
    msg += f"⏱ {time_str}  📊 {scoreboard}\n"
    msg += "─────────────────────\n"
    msg += f"<b>{home_padded}</b>  {home_bar}\n"
    msg += f"<b>{away_padded}</b>  {away_bar}\n"
    msg += "─────────────────────\n"
    msg += f"{liga_status}  |  BTTS {btts:.0f}%  |  HT+1.5 {ht_pct:.0f}%"
    if link_str:
        msg += f"\n{link_str}"

    return msg


def format_result_message(tip, ht_home, ht_away, ft_home, ft_away, result):
    """
    ✅ RESULTADO OTIMIZADO — Mostra quem marcou o quê + confidence original.
    """
    strategy    = tip.get('strategy', '')
    league      = tip.get('league', '')
    home_p      = tip.get('home_player', '?')
    away_p      = tip.get('away_player', '?')
    sent_odd    = tip.get('sent_odd', '')
    tipped_nick = tip.get('tipped_player_nick', '')
    conf        = tip.get('avg_confidence', None)

    ft_total = ft_home + ft_away

    if result == 'green':
        emoji  = "✅"
        status = "GREEN"
    else:
        emoji  = "❌"
        status = "RED"

    odd_str  = f" @ {sent_odd}" if sent_odd else ""
    conf_str = f"  |  Conf: {conf:.0f}%" if conf is not None else ""

    msg  = f"{emoji} <b>{status}</b> — {league}\n"
    msg += f"<b>{strategy}</b>{odd_str}{conf_str}\n"
    msg += "─────────────────────\n"
    msg += f"HT {ht_home}-{ht_away}  →  FT <b>{ft_home}-{ft_away}</b>  ({ft_total} gols)\n"

    # Mostrar gols individuais se for aposta individual
    if tipped_nick:
        from_home = tipped_nick.upper() == (home_p or '').upper()
        from_away = tipped_nick.upper() == (away_p or '').upper()
        if from_home:
            msg += f"<b>{home_p}</b>: {ft_home} gols  |  {away_p}: {ft_away} gols"
        elif from_away:
            msg += f"{home_p}: {ft_home} gols  |  <b>{away_p}</b>: {ft_away} gols"
        else:
            msg += f"{home_p}: {ft_home}  |  {away_p}: {ft_away}"
    else:
        msg += f"{home_p}: {ft_home} gols  |  {away_p}: {ft_away} gols"

    return msg



def format_league_stats_text(stats):
    """
    Formato sem colunas: 1 linha por liga com barra de progresso.
    Funciona perfeitamente no mobile sem depender de alinhamento.
    """
    LIGAS_MONITORADAS = {
        "BATTLE 8 MIN", "VALHALLA CUP", "VALKYRIE CUP",
        "GT LEAGUE 12 MIN", "CLA 10 MIN", "H2H 8 MIN",
        "VOLTA 6 MIN", "INT 8 MIN",
    }
    ORDER = [
        "BATTLE 8 MIN", "VALHALLA CUP", "VALKYRIE CUP",
        "GT LEAGUE 12 MIN", "CLA 10 MIN",
        "H2H 8 MIN", "VOLTA 6 MIN", "INT 8 MIN",
    ]

    def badge(avg):
        n = round(avg / 10)
        bar = "█" * n + "░" * (10 - n)
        color = "🟢" if avg >= 78 else ("🟡" if avg >= 48 else "🔴")
        return color, bar

    if not stats:
        return "📊 <b>ANÁLISE DE LIGAS</b>\n\nSem dados disponíveis."

    filtered = {k: v for k, v in stats.items() if k in LIGAS_MONITORADAS}
    if not filtered:
        return "📊 <b>ANÁLISE DE LIGAS</b>\n\nAguardando dados das ligas monitoradas."

    scores = {}
    for league, s in filtered.items():
        scores[league] = (
            s['ht']['o05'] + s['ht']['o15'] + s['ht']['btts'] +
            s['ft']['o15'] + s['ft']['o25'] + s['ft']['btts']
        ) / 6

    best  = max(scores, key=scores.get)
    worst = min(scores, key=scores.get)

    now_str = datetime.now(MANAUS_TZ).strftime('%H:%M')
    out = [f"📊 <b>LIGAS — últimos 5j</b>  <i>{now_str}</i>", ""]

    for league in ORDER:
        if league not in filtered:
            continue
        avg          = scores[league]
        color, bar   = badge(avg)
        tag          = " 🏆" if league == best else (" ⚠️" if league == worst else "")
        out.append(f"{color} <b>{league}</b>{tag}")
        out.append(f"     {bar} {avg:.0f}%")

    out.append("")
    out.append(f"🏆 <b>{best}</b>  ⚠️ <b>{worst}</b>")
    return "\n".join(out)


async def update_league_stats(bot, recent_matches, force=False):
    global last_league_summary, last_league_message_id, league_stats, last_league_update_time

    try:
        from collections import defaultdict
        recent_matches = sorted(recent_matches, key=lambda x: x.get('data_realizacao', ''), reverse=True)

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
            total  = len(last_n)
            def calc(cond): return int(sum(1 for g in last_n if cond(g)) / total * 100)
            stats[league] = {
                'ht': {
                    'o05':  calc(lambda g: g['ht_goals'] > 0),
                    'o15':  calc(lambda g: g['ht_goals'] > 1),
                    'btts': calc(lambda g: g['ht_btts']),
                },
                'ft': {
                    'o15':  calc(lambda g: g['ft_goals'] > 1),
                    'o25':  calc(lambda g: g['ft_goals'] > 2),
                    'btts': calc(lambda g: g['ft_btts']),
                },
                'count': total
            }

        if not stats:
            return

        current_time = time.time()

        # ✅ FIX #3: Removido o guard "if league_stats == stats: return"
        # Motivo: impedia reenvio mesmo após reinício do bot, pois os dados
        # eram iguais ao estado salvo. Agora o cooldown de 10min é suficiente.
        # O guard de "dados iguais" só bloqueava sem benefício real.

        # Cooldown de 10 min entre envios (evita spam) — exceto se force=True
        if not force and last_league_update_time > 0 and current_time - last_league_update_time < 600:
            league_stats = stats  # Atualiza o cache mesmo sem enviar
            return

        league_stats            = stats
        last_league_update_time = current_time

        # ✅ Texto rico substitui imagem PNG
        league_text = format_league_stats_text(stats)

        if last_league_message_id:
            try:
                await bot.delete_message(chat_id=CHAT_ID, message_id=last_league_message_id)
            except:
                pass

        msg = await bot.send_message(
            chat_id=CHAT_ID,
            text=league_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        last_league_message_id = msg.message_id
        save_state()
        print("[✓] Resumo das ligas atualizado (texto)")

    except Exception as e:
        print(f"[ERROR] update_league_stats: {e}")


# =============================================================================
# ENVIO DE MENSAGENS
# =============================================================================

async def send_tip(bot, event, strategy, obs_odd, home_stats, away_stats, avg_confidence=0):
    global sent_tips, sent_match_ids
    event_id = event.get('id')
    period = 'HT' if 'HT' in strategy.upper() else 'FT'
    event_base_key = f"{event_id}_ANY"
    if event_base_key in sent_match_ids:
        print(f"[SKIP] Evento {event_id} já teve tip enviada.")
        return

    timer = event.get('timer', {})
    sent_minute = timer.get('minute', 0)

    home_player = event.get('homePlayer', '')
    away_player = event.get('awayPlayer', '')
    for player in [home_player, away_player]:
        if player:
            on_cooldown, remaining = is_player_on_cooldown(player)
            if on_cooldown:
                print(f"[COOLDOWN] {player} em cooldown por {remaining:.0f} min.")
                return

    liga_det = event.get('_liga_det')
    for attempt in range(3):
        try:
            msg = format_tip_message(event, strategy, obs_odd, home_stats, away_stats, liga_det, avg_confidence=avg_confidence)
            message_obj = await bot.send_message(
                chat_id=CHAT_ID, text=msg, parse_mode="HTML", disable_web_page_preview=True
            )
            sent_match_ids.add(event_base_key)
            sent_match_ids.add(f"{event_id}_{period}")

            tipped_player = event.get('homePlayer')
            tipped_nick = extract_pure_nick(tipped_player) if tipped_player else ""
            strategy_lower = strategy.lower()
            for p in [event.get('homePlayer'), event.get('awayPlayer')]:
                if p and p.lower() in strategy_lower:
                    tipped_player = p
                    tipped_nick = extract_pure_nick(p)
                    break

            sent_tips.append({
                'event_id': event_id,
                'strategy': strategy,
                'sent_time': datetime.now(MANAUS_TZ),
                'status': 'pending',
                'message_id': message_obj.message_id,
                'message_text': msg,
                'home_player': home_player,
                'away_player': away_player,
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
                'sent_odd': obs_odd,
                'avg_confidence': avg_confidence,
                'liga_det': liga_det or {},
            })
            save_state()
            print(f"[✓] Tip enviada: {event_id} - {strategy} @ min {sent_minute}")
            break
        except Exception as e:
            print(f"[ERROR] send_tip tentativa {attempt+1}: {e}")
            if attempt < 2:
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
            # home_score_ft pode ser só o 2T em algumas ligas → somar com ht para garantir total
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
            # Apostas INDIVIDUAIS (nome do jogador na strategy) — usa gols do jogador
            elif '(TOTAL)' not in strategy and tipped_nick and '+1.5 GOLS FT' in strategy:
                gols = get_player_ft_goals(tipped_nick, matched)
                result = 'green' if gols >= 2 else 'red'
            elif '(TOTAL)' not in strategy and tipped_nick and '+2.5 GOLS FT' in strategy:
                gols = get_player_ft_goals(tipped_nick, matched)
                result = 'green' if gols >= 3 else 'red'
            elif '(TOTAL)' not in strategy and tipped_nick and '+3.5 GOLS FT' in strategy:
                gols = get_player_ft_goals(tipped_nick, matched)
                result = 'green' if gols >= 4 else 'red'
            elif '(TOTAL)' not in strategy and tipped_nick and '+4.5 GOLS FT' in strategy:
                gols = get_player_ft_goals(tipped_nick, matched)
                result = 'green' if gols >= 5 else 'red'
            # Apostas TOTAIS do jogo — sempre usa ft_total (soma dos dois jogadores)
            elif '+1.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 2 else 'red'
            elif '+2.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 3 else 'red'
            elif '+3.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 4 else 'red'
            elif '+4.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 5 else 'red'
            elif '+5.5 GOLS FT' in strategy:
                result = 'green' if ft_total >= 6 else 'red'
            elif 'BTTS FT' in strategy:
                result = 'green' if (ft_home_total > 0 and ft_away_total > 0) else 'red'

            if result:
                tip['status'] = result
                result_msg = format_result_message(
                    tip, ht_home, ht_away, ft_home_total, ft_away_total, result
                )
                try:
                    await bot.edit_message_text(
                        chat_id=CHAT_ID, message_id=tip['message_id'],
                        text=result_msg, parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"[WARN] Não editou mensagem: {e}")

                print(f"[✓] {result.upper()} - {strategy}")
                save_tip_result(tip, ht_home, ht_away, ft_home_total, ft_away_total)

                tip_league = tip.get('league', '')
                if tip_league:
                    mudou_liga, novo_status, msg_liga = league_manager.record_result(tip_league, result == 'green')
                    if mudou_liga and msg_liga:
                        try:
                            await bot.send_message(chat_id=CHAT_ID, text=msg_liga, parse_mode="HTML")
                        except Exception as e:
                            print(f"[WARN] Não enviou notif de liga: {e}")

                tipped_p = tip.get('tipped_player_nick', '')
                if tipped_p:
                    update_player_red_streak(tipped_p, result)

                d_key = tip['sent_time'].astimezone(MANAUS_TZ).strftime('%Y-%m-%d')
                if d_key not in daily_stats:
                    daily_stats[d_key] = {'green': 0, 'red': 0}
                daily_stats[d_key][result] += 1

        sent_tips[:] = [t for t in sent_tips if t.get('status') == 'pending']

        today_greens = daily_stats.get(today_str, {}).get('green', 0)
        today_reds   = daily_stats.get(today_str, {}).get('red', 0)
        total_resolved = today_greens + today_reds
        if total_resolved > 0:
            perc = (today_greens / total_resolved) * 100
            summary = (
                f"<b>👑 RW TIPS - FIFA 🎮</b>\n"
                f"✅ Green [{today_greens}]\n"
                f"❌ Red [{today_reds}]\n"
                f"📊 {perc:.1f}%"
            )
            if summary != last_summary:
                await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
                last_summary = summary

        if last_daily_message_date and last_daily_message_date != today_str:
            sorted_dates = sorted(daily_stats.keys())
            if sorted_dates:
                msg = "🚨 <b>Resumo Geral:</b>\n\n"
                for date_str in sorted_dates[-7:]:
                    ds = daily_stats[date_str]
                    g, r = ds['green'], ds['red']
                    t = g + r
                    if t == 0:
                        continue
                    pct = (g / t) * 100
                    fmt_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d/%m')
                    msg += f"📅 {fmt_date} --> ✅ [{g}] | ❌ [{r}] | 📊 [{pct:.1f}%]\n"
                await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")

        if last_daily_message_date != today_str:
            last_daily_message_date = today_str
            try:
                await bot.send_message(
                    chat_id=CHAT_ID, text=league_manager.get_status_report(), parse_mode="HTML"
                )
            except Exception as e:
                print(f"[WARN] Erro ao enviar status das ligas: {e}")
            save_state()

        await update_league_stats(bot, recent)

    except Exception as e:
        print(f"[ERROR check_results] {e}")


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

                # ✅ Registrar liga automaticamente (aparece no relatório mesmo sem tips)
                if mapped_league and mapped_league != 'Unknown':
                    league_manager.register_league(mapped_league)

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

                # ✅ FILTRO DE QUALIDADE DA LIGA — Verificar cenário favorável
                league_last5 = get_league_last5(mapped_league, global_history_cache.get('matches', []))
                liga_ok, liga_score, liga_det = check_league_quality(mapped_league, league_last5)
                if not liga_ok:
                    falhas = ' | '.join(liga_det.get('failed', [])[:3])
                    print(f"[LIGA FRIA] {mapped_league} ({liga_score:.0f}%): {falhas}")
                    continue
                print(f"[LIGA OK] {mapped_league} — score {liga_score:.0f}% ({liga_det['tipo']})")

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

                # Confidence calculado com H2H integrado
                h2h = get_h2h_stats(home_player, away_player)
                conf_result     = calculate_confidence_score(home_stats, away_stats, h2h)
                home_confidence = conf_result['home_confidence']
                away_confidence = conf_result['away_confidence']
                avg_confidence  = conf_result['avg_confidence']
                max_confidence  = max(home_confidence, away_confidence)

                # GATE DE CONFIANÇA — mínimo 70% para qualquer tip
                #
                # GATE 1 — Apostas INDIVIDUAIS (gols de 1 jogador):
                #   Passa se PELO MENOS 1 jogador >= 70%
                #
                # GATE 2 — Apostas TOTAIS do jogo (over X.5 FT total):
                #   Passa se MÉDIA dos dois >= 70%
                #
                # Se nenhum gate passa → bloqueia

                gate_individual = max_confidence >= 70   # 1 jogador forte basta
                gate_total      = avg_confidence  >= 70  # média mínima para totais

                if not gate_individual and not gate_total:
                    bd_h = " | ".join(conf_result['breakdown_home'])
                    bd_a = " | ".join(conf_result['breakdown_away'])
                    print(f"[BLOCKED] max={max_confidence:.0f}% avg={avg_confidence:.0f}% — nenhum gate passou")
                    print(f"  {home_player}: {home_confidence:.0f}% — {bd_h}")
                    print(f"  {away_player}: {away_confidence:.0f}% — {bd_a}")
                    continue

                # Gate: pelo menos 1 jogador com média mínima para a liga
                league_profile = LEAGUE_PROFILES.get(mapped_league, LEAGUE_PROFILES["DEFAULT"])
                min_avg = league_profile['min_player_avg']
                if home_stats['avg_goals_scored_ft'] < min_avg and away_stats['avg_goals_scored_ft'] < min_avg:
                    print(f"[BLOCKED] Ambos com média FT abaixo do mínimo da liga ({min_avg})")
                    continue

                h2h_info = f"H2H: {h2h['count']}j" if h2h else "H2H: sem dados"
                gate_str = f"gate={'IND+TOT' if gate_individual and gate_total else ('IND' if gate_individual else 'TOT')}"
                print(f"[STATS] {home_player}: FT={home_stats['avg_scored_ft_5j']:.1f} HT={home_stats['avg_scored_ht_5j']:.1f} conf={home_confidence:.0f}%")
                print(f"[STATS] {away_player}: FT={away_stats['avg_scored_ft_5j']:.1f} HT={away_stats['avg_scored_ht_5j']:.1f} conf={away_confidence:.0f}% | {h2h_info} | {gate_str}")

                all_league_stats = league_stats if mapped_league in league_stats else {}

                strategies = evaluate_open_lines(
                    event, home_stats, away_stats, all_league_stats, open_lines,
                    avg_confidence,
                    gate_individual=gate_individual,
                    gate_total=gate_total,
                    home_confidence=home_confidence,
                    away_confidence=away_confidence,
                )

                for strat_obj in strategies:
                    strategy_name = strat_obj['name']
                    obs_odd = strat_obj['odd']

                    # Para apostas INDIVIDUAIS, exibir confiança DO JOGADOR apostado
                    # Para apostas TOTAIS, manter a média dos dois
                    is_individual = '(TOTAL)' not in strategy_name and 'HT' not in strategy_name
                    if is_individual:
                        home_raw_up = event.get('homeRaw', '').upper()
                        away_raw_up = event.get('awayRaw', '').upper()
                        strat_up    = strategy_name.upper()
                        if home_raw_up and home_raw_up in strat_up:
                            display_confidence = home_confidence
                        elif away_raw_up and away_raw_up in strat_up:
                            display_confidence = away_confidence
                        else:
                            display_confidence = max(home_confidence, away_confidence)
                    else:
                        display_confidence = avg_confidence

                    print(f"[✓] OPORTUNIDADE: {strategy_name} (Odd: {obs_odd}) | Conf: {display_confidence:.0f}%")
                    event['_liga_det'] = liga_det
                    await send_tip(bot, event, strategy_name, obs_odd, home_stats, away_stats, avg_confidence=display_confidence)
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
    # ✅ FIX #3: force=True garante envio do resumo sempre ao iniciar
    await update_league_stats(bot, recent, force=True)

    # ✅ Enviar status das ligas imediatamente na inicialização
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=league_manager.get_status_report(),
            parse_mode="HTML"
        )
        print("[✓] Status inicial das ligas enviado")
    except Exception as e:
        print(f"[WARN] Não enviou status inicial: {e}")

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