"""
BOT FIFA v5.0 — REESCRITO DO ZERO
Critérios diretos baseados nos últimos 5 jogos de cada player + liga.
Sem sistema de confidence, sem funil excessivo, sem filtros impossíveis.

CRITÉRIOS (ligas 8 min):
  GATE HT: liga_avg_ht >= 3.2  +  p1_avg_ht >= 2.1  +  p2_avg_ht >= 2.1
  +0.5 HT : p1_ht >= 1.5  E  p2_ht >= 1.5
  +1.5 HT : p1_ht >= 2.1  E  p2_ht >= 2.1  (+ gate)
  +2.5 HT : p1_ht >= 2.6  E  p2_ht >= 2.6  (+ gate 4.0)
  BTTS HT : p1_btts_ht >= 60%  E  p2_btts_ht >= 60%
  +2.5 FT : p1_ft + p2_ft >= 5.0
  +3.5 FT : p1_ft + p2_ft >= 6.5
  +4.5 FT : p1_ft + p2_ft >= 8.0
  BTTS FT : p1_btts_ft >= 70%  E  p2_btts_ft >= 70%
  +1.5 IND: player_avg_ft >= 2.2  E  pct_scored_2+ >= 70%
  +2.5 IND: player_avg_ft >= 3.2  E  pct_scored_3+ >= 60%
  +3.5 IND: player_avg_ft >= 4.2  E  pct_scored_4+ >= 50%
"""

import os, time, re, json, asyncio, logging, concurrent.futures
import requests
from datetime import datetime, timezone, timedelta
from collections import deque
from telegram import Bot
from telegram.request import HTTPXRequest

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID   = "-1001981134607"
MANAUS_TZ = timezone(timedelta(hours=-4))

ALTENAR_LIVE  = (
    "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents"
    "?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1"
    "&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66"
    "&catIds=2085,1571,1728,1594,2086,1729,2130"
)
ALTENAR_EVENT = (
    "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventDetails"
    "?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1"
    "&numFormat=en-GB&countryCode=BR&eventId={}&showNonBoosts=false"
)
SUPERBET_LIVE   = (
    "https://production-superbet-offer-br.freetls.fastly.net"
    "/v2/pt-BR/events/by-date?sportId=75&currentStatus=active&offerState=live"
)
SUPERBET_STRUCT = (
    "https://production-superbet-offer-br.freetls.fastly.net"
    "/v2/pt-BR/sport/75/tournaments"
)
SUPERBET_EVENT  = (
    "https://production-superbet-offer-br.freetls.fastly.net"
    "/v2/pt-BR/events/{}"
)
HISTORY_URL = "https://rwtips-k8j2.onrender.com/api/history"

# =============================================================================
# CRITÉRIOS POR TIPO DE LIGA
# =============================================================================
# =============================================================================
# CRITÉRIOS POR TIPO DE LIGA
#
# Todas as apostas HT são enviadas durante o 1ºT.
# Todas as apostas FT são enviadas durante o 2ºT.
# Médias baseadas nos últimos 5 jogos do player (independente do adversário).
# Médias de liga baseadas nos últimos 5 jogos da liga.
# =============================================================================

# 6 min (Volta) e 8 min (Battle, H2H, Valkyrie) — mesmos critérios
CRIT_8MIN = {
    # ── Gate geral ─────────────────────────────────────────────────
    # HT: liga e ambos players precisam passar para liberar QUALQUER aposta HT
    "ht_gate_league": 2.7,   # média gols HT da liga (últimos 5j)
    "ht_gate_p":      1.8,   # média gols HT marcados de CADA player (últimos 5j)
    # FT: liga e ambos players precisam passar para liberar QUALQUER aposta FT
    "ft_gate_league": 3.7,   # média gols FT da liga (últimos 5j)
    "ft_gate_p":      2.1,   # média gols FT marcados de CADA player (últimos 5j)

    # ── Apostas HT (1ºT) ───────────────────────────────────────────
    # +0.5 HT   | Placar: 0x0
    "ht_05":   {"p1_marc": 1.5, "p2_marc": 1.5},                        # +0.2
    # +1.5 HT   | Placar: 0x0 | 1x0 | 0x1
    "ht_15":   {"p1_marc": 1.9, "p2_marc": 1.9},                        # +0.2
    # +2.5 HT   | Placar: 1x0 | 0x1 | 1x1
    "ht_25":   {"p1_marc": 2.3, "p2_marc": 2.3},                        # +0.3
    # BTTS HT   | Placar: total <= 1
    "ht_btts": {"p1_marc": 1.5, "p1_sof": 1.0, "p2_marc": 1.5, "p2_sof": 1.0},

    # ── Apostas FT (2ºT) ───────────────────────────────────────────
    # pct_ft2 = % dos últimos 5 jogos com >= 2 gols marcados (consistência)
    # pct_ft3 = % dos últimos 5 jogos com >= 3 gols marcados
    # +1.5 FT   | Placar: 0 gols
    "ft_15":   {"p1_marc": 2.0, "p2_marc": 2.0, "pct_ft2": 0.60},      # +0.3 + consistência
    # +2.5 FT   | Placar: < 2 gols
    "ft_25":   {"p1_marc": 2.0, "p2_marc": 2.0, "pct_ft2": 0.60},      # +0.5 + consistência
    # +3.5 FT   | Placar: < 3 gols
    "ft_35":   {"p1_marc": 2.5, "p2_marc": 2.5, "pct_ft3": 0.60},      # +0.3 + consistência
    # +4.5 FT   | Placar: <= 3 gols
    "ft_45":   {"p1_marc": 3.0, "p2_marc": 3.0, "pct_ft3": 0.80},      # +0.3 + consistência alta
    # BTTS FT   | Placar: total <= 1
    "ft_btts": {"p1_marc": 1.2, "p1_sof": 1.2, "p2_marc": 1.2, "p2_sof": 1.2, "pct_ft2": 0.60},

    "min_odd": 1.55,
}

# 6 min (Volta) — mesmos critérios do 8 min
CRIT_6MIN = CRIT_8MIN

# 10 min (CLA, Adriatic) e 12 min (GT, Valhalla, Battle 12)
CRIT_12MIN = {
    # ── Gate geral ─────────────────────────────────────────────────
    "ht_gate_league": 2.8,   # reduzido de 3.2 — ligas 12min têm poucos dados
    "ht_gate_p":      1.6,   # reduzido de 2.0 — players 12min marcam menos no HT
    "ft_gate_league": 4.1,
    "ft_gate_p":      2.4,

    # ── Apostas HT (1ºT) ───────────────────────────────────────────
    # +0.5 HT   | Placar: 0x0
    "ht_05": {"p1_marc": 1.5, "p2_marc": 1.5},
    # +1.5 HT   | Placar: 0x0 | 1x0 | 0x1
    "ht_15": {"p1_marc": 1.9, "p2_marc": 1.9},
    # +2.5 HT   | Placar: 1x0 | 0x1 | 1x1
    "ht_25": {"p1_marc": 2.2, "p2_marc": 2.2},
    # BTTS HT   | Placar: total <= 1
    "ht_btts": {"p1_marc": 1.6, "p1_sof": 1.0, "p2_marc": 1.6, "p2_sof": 1.0},

    # ── Apostas FT (2ºT) ───────────────────────────────────────────
    "ft_15":   {"p1_marc": 2.2, "p2_marc": 2.2, "pct_ft2": 0.60},
    "ft_25":   {"p1_marc": 2.5, "p2_marc": 2.5, "pct_ft2": 0.60},
    "ft_35":   {"p1_marc": 3.0, "p2_marc": 3.0, "pct_ft3": 0.60},
    "ft_45":   {"p1_marc": 3.5, "p2_marc": 3.5, "pct_ft3": 0.80},
    "ft_btts": {"p1_marc": 2.0, "p1_sof": 1.5, "p2_marc": 2.0, "p2_sof": 1.5, "pct_ft2": 0.60},

    "min_odd": 1.55,
}

LEAGUE_PROFILES = {
    "BATTLE 8 MIN":    {"crit": "8MIN",  "duration": 8,  "ht_dur": 4},
    "BATTLE 12 MIN":   {"crit": "12MIN", "duration": 12, "ht_dur": 6},
    "H2H 8 MIN":       {"crit": "8MIN",  "duration": 8,  "ht_dur": 4},
    "VALKYRIE CUP":    {"crit": "8MIN",  "duration": 8,  "ht_dur": 4},
    "VALHALLA CUP":    {"crit": "12MIN", "duration": 12, "ht_dur": 6},
    "GT LEAGUE 12 MIN":{"crit": "12MIN", "duration": 12, "ht_dur": 6},
    "CLA 10 MIN":      {"crit": "12MIN", "duration": 10, "ht_dur": 5},
    "ADRIATIC":        {"crit": "12MIN", "duration": 10, "ht_dur": 5},
    "VOLTA 6 MIN":     {"crit": "6MIN",  "duration": 6,  "ht_dur": 3},
    "DEFAULT":         {"crit": "8MIN",  "duration": 8,  "ht_dur": 4},
}

def get_crit(league_key):
    prof = LEAGUE_PROFILES.get(league_key, LEAGUE_PROFILES["DEFAULT"])
    crit_key = prof["crit"]
    if crit_key == "12MIN": return CRIT_12MIN
    if crit_key == "6MIN":  return CRIT_6MIN
    return CRIT_8MIN

def get_profile(league_key):
    return LEAGUE_PROFILES.get(league_key, LEAGUE_PROFILES["DEFAULT"])

# =============================================================================
# MAPEAMENTO DE LIGAS
# =============================================================================
LIVE_MAP = {
    "E-Soccer - Battle - 8 minutos de jogo":          "BATTLE 8 MIN",
    "Esoccer Battle - 8 mins play":                    "BATTLE 8 MIN",
    "E-Soccer - H2H GG League - 8 minutos de jogo":   "H2H 8 MIN",
    "Esoccer H2H GG League - 8 mins play":             "H2H 8 MIN",
    "H2H GG LEAGUE - E-FOOTBALL":                      "H2H 8 MIN",
    "H2H GG LEAGUE":                                   "H2H 8 MIN",
    "H2H GG":                                          "H2H 8 MIN",
    "E-Soccer - GT Leagues - 12 minutos de jogo":      "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues - 12 mins play":               "GT LEAGUE 12 MIN",
    "Esoccer GT Leagues \u2013 12 mins play":          "GT LEAGUE 12 MIN",
    "E-Soccer - Battle Volta - 6 minutos de jogo":     "VOLTA 6 MIN",
    "Esoccer Battle Volta - 6 mins play":              "VOLTA 6 MIN",
    "Valhalla Cup": "VALHALLA CUP", "Valhalla League": "VALHALLA CUP",
    "Valkyrie Cup": "VALKYRIE CUP",
    "CLA": "CLA 10 MIN",
    # "Cyber Live Arena" — liga DIFERENTE de CLA, não mapear (evita tips em liga errada)
    "Champions Cyber League": "CLA 10 MIN", "Cyber League": "CLA 10 MIN",
    "Champions League B 2\u00d76": "GT LEAGUE 12 MIN",
    "Champions League B 2x6":   "GT LEAGUE 12 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)":  "BATTLE 8 MIN",
    "ESportsBattle. Premier League (2x4 mins)":  "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)":    "VOLTA 6 MIN",
}
HIST_MAP = {
    "Battle 6m": "VOLTA 6 MIN", "Battle 8m": "BATTLE 8 MIN",
    "H2H 8m": "H2H 8 MIN", "GT Leagues 12m": "GT LEAGUE 12 MIN",
    "GT League 12m": "GT LEAGUE 12 MIN",
    "Esoccer Battle - 8 mins play":           "BATTLE 8 MIN",
    "Esoccer Battle Volta - 6 mins play":     "VOLTA 6 MIN",
    "Esoccer GT Leagues \u2013 12 mins play": "GT LEAGUE 12 MIN",
    "Esoccer H2H GG League - 8 mins play":   "H2H 8 MIN",
    "Valhalla Cup": "VALHALLA CUP", "Valkyrie Cup": "VALKYRIE CUP",
    "CLA League": "CLA 10 MIN", "CLA": "CLA 10 MIN",
    "Champions Cyber League": "CLA 10 MIN", "Cyber League": "CLA 10 MIN",
    "ESportsBattle. Club World Cup (2x4 mins)": "BATTLE 8 MIN",
    "Volta International III 4x4 (2x3 mins)":   "VOLTA 6 MIN",
    # Nomes reais confirmados pelos logs do histórico
    # GT LEAGUE
    "GT LEAGUES":                               "GT LEAGUE 12 MIN",
    "GT League 12m":                            "GT LEAGUE 12 MIN",
    "GT Leagues 12m":                           "GT LEAGUE 12 MIN",
    "GT Leagues":                               "GT LEAGUE 12 MIN",
    # VOLTA
    "VOLTA - 6 MIN":                            "VOLTA 6 MIN",
    "Volta 6m":                                 "VOLTA 6 MIN",
    "Volta - 6 MIN":                            "VOLTA 6 MIN",
    # BATTLE 12
    "Battle 12m":                               "BATTLE 12 MIN",
    "Champions League B 2×6":             "BATTLE 12 MIN",
    "Battle - Liga dos Campeões 2":             "BATTLE 12 MIN",
    "Battle - Liga de Campeões 2":              "BATTLE 12 MIN",
    "BATTLE 12":                                "BATTLE 12 MIN",
    # ADRIATIC
    "eAdriatic League":                         "ADRIATIC",
    "E-Adriatic":                               "ADRIATIC",
    "Adriatic":                                 "ADRIATIC",
    # H2H
    "H2H GG League - 8 mins play":             "H2H 8 MIN",
    "H2H 8m":                                   "H2H 8 MIN",
}

def map_league(name):
    if not name: return "Unknown"
    if name in LIVE_MAP: return LIVE_MAP[name]
    if name in HIST_MAP: return HIST_MAP[name]
    nu = name.upper()
    for k, v in LIVE_MAP.items():
        if nu.startswith(k.upper()): return v
    for k, v in HIST_MAP.items():
        if nu.startswith(k.upper()): return v
    return name

# =============================================================================
# LEAGUE MANAGER — auto-bloqueio/desbloqueio por performance
# =============================================================================
LEAGUE_INITIAL = {lg: True for lg in LEAGUE_PROFILES if lg != "DEFAULT"}
LEAGUE_RELOCK  = 55  # % → bloqueia
LEAGUE_UNLOCK  = 68  # % → desbloqueia
LEAGUE_WINDOW  = 15  # últimas N tips
LEAGUE_MIN_TIPS = 5  # amostras mínimas para decisão


class LeagueManager:
    def __init__(self, fn='league_perf.json'):
        self.fn = fn
        self.leagues = {}
        self._load()

    def _load(self):
        if os.path.exists(self.fn):
            try:
                with open(self.fn) as f:
                    raw = json.load(f)
                for lg, d in raw.items():
                    self.leagues[lg] = {
                        'active': d.get('active', True),
                        'window': deque(d.get('window', []), maxlen=LEAGUE_WINDOW),
                        'total':  d.get('total', 0),
                    }
            except Exception as e:
                print(f"[LM] load error: {e}")

    def save(self):
        try:
            with open(self.fn, 'w') as f:
                json.dump(
                    {lg: {'active': v['active'], 'window': list(v['window']), 'total': v['total']}
                     for lg, v in self.leagues.items()},
                    f, indent=2
                )
        except Exception as e:
            print(f"[LM] save error: {e}")

    def _ensure(self, league):
        if league not in self.leagues:
            self.leagues[league] = {
                'active': LEAGUE_INITIAL.get(league, True),
                'window': deque(maxlen=LEAGUE_WINDOW),
                'total': 0,
            }
        return self.leagues[league]

    def is_active(self, league):
        d = self._ensure(league)
        n = len(d['window'])
        if n < LEAGUE_MIN_TIPS:
            return d['active'], f"coletando dados ({n}/{LEAGUE_MIN_TIPS})"
        pct = sum(d['window']) / n * 100
        return d['active'], f"{pct:.0f}% | {n} tips"

    def record(self, league, green):
        d = self._ensure(league)
        d['window'].append(1 if green else 0)
        d['total'] += 1
        n = len(d['window'])
        if n < LEAGUE_MIN_TIPS:
            self.save()
            return False, None
        pct = sum(d['window']) / n * 100
        changed, msg = False, None
        if not d['active'] and pct >= LEAGUE_UNLOCK:
            d['active'] = True; changed = True
            msg = f"🟢 <b>LIGA ATIVA: {league}</b>\n{pct:.0f}% nas últimas {n} tips"
        elif d['active'] and pct < LEAGUE_RELOCK:
            d['active'] = False; changed = True
            msg = f"🔴 <b>LIGA PAUSADA: {league}</b>\n{pct:.0f}% nas últimas {n} tips"
        self.save()
        return changed, msg

    def register(self, league):
        self._ensure(league)

    def status(self):
        if not self.leagues:
            return "📊 Nenhuma liga registrada."
        lines = ["📊 <b>STATUS DAS LIGAS</b>\n"]
        for lg, d in sorted(self.leagues.items()):
            n = len(d['window'])
            if n >= LEAGUE_MIN_TIPS:
                pct = sum(d['window']) / n * 100
                bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
                st  = f"{pct:.0f}% {bar}"
            elif n:
                st = f"coletando ({n}/{LEAGUE_MIN_TIPS})"
            else:
                st = "sem tips"
            e = "🟢" if d['active'] else "🔴"
            lines.append(f"{e} <b>{lg}</b>: {st} | {d['total']} total")
        return "\n".join(lines)


league_manager = LeagueManager()

# =============================================================================
# ESTADO GLOBAL
# =============================================================================
history_cache  = {'matches': [], 'ts': 0}
HISTORY_TTL    = 120   # segundos

# Cache de stats pré-computados (atualizado junto com o histórico)
# Evita re-varredura de 800+ partidas a cada ciclo
stats_cache = {
    'players': {},       # {nick_upper: {avg_ht, avg_ft, avg_ht_sof, avg_ft_sof, games}}
    'leagues': {},       # {league_name: {avg_ht, avg_ft, games}}
    'ts':       0,       # timestamp da última atualização
}
STATS_TTL = 120          # atualiza junto com o histórico (2 min)

sent_tips       = []   # lista de dicts
sent_keys       = set()  # "{event_id}_HT" e "{event_id}_FT"
player_cooldown = {}   # {nick: datetime_liberacao}

daily_stats     = {}   # {"2026-03-14": {"green": N, "red": N}}
last_summary    = None
last_daily_date = None

sb_tournaments     = {}   # {tid: {"name": liga_canonica, "raw": str}}
sb_struct_ts       = 0

PLAYER_COOLDOWN_MIN = 30   # minutos de cooldown após 3 reds seguidos
PLAYER_RED_BLOCK    = 3


def save_state():
    try:
        state = {
            'sent_keys': list(sent_keys),
            'player_cooldown': {k: v.isoformat() for k, v in player_cooldown.items() if isinstance(v, datetime)},
            'last_summary': last_summary,
            'last_daily_date': last_daily_date,
        }
        with open('bot_state.json', 'w') as f:
            json.dump(state, f, indent=2)

        pending = [
            {**t, 'sent_time': t['sent_time'].isoformat()}
            for t in sent_tips if t.get('status') == 'pending'
        ]
        with open('tips_pending.json', 'w') as f:
            json.dump(pending, f, indent=2)
    except Exception as e:
        print(f"[save_state] {e}")


def save_result(tip, ht_h, ht_a, ft_h, ft_a):
    try:
        fname = 'tips_results.json'
        data = {}
        if os.path.exists(fname):
            with open(fname) as f:
                data = json.load(f)
        sent = tip['sent_time']
        if isinstance(sent, str):
            sent = datetime.fromisoformat(sent)
        dk = sent.astimezone(MANAUS_TZ).strftime('%Y-%m-%d')
        data.setdefault(dk, []).append({
            'strategy': tip.get('strategy'),
            'status':   tip.get('status'),
            'league':   tip.get('league'),
            'home':     tip.get('home_player'),
            'away':     tip.get('away_player'),
            'ht':       f"{ht_h}-{ht_a}",
            'ft':       f"{ft_h}-{ft_a}",
        })
        with open(fname, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[save_result] {e}")


def load_state():
    global sent_keys, player_cooldown, last_summary, last_daily_date, sent_tips, daily_stats
    try:
        if os.path.exists('bot_state.json'):
            with open('bot_state.json') as f:
                s = json.load(f)
            sent_keys   = set(s.get('sent_keys', []))
            last_summary = s.get('last_summary')
            last_daily_date = s.get('last_daily_date')
            for k, v in s.get('player_cooldown', {}).items():
                try:
                    player_cooldown[k] = datetime.fromisoformat(v)
                except (TypeError, ValueError):
                    pass  # ignora valores não-datetime

        if os.path.exists('tips_pending.json'):
            with open('tips_pending.json') as f:
                raw = json.load(f)
            for t in raw:
                try:
                    t['sent_time'] = datetime.fromisoformat(t['sent_time'])
                    sent_tips.append(t)
                except:
                    pass

        if os.path.exists('tips_results.json'):
            with open('tips_results.json') as f:
                results = json.load(f)
            for dk, tips in results.items():
                g = sum(1 for t in tips if t.get('status') == 'green')
                r = sum(1 for t in tips if t.get('status') == 'red')
                daily_stats[dk] = {'green': g, 'red': r}

        print(f"[load_state] {len(sent_tips)} tips pendentes, {len(sent_keys)} keys")
    except Exception as e:
        print(f"[load_state] {e}")


# =============================================================================
# UTILITÁRIOS
# =============================================================================
def extract_nick(raw):
    """Extrai o apelido curto de um nome bruto como 'Man City (fantazer)'."""
    if not raw or not isinstance(raw, str):
        return ""
    m = re.search(r'\(([^)]+)\)', raw)
    if m:
        inside = m.group(1).strip()
        if 2 <= len(inside) <= 16:
            return inside.upper()
    # Remove números de time, mantém só o nick
    cleaned = re.sub(r'\s*\(.*?\)', '', raw).strip()
    words = cleaned.split()
    # Preferir a última palavra (geralmente o nick)
    for w in reversed(words):
        if len(w) >= 2 and not w.isdigit():
            return w.upper()
    return cleaned.upper()[:15]


def normalize_nick(raw):
    """Versão que mantém capitalização bonita."""
    nick = extract_nick(raw)
    return nick.capitalize() if nick else raw.strip()


def is_player_blocked(player_raw):
    nick = extract_nick(player_raw)
    if not nick:
        return False, 0
    exp = player_cooldown.get(nick)
    if exp is None:
        return False, 0
    now = datetime.now(MANAUS_TZ)
    if isinstance(exp, datetime) and exp.tzinfo is None:
        exp = exp.replace(tzinfo=MANAUS_TZ)
    if now < exp:
        mins = (exp - now).total_seconds() / 60
        return True, mins
    del player_cooldown[nick]
    return False, 0


def update_cooldown(player_raw, result):
    nick = extract_nick(player_raw)
    if not nick:
        return
    key = f"_reds_{nick}"
    if result == 'green':
        player_cooldown.pop(key, None)
        return
    reds = player_cooldown.get(key, 0)
    if isinstance(reds, datetime):
        reds = 0
    reds += 1
    player_cooldown[key] = reds
    if reds >= PLAYER_RED_BLOCK:
        exp = datetime.now(MANAUS_TZ) + timedelta(minutes=PLAYER_COOLDOWN_MIN)
        player_cooldown[nick] = exp
        player_cooldown[key] = 0
        print(f"[COOLDOWN] {nick} bloqueado por {PLAYER_COOLDOWN_MIN}min após {PLAYER_RED_BLOCK} reds")


def parse_dt(s):
    """Converte string de data/hora para datetime com timezone."""
    if not s:
        return None
    s = str(s)
    if not s.endswith('Z') and not re.search(r'[+-]\d{2}:\d{2}$', s):
        s += 'Z'
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt.astimezone(MANAUS_TZ)
    except:
        return None

# =============================================================================
# SUPERBET — MAPEAMENTO DINÂMICO DE TORNEIOS
# =============================================================================
SB_CAT_MAP = {
    954:  "H2H 8 MIN",
    1293: "CLA 10 MIN",
    1269: "GT LEAGUE 12 MIN",
    1588: "ADRIATIC",
    # 1294 = Battle → classificado pelo nome
}


def _classify_battle(name):
    n = name.upper()
    if "VOLTA" in n:                                           return "VOLTA 6 MIN"
    if ("LIGA DOS CAMP" in n or "LIGA DE CAMP" in n) and ("2" in n or " II" in n):
        return "BATTLE 12 MIN"
    return "BATTLE 8 MIN"


def _fallback_by_name(name):
    n = name.upper()
    if "H2H" in n:                                    return "H2H 8 MIN"
    if "CYBER LIVE" in n or " CLA" in n:              return "CLA 10 MIN"
    if "GT" in n and ("LIGA" in n or "LEAGUE" in n):  return "GT LEAGUE 12 MIN"
    if "ADRIATIC" in n or "EAL " in n:                return "ADRIATIC"
    if "VALHALLA" in n:                               return "VALHALLA CUP"
    if "VALKYRIE" in n:                               return "VALKYRIE CUP"
    if "VOLTA" in n:                                  return "VOLTA 6 MIN"
    if "BATTLE" in n and ("CAMP" in n or "2X6" in n): return "BATTLE 12 MIN"
    if "BATTLE" in n:                                 return "BATTLE 8 MIN"
    return name


def update_sb_struct():
    global sb_tournaments, sb_struct_ts
    if time.time() - sb_struct_ts < 600:
        return
    hdrs = {
        'User-Agent': 'Mozilla/5.0',
        'Origin': 'https://superbet.bet.br',
        'Referer': 'https://superbet.bet.br/'
    }
    try:
        r = requests.get(SUPERBET_STRUCT, headers=hdrs, timeout=15)
        if r.status_code == 304:
            sb_struct_ts = time.time(); return
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        new_map = {}
        for cat in r.json().get('data', []):
            cat_id = cat.get('categoryId')
            for comp in cat.get('competitions', []):
                tid   = str(comp.get('tournamentId'))
                tname = comp.get('localNames', {}).get('pt-BR', '')
                if not tid or not tname:
                    continue
                if cat_id in SB_CAT_MAP:
                    league = SB_CAT_MAP[cat_id]
                elif cat_id == 1294:
                    league = _classify_battle(tname)
                else:
                    league = _fallback_by_name(tname)
                new_map[tid] = {"name": league, "raw": tname}
        if new_map:
            sb_tournaments.update(new_map)
            print(f"[SB] {len(new_map)} torneios mapeados")
        sb_struct_ts = time.time()
    except Exception as e:
        print(f"[SB struct] falhou ({e}) — usando fallback estático")
        STATIC = {
            "80560": "H2H 8 MIN", "71851": "BATTLE 12 MIN",
            "49965": "BATTLE 8 MIN", "81987": "BATTLE 8 MIN",
            "72619": "VOLTA 6 MIN", "94993": "CLA 10 MIN",
            "62997": "GT LEAGUE 12 MIN", "67383": "ADRIATIC",
        }
        for tid, lg in STATIC.items():
            if tid not in sb_tournaments:
                sb_tournaments[tid] = {"name": lg}
        sb_struct_ts = time.time()

# =============================================================================
# FETCH — SUPERBET LIVE
# =============================================================================
def fetch_superbet_live():
    update_sb_struct()
    try:
        past = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d') + "+06:00:00"
        url  = f"{SUPERBET_LIVE}&startDate={past}"
        hdrs = {
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'Mozilla/5.0',
            'Origin': 'https://superbet.bet.br',
            'Referer': 'https://superbet.bet.br/'
        }
        r = requests.get(url, headers=hdrs, timeout=15)
        if r.status_code != 200:
            return []
        events    = r.json().get('data', [])
        result    = []
        for ev in events:
            if not isinstance(ev, dict) or ev.get('sportId') != 75:
                continue
            try:
                match_name = ev.get('matchName', '')
                if '\xb7' not in match_name and '\u00b7' not in match_name:
                    continue
                parts    = match_name.split('\u00b7') if '\u00b7' in match_name else match_name.split('\xb7')
                home_raw = parts[0].strip()
                away_raw = parts[1].strip()
                home_nik = normalize_nick(home_raw)
                away_nik = normalize_nick(away_raw)
                t_id     = str(ev.get('tournamentId'))
                cached   = sb_tournaments.get(t_id)
                if isinstance(cached, dict):
                    league_raw = cached.get('name', f"SB-{t_id}")
                elif isinstance(cached, str):
                    league_raw = cached
                else:
                    league_raw = f"SB-{t_id}"
                mapped = map_league(league_raw)
                meta   = ev.get('metadata', {})
                hg     = int(meta.get('homeTeamScore', 0) or 0)
                ag     = int(meta.get('awayTeamScore', 0) or 0)
                eid    = str(ev.get('eventId'))

                # Tentar múltiplos campos — matchTime pode ser 0 mesmo com jogo em andamento
                minute = 0
                for _f in ['matchTime', 'currentTime', 'liveTime', 'matchClock',
                           'matchMinute', 'elapsed', 'minute']:
                    _v = ev.get(_f) or meta.get(_f)
                    if _v and int(_v) > 0:
                        minute = int(_v)
                        break

                # Fallback via utcDate — calcular elapsed desde o início do jogo
                if minute == 0:
                    _utc = ev.get('utcDate', '')
                    if _utc:
                        try:
                            _start = datetime.fromisoformat(_utc.replace('Z', '+00:00'))
                            _elapsed = (datetime.now(timezone.utc) - _start).total_seconds()
                            # Limite: elapsed deve ser menor que duração+3min do jogo
                            # Se maior, utcDate provavelmente é início do torneio, não do jogo
                            _dur_tmp = get_profile(map_league(league_raw)).get('duration', 12)
                            _max_elapsed = (_dur_tmp + 3) * 60
                            if 0 < _elapsed < _max_elapsed:
                                minute = max(0, int(_elapsed / 60))
                            # Se elapsed >= max, não usar (utcDate inválido) — manter minute=0
                        except:
                            pass

                # Fallback final: placar alto → provavelmente 2ºT
                if minute == 0 and (hg + ag) >= 4:
                    # Jogo com 4+ gols dificilmente está no início
                    # Usar duração do HT + 1 como estimativa conservadora
                    _t_id_tmp = str(ev.get('tournamentId', ''))
                    _cached_tmp = sb_tournaments.get(_t_id_tmp, {})
                    _lg_tmp = _cached_tmp.get('name', '') if isinstance(_cached_tmp, dict) else ''
                    _prof_tmp = get_profile(map_league(_lg_tmp))
                    minute = _prof_tmp.get('ht_dur', 4) + 1

                def _slug(s):
                    import unicodedata as _ud
                    n = _ud.normalize('NFKD', s)
                    n = ''.join(c for c in n if not _ud.combining(c))
                    n = re.sub(r'[\(\)]', ' ', n)
                    n = re.sub(r'[^a-zA-Z0-9]+', '-', n).strip('-')
                    return n.lower()

                sb_link = (f"https://superbet.bet.br/odds/e-sport-futebol/"
                           f"{_slug(home_raw)}-x-{_slug(away_raw)}-{eid}"
                           f"/?t=offer-live-{t_id}&mdt=o")

                # Filtrar jogo que já deveria ter encerrado
                # (Superbet pode manter evento como 'active' por alguns minutos após o fim)
                _utc_start = ev.get('utcDate', '')
                if _utc_start:
                    try:
                        _start_dt  = datetime.fromisoformat(_utc_start.replace('Z', '+00:00'))
                        _prof_tmp  = get_profile(mapped)
                        _dur_total = _prof_tmp.get('duration', 12)
                        _expected_end = _start_dt + timedelta(minutes=_dur_total + 2)
                        if _expected_end < datetime.now(timezone.utc):
                            continue   # Jogo provavelmente encerrado — ignorar
                    except:
                        pass

                result.append({
                    'id': f"sb-{eid}",
                    'leagueName': league_raw,
                    'mappedLeague': mapped,
                    'homePlayer': home_nik,
                    'awayPlayer': away_nik,
                    'homeRaw': home_raw,
                    'awayRaw': away_raw,
                    'tournamentId': t_id,
                    'superbetLink': sb_link,
                    'timer': {'minute': minute, 'second': 0,
                              'formatted': f"{minute:02d}:00"},
                    'score': {'home': hg, 'away': ag},
                    'scoreboard': f"{hg}-{ag}",
                    'liveTimeRaw': str(minute),
                    'startDateRaw': ev.get('utcDate', ''),
                    'source': 'superbet',
                })
            except:
                continue
        return result
    except Exception as e:
        print(f"[SB live] {e}")
        return []

# =============================================================================
# FETCH — ALTENAR (EstrelaBet) LIVE
# =============================================================================
def fetch_altenar_live():
    try:
        r = requests.get(ALTENAR_LIVE, timeout=10)
        r.raise_for_status()
        data  = r.json()
        comps = {c['id']: c for c in data.get('competitors', [])}
        champ = {c['id']: c['name'] for c in data.get('champs', [])}
        evts  = [e for e in data.get('events', []) if e.get('sportId') == 66]
        result = []

        for ev in evts:
            try:
                eid = ev.get('id')
                cids = ev.get('competitorIds', [])
                sc_raw = ev.get('score', [0, 0])

                def _score(s):
                    if isinstance(s, list):
                        return [int(s[0] if s else 0), int(s[1] if len(s) > 1 else 0)]
                    if isinstance(s, dict):
                        return [int(s.get('home', 0)), int(s.get('away', 0))]
                    if isinstance(s, str):
                        for sep in [':', '-']:
                            if sep in s:
                                try:
                                    p = s.split(sep)
                                    return [int(p[0].strip()), int(p[1].strip())]
                                except:
                                    pass
                    return [0, 0]

                sc = _score(sc_raw)
                hc = comps.get(cids[0], {}) if cids else {}
                ac = comps.get(cids[1], {}) if len(cids) > 1 else {}
                hn = hc.get('name', '')
                an = ac.get('name', '')
                if not hn or not an:
                    continue

                home_nick = normalize_nick(hn)
                away_nick = normalize_nick(an)
                league_name = champ.get(ev.get('champId'), 'Unknown')
                if "ECOMP" in league_name.upper() or "VIRTUAL" in league_name.upper():
                    continue

                mapped = map_league(league_name)

                # Parse do tempo ao vivo
                lt = ev.get('liveTime', '')
                minute, second = 0, 0
                if lt:
                    m = re.search(r'(\d+):(\d+)', lt)
                    if m:
                        minute, second = int(m.group(1)), int(m.group(2))
                    elif any(x in lt.upper() for x in ['1\u00aa PARTE', '1ST HALF']):
                        minute = 2

                result.append({
                    'id': str(eid),
                    'leagueName': league_name,
                    'mappedLeague': mapped,
                    'homePlayer': home_nick,
                    'awayPlayer': away_nick,
                    'homeRaw': hn,
                    'awayRaw': an,
                    'timer': {'minute': minute, 'second': second,
                              'formatted': f"{minute:02d}:{second:02d}"},
                    'score': {'home': sc[0], 'away': sc[1]},
                    'scoreboard': f"{sc[0]}-{sc[1]}",
                    'liveTimeRaw': lt,
                    'startDateRaw': ev.get('startDate', ''),
                    'source': 'altenar',
                })
            except:
                continue
        return result
    except Exception as e:
        print(f"[Altenar live] {e}")
        return []


def fetch_live_matches():
    """Combina Superbet + Altenar, deduplicando por par de jogadores."""
    sb   = fetch_superbet_live()
    alt  = fetch_altenar_live()
    seen = set()
    out  = []
    for ev in sb + alt:
        key = f"{ev['homePlayer']}_{ev['awayPlayer']}".upper()
        if key not in seen:
            seen.add(key)
            out.append(ev)
    print(f"[live] {len(out)} eventos ({len(sb)} SB / {len(alt)} ALT)")
    return out

# =============================================================================
# FETCH — MERCADOS DE UM EVENTO
# =============================================================================
def fetch_markets_altenar(event_id):
    try:
        url = ALTENAR_EVENT.format(event_id)
        hdrs = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.estrelabet.bet.br/"}
        r = requests.get(url, headers=hdrs, timeout=10)
        r.raise_for_status()
        data  = r.json()
        mkts  = data.get('markets', [])
        odds  = {o['id']: o for o in data.get('odds', [])}
        lines = []
        for mkt in mkts:
            mkt_name = mkt.get('name', '')
            ids = []
            for grp in mkt.get('desktopOddIds', []):
                if isinstance(grp, list):
                    ids.extend(grp)
                else:
                    ids.append(grp)
            for oid in ids:
                o = odds.get(oid)
                if o and o.get('oddStatus') == 0:
                    lines.append({
                        'market_name': mkt_name,
                        'odd_name':    o.get('name', ''),
                        'odd_sv':      o.get('sv', ''),
                        'price':       float(o.get('price', 0)),
                    })
        return lines
    except Exception as e:
        print(f"[markets ALT] {event_id}: {e}")
        return []


def fetch_markets_superbet(event_id):
    try:
        pure_id = str(event_id).replace('sb-', '')
        url  = SUPERBET_EVENT.format(pure_id)
        hdrs = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0',
                'Origin': 'https://superbet.bet.br', 'Referer': 'https://superbet.bet.br/'}
        r = requests.get(url, headers=hdrs, timeout=10)
        r.raise_for_status()
        data = r.json()
        evts = data.get('data', [])
        if not evts:
            return []
        lines = []
        for odd in (evts[0].get('odds') or []):
            if not isinstance(odd, dict):
                continue
            if odd.get('status') == 'active':
                lines.append({
                    'market_name': odd.get('marketName', ''),
                    'odd_name':    odd.get('name', ''),
                    'odd_sv':      odd.get('specialBetValue', ''),
                    'price':       float(odd.get('price', 0)),
                })
        return lines
    except Exception as e:
        print(f"[markets SB] {event_id}: {e}")
        return []


def fetch_markets(event_id):
    if str(event_id).startswith('sb-'):
        return fetch_markets_superbet(event_id)
    return fetch_markets_altenar(event_id)

# =============================================================================
# FETCH — HISTÓRICO (RWTIPS API)
# =============================================================================
def fetch_history(pages=20, use_cache=True):
    global history_cache
    if use_cache and history_cache['matches'] and (time.time() - history_cache['ts']) < HISTORY_TTL:
        return history_cache['matches']

    print(f"[history] buscando {pages} páginas...")
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    sess  = requests.Session()
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    sess.mount("https://", HTTPAdapter(max_retries=retry))

    def _page(p):
        try:
            r = sess.get(HISTORY_URL, params={'page': p, 'limit': 40}, timeout=15)
            return r.json().get('results', []) if r.status_code == 200 else []
        except:
            return []

    all_raw = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        for res in ex.map(_page, range(1, pages + 1)):
            all_raw.extend(res)

    def _fix_dt(s):
        s = str(s or '')
        if s and not s.endswith('Z') and not re.search(r'[+-]\d{2}:\d{2}$', s):
            s += 'Z'
        return s

    matches = []
    for m in all_raw:
        league = map_league(m.get('league_mapped') or m.get('league_name', ''))
        h_raw  = (m.get('home_nick') or m.get('home_player') or
                  m.get('home_player_name') or m.get('home_competitor_name', ''))
        a_raw  = (m.get('away_nick') or m.get('away_player') or
                  m.get('away_player_name') or m.get('away_competitor_name', ''))
        home_p = normalize_nick(h_raw) or h_raw.strip()
        away_p = normalize_nick(a_raw) or a_raw.strip()

        if m.get('finished_at'):
            data_r = _fix_dt(m['finished_at'])
        elif m.get('match_date'):
            data_r = f"{m['match_date']}T{m.get('match_time', '00:00:00')}Z"
        else:
            data_r = datetime.now(timezone.utc).isoformat()

        started = _fix_dt(m.get('started_at', ''))

        matches.append({
            'id': m.get('event_id') or m.get('id') or '',
            'league_name': league,
            'home_player': home_p,
            'away_player': away_p,
            'home_team':   m.get('home_raw') or m.get('home_team', ''),
            'away_team':   m.get('away_raw') or m.get('away_team', ''),
            'data_realizacao': data_r,
            'started_at':      started,
            'home_score_ht': int(m.get('home_score_ht') or 0),
            'away_score_ht': int(m.get('away_score_ht') or 0),
            'home_score_ft': int(m.get('home_score_ft') or 0),  # TOTAL do jogo
            'away_score_ft': int(m.get('away_score_ft') or 0),  # TOTAL do jogo
        })

    matches.sort(key=lambda x: x['data_realizacao'], reverse=True)
    history_cache = {'matches': matches, 'ts': time.time()}
    print(f"[history] {len(matches)} partidas carregadas")
    # Pré-computar stats de todos os players e ligas vistos
    _rebuild_stats_cache(matches)
    return matches


def _rebuild_stats_cache(matches):
    """
    Pré-computa e armazena as estatísticas de TODOS os players e ligas
    encontrados no histórico. Chamado automaticamente após fetch_history.

    Custo: único — percorre a lista uma vez por player único.
    Benefício: main_loop faz lookup O(1) em vez de varredura O(n) por ciclo.
    """
    global stats_cache

    # Coletar todos os nicks únicos
    nicks = set()
    for m in matches:
        h = extract_nick(m.get('home_player', ''))
        a = extract_nick(m.get('away_player', ''))
        if h: nicks.add(h)
        if a: nicks.add(a)

    # Coletar todas as ligas únicas
    leagues = set(m.get('league_name', '') for m in matches if m.get('league_name'))

    new_players = {}
    new_leagues = {}

    # Log das ligas únicas no histórico — ajuda a identificar nomes não mapeados
    mapped_leagues   = set()
    unmapped_leagues = set()
    for lg in sorted(leagues):
        mapped = map_league(lg)
        if mapped != lg:
            mapped_leagues.add(f"{lg!r} → {mapped!r}")
        else:
            unmapped_leagues.add(lg)

    # Computar stats por player
    for nick in nicks:
        st = player_stats(nick, matches, last_n=5)
        if st:
            new_players[nick] = st

    # Computar stats por liga (nome já mapeado pelo fetch_history)
    for lg in leagues:
        st = league_stats(lg, matches, last_n=5)
        if st and not st.get('estimated'):
            new_leagues[lg] = st

    stats_cache = {
        'players': new_players,
        'leagues': new_leagues,
        'ts':      time.time(),
    }
    print(f"[stats_cache] {len(new_players)} players | {len(new_leagues)} ligas")
    # Ligas com dados reais no histórico
    known = sorted(new_leagues.keys())
    print(f"[stats_cache] ligas com dados: {known}")
    # Ligas do LEAGUE_PROFILES sem dados no histórico
    missing = [lg for lg in LEAGUE_PROFILES if lg != 'DEFAULT' and lg not in new_leagues]
    if missing:
        print(f"[stats_cache] ligas SEM dados (EST): {missing}")


def get_player_stats_cached(player_name):
    """Retorna stats do cache. Se expirado, reconstrói antes de responder."""
    nick = extract_nick(player_name)
    if not nick:
        return None
    # Cache expirado? Forçar rebuild
    if time.time() - stats_cache['ts'] > STATS_TTL:
        matches = history_cache.get('matches', [])
        if matches:
            _rebuild_stats_cache(matches)
    return stats_cache['players'].get(nick)


def get_league_stats_cached(league_name):
    """Retorna stats da liga do cache. Se expirado, reconstrói antes."""
    if time.time() - stats_cache['ts'] > STATS_TTL:
        matches = history_cache.get('matches', [])
        if matches:
            _rebuild_stats_cache(matches)
    st = stats_cache['leagues'].get(league_name)
    if st:
        return st
    # Liga sem dados suficientes — retorna estimativa baseada no perfil
    prof = get_profile(league_name)
    crit = get_crit(league_name)
    return {
        'avg_ht':   crit['ht_gate_league'],
        'avg_ft':   crit['ft_gate_league'],
        'games':    0,
        'estimated': True,
    }

# =============================================================================
# ESTATÍSTICAS — PLAYER E LIGA (últimos 5 jogos)
# =============================================================================
def player_stats(player_name, all_matches, last_n=5):
    """
    Calcula estatísticas de um jogador nos últimos N jogos
    (independente do adversário).

    Retorna médias de gols MARCADOS e SOFRIDOS para HT e FT,
    necessárias para critérios de BTTS e linhas individuais.

    home_score_ft = gols TOTAIS do jogo inteiro (inclui HT)
    home_score_ht = gols só do 1T
    """
    nick = extract_nick(player_name)
    games = []
    for m in all_matches:
        h = extract_nick(m.get('home_player', ''))
        a = extract_nick(m.get('away_player', ''))
        if nick in (h, a):
            games.append(m)
        if len(games) >= last_n:
            break

    n = len(games)
    if n < 3:
        return None

    ht_marc = []   # gols marcados pelo player no HT
    ht_sof  = []   # gols sofridos pelo player no HT (= gols do adversário no HT)
    ft_marc = []   # gols marcados pelo player no jogo inteiro
    ft_sof  = []   # gols sofridos pelo player no jogo inteiro

    for m in games:
        is_home = extract_nick(m.get('home_player', '')) == nick
        ht_h = int(m.get('home_score_ht', 0) or 0)
        ht_a = int(m.get('away_score_ht', 0) or 0)
        ft_h = int(m.get('home_score_ft', 0) or 0)
        ft_a = int(m.get('away_score_ft', 0) or 0)

        if is_home:
            ht_marc.append(ht_h); ht_sof.append(ht_a)
            ft_marc.append(ft_h); ft_sof.append(ft_a)
        else:
            ht_marc.append(ht_a); ht_sof.append(ht_h)
            ft_marc.append(ft_a); ft_sof.append(ft_h)

    avg_ft = sum(ft_marc) / n
    avg_ht = sum(ht_marc) / n

    # Trend: razão média dos 2 jogos mais recentes / média dos anteriores
    # > 1.0 = em alta | < 1.0 = em queda | 1.0 = estável
    if n >= 4:
        recent = sum(ft_marc[:2]) / 2
        older  = sum(ft_marc[2:]) / len(ft_marc[2:])
        trend  = round(recent / older, 2) if older > 0 else 1.0
    else:
        trend = 1.0

    # % de jogos com FT marcado >= threshold (consistência)
    pct_over_ft3 = sum(1 for v in ft_marc if v >= 3) / n   # >= 3 gols marcados
    pct_over_ft2 = sum(1 for v in ft_marc if v >= 2) / n   # >= 2 gols marcados
    pct_over_ht1 = sum(1 for v in ht_marc if v >= 1) / n   # >= 1 gol marcado no HT

    return {
        'avg_ht':      avg_ht,
        'avg_ft':      avg_ft,
        'avg_ht_sof':  sum(ht_sof)  / n,
        'avg_ft_sof':  sum(ft_sof)  / n,
        'pct_over_ft3': pct_over_ft3,
        'pct_over_ft2': pct_over_ft2,
        'pct_over_ht1': pct_over_ht1,
        'trend':        trend,
        'ht_list':      ht_marc,
        'ft_list':      ft_marc,
        'games':        n,
    }


def league_stats(league_name, all_matches, last_n=5):
    """
    Média de gols HT e FT nos últimos N jogos da liga.
    Busca tanto pelo nome mapeado quanto pelos nomes originais do histórico.
    """
    # Busca direta pelo nome mapeado
    games = [m for m in all_matches if m.get('league_name') == league_name]
    # Se não encontrou, tentar pelos nomes alternativos do histórico
    if len(games) < last_n:
        for raw, mapped in HIST_MAP.items():
            if mapped == league_name:
                extras = [m for m in all_matches if m.get('league_name') == raw]
                games.extend(extras)
        # Deduplica preservando ordem
        seen = set()
        uniq = []
        for g in games:
            gid = g.get('id', id(g))
            if gid not in seen:
                seen.add(gid)
                uniq.append(g)
        games = uniq
    games = games[:last_n]
    n = len(games)
    if n < 3:
        return {'avg_ht': 3.5, 'avg_ft': 6.0, 'games': 0, 'estimated': True}

    ht_totals = [int(m.get('home_score_ht', 0) or 0) + int(m.get('away_score_ht', 0) or 0)
                 for m in games]
    ft_totals = [int(m.get('home_score_ft', 0) or 0) + int(m.get('away_score_ft', 0) or 0)
                 for m in games]
    btts_ht   = [1 if int(m.get('home_score_ht', 0) or 0) > 0
                      and int(m.get('away_score_ht', 0) or 0) > 0 else 0 for m in games]
    btts_ft   = [1 if int(m.get('home_score_ft', 0) or 0) > 0
                      and int(m.get('away_score_ft', 0) or 0) > 0 else 0 for m in games]

    return {
        'avg_ht':      sum(ht_totals) / n,
        'avg_ft':      sum(ft_totals) / n,
        'btts_ht_pct': sum(btts_ht) / n * 100,
        'btts_ft_pct': sum(btts_ft) / n * 100,
        'games':       n,
        'estimated':   False,
    }

# =============================================================================
# BUSCA DE ODD NO MERCADO
# =============================================================================
def find_odd(open_lines, category, value=None, player_raw=None, min_odd=1.55):
    """
    Retorna a melhor odd para a estratégia pedida.

    category: 'ht_total' | 'ft_total' | 'ht_btts' | 'ft_btts' | 'individual'
    value: float linha (0.5, 1.5, ...) ou None para BTTS
    player_raw: nome bruto do jogador para bets individuais
    """
    best = None
    best_mkt = ''
    for ln in open_lines:
        mkt   = ln.get('market_name', '').lower()
        name  = ln.get('odd_name', '').lower()
        price = float(ln.get('price', 0))

        if price < min_odd:
            continue
        if 'menos' in name or 'under' in name:
            continue

        sv_val = None
        sv_raw = str(ln.get('odd_sv', '')).replace('+', '').strip()
        try:
            sv_val = float(sv_raw.split('|')[-1].strip())
        except:
            pass

        # HT: mercado de 1º tempo — NÃO inclui 'tempo' sozinho ('Tempo Normal' é FT)
        is_ht_mkt = any(x in mkt for x in
                        ['1º', '1ª', '1st', 'primeiro', 'half', 'período', '1 per'])
        # FIX: mercados individuais ('Rose - Total de Gols') têm jogador antes do ' - '
        _mkt_prefix = ln.get('market_name', '').split(' - ')[0].lower()
        _is_player_mkt = (' - ' in ln.get('market_name', '') and
                          any(x in mkt for x in ['total', 'gol', 'score']) and
                          not any(x in _mkt_prefix for x in
                                  ['1º', '1ª', 'total', 'gol', 'ambas', 'ambos']))
        is_total_mkt = (any(x in mkt for x in ['total', 'gol', 'score', 'num'])
                        and not _is_player_mkt)
        is_over = any(x in name for x in ['mais', 'over', 'acima', '+'])
        is_btts = any(x in mkt for x in ['ambas', 'btts', 'ambos'])

        if category == 'ht_total':
            if is_ht_mkt and is_total_mkt and is_over and sv_val is not None:
                if abs(sv_val - value) < 0.1:
                    if best is None or price > best:
                        best = price

        elif category == 'ft_total':
            if not is_ht_mkt and is_total_mkt and is_over and sv_val is not None:
                if abs(sv_val - value) < 0.1:
                    if best is None or price > best:
                        best = price

        elif category == 'ht_btts':
            if is_ht_mkt and is_btts:
                if 'sim' in name or 'yes' in name or 'ambas' in name or 'both' in name:
                    if best is None or price > best:
                        best = price

        elif category == 'ft_btts':
            if not is_ht_mkt and is_btts:
                if 'sim' in name or 'yes' in name or 'ambas' in name or 'both' in name:
                    if best is None or price > best:
                        best = price
                        best_mkt = f"{ln.get('market_name','')} | {ln.get('odd_name','')}"

        elif category == 'individual' and player_raw:
            # FIX: usar só o nick limpo — 'Man City (fantazer)' → busca 'fantazer'
            p_nick_lo = extract_nick(player_raw).lower()
            in_mkt  = p_nick_lo in mkt
            in_name = p_nick_lo in name
            if (in_mkt or in_name) and is_over and sv_val is not None:
                if abs(sv_val - value) < 0.1:
                    if best is None or price > best:
                        best = price

    if best and 'btts' in str(best_mkt).lower() or 'ambas' in str(best_mkt).lower():
        print(f"[find_odd] BTTS match: '{best_mkt}' @ {best}")
    return best

# =============================================================================
# AVALIAÇÃO DAS ESTRATÉGIAS
# =============================================================================
def evaluate_strategies(event, p1_st, p2_st, lg_st, open_lines):
    """
    Avalia estratégias HT (durante o 1ºT) e FT (durante o 2ºT).

    Detecção de período:
      is_ht = minute < ht_dur       → estamos no 1ºT
      is_ft = minute >= ht_dur      → estamos no 2ºT

    Regras de placar por aposta:
      HT +0.5  : 0x0
      HT +1.5  : 0x0 | 1x0 | 0x1            (total_ht <= 1)
      HT +2.5  : 1x0 | 0x1 | 1x1            (1 <= total_ht <= 2)
      HT BTTS  : total_ht <= 1 e NÃO ambos marcaram
      FT +1.5  : total_ft == 0
      FT +2.5  : total_ft < 2  (0 ou 1)
      FT +3.5  : total_ft < 3  (0, 1 ou 2)
      FT +4.5  : total_ft <= 3 (0, 1, 2 ou 3)
      FT BTTS  : total_ft <= 1 e NÃO ambos marcaram

    Gate geral HT: liga_avg_ht >= gate_league E p1_avg_ht >= gate_p E p2_avg_ht >= gate_p
    Gate geral FT: liga_avg_ft >= gate_league E p1_avg_ft >= gate_p E p2_avg_ft >= gate_p

    Resultado: max 1 tip HT + max 1 tip FT (melhor score de cada grupo).
    """
    candidates = {'HT': [], 'FT': []}

    league_key = event.get('mappedLeague', '')
    crit        = get_crit(league_key)
    profile     = get_profile(league_key)
    ht_dur      = profile['ht_dur']    # minutos do 1ºT
    duration    = profile['duration']  # duração total do jogo
    min_odd     = crit['min_odd']

    timer    = event.get('timer', {})
    minute   = timer.get('minute', 0)
    second   = timer.get('second', 0)
    score    = event.get('score', {})
    hg       = score.get('home', 0)    # gols home acumulados até agora
    ag       = score.get('away', 0)    # gols away acumulados até agora
    total_ft = hg + ag                 # total de gols no jogo até agora

    # Durante o 1ºT: hg/ag são os gols do HT
    total_ht = total_ft

    home_raw = event.get('homeRaw', event.get('homePlayer', ''))
    away_raw = event.get('awayRaw', event.get('awayPlayer', ''))

    elapsed_sec  = minute * 60 + second
    is_ht        = elapsed_sec < ht_dur * 60   # ainda no 1ºT
    is_ft        = elapsed_sec >= ht_dur * 60  # já no 2ºT

    # Stats dos players
    p1_ht_marc = p1_st.get('avg_ht', 0)
    p1_ht_sof  = p1_st.get('avg_ht_sof', 0)
    p1_ft_marc = p1_st.get('avg_ft', 0)
    p1_ft_sof  = p1_st.get('avg_ft_sof', 0)

    p2_ht_marc = p2_st.get('avg_ht', 0)
    p2_ht_sof  = p2_st.get('avg_ht_sof', 0)
    p2_ft_marc = p2_st.get('avg_ft', 0)
    p2_ft_sof  = p2_st.get('avg_ft_sof', 0)

    # Stats da liga
    lg_avg_ht = lg_st.get('avg_ht', 0)
    lg_avg_ft = lg_st.get('avg_ft', 0)

    def skip(reason):
        print(f"    [SKIP] {reason}")

    # ── Gate geral HT ──────────────────────────────────────────────
    ht_gate = (
        lg_avg_ht  >= crit['ht_gate_league'] and
        p1_ht_marc >= crit['ht_gate_p'] and
        p2_ht_marc >= crit['ht_gate_p']
    )

    # ── Gate geral FT ──────────────────────────────────────────────
    ft_gate = (
        lg_avg_ft  >= crit['ft_gate_league'] and
        p1_ft_marc >= crit['ft_gate_p'] and
        p2_ft_marc >= crit['ft_gate_p']
    )

    # ══════════════════════════════════════════════════════════════
    # APOSTAS HT — só no 1ºT (is_ht) e só se gate HT passou
    # ══════════════════════════════════════════════════════════════
    if is_ht:
        if not ht_gate:
            skip(f"Gate HT: liga={lg_avg_ht:.1f}(min {crit['ht_gate_league']}) "
                 f"p1={p1_ht_marc:.1f} p2={p2_ht_marc:.1f}(min {crit['ht_gate_p']})")
        else:
            # ── +0.5 HT | Placar: 0x0 ──────────────────────────────
            c = crit['ht_05']
            if hg == 0 and ag == 0:
                if p1_ht_marc >= c['p1_marc'] and p2_ht_marc >= c['p2_marc']:
                    odd = find_odd(open_lines, 'ht_total', 0.5, min_odd=min_odd)
                    if odd:
                        candidates['HT'].append({
                            'name': '⚽ +0.5 GOL HT',
                            'odd': odd, 'category': 'HT',
                            'score': (p1_ht_marc + p2_ht_marc) * (odd - 1),
                        })
            else:
                skip(f"+0.5 HT: placar {hg}x{ag} (precisa 0x0)")

            # ── +1.5 HT | Placar: 0x0 | 1x0 | 0x1 ────────────────
            c = crit['ht_15']
            if total_ht <= 1:
                if p1_ht_marc >= c['p1_marc'] and p2_ht_marc >= c['p2_marc']:
                    odd = find_odd(open_lines, 'ht_total', 1.5, min_odd=min_odd)
                    if odd:
                        candidates['HT'].append({
                            'name': '⚽ +1.5 GOLS HT',
                            'odd': odd, 'category': 'HT',
                            'score': (p1_ht_marc + p2_ht_marc) * (odd - 1),
                        })
            else:
                skip(f"+1.5 HT: placar {hg}x{ag} total={total_ht} (precisa <=1)")

            # ── +2.5 HT | Placar: 1x0 | 0x1 | 1x1 ────────────────
            c = crit['ht_25']
            if 1 <= total_ht <= 2:
                if p1_ht_marc >= c['p1_marc'] and p2_ht_marc >= c['p2_marc']:
                    odd = find_odd(open_lines, 'ht_total', 2.5, min_odd=min_odd)
                    if odd:
                        candidates['HT'].append({
                            'name': '⚽ +2.5 GOLS HT',
                            'odd': odd, 'category': 'HT',
                            'score': (p1_ht_marc + p2_ht_marc) * (odd - 1),
                        })
            else:
                skip(f"+2.5 HT: placar {hg}x{ag} total={total_ht} (precisa 1-2)")

            # ── BTTS HT | Placar: total <= 1, NÃO ambos marcaram ──
            c = crit['ht_btts']
            if total_ht <= 1 and not (hg > 0 and ag > 0):
                if (p1_ht_marc >= c['p1_marc'] and p1_ht_sof >= c['p1_sof'] and
                        p2_ht_marc >= c['p2_marc'] and p2_ht_sof >= c['p2_sof']):
                    odd = find_odd(open_lines, 'ht_btts', min_odd=min_odd)
                    if odd:
                        candidates['HT'].append({
                            'name': '⚽ BTTS HT',
                            'odd': odd, 'category': 'HT',
                            'score': (p1_ht_marc + p2_ht_marc) / 2 * (odd - 1),
                        })
            else:
                if total_ht > 1:
                    skip(f"BTTS HT: total={total_ht} (precisa <=1)")
                elif hg > 0 and ag > 0:
                    skip(f"BTTS HT: placar {hg}x{ag} — ambos já marcaram")

    # ══════════════════════════════════════════════════════════════
    # APOSTAS FT — só no 2ºT (is_ft) e só se gate FT passou
    # ══════════════════════════════════════════════════════════════
    if is_ft:
        # Penalidade: se o jogo foi 0-0 no HT completo, exigir avg_ft maior
        ht_was_zero = (hg + ag == 0)
        ft_gate_adj = ft_gate
        if ht_was_zero:
            adj_p1 = p1_ft_marc >= crit['ft_gate_p'] * 1.3
            adj_p2 = p2_ft_marc >= crit['ft_gate_p'] * 1.3
            adj_lg = lg_avg_ft  >= crit['ft_gate_league'] * 1.1
            ft_gate_adj = adj_p1 and adj_p2 and adj_lg
            if ft_gate and not ft_gate_adj:
                skip(f"Gate FT ajustado (HT 0-0): "
                     f"p1={p1_ft_marc:.1f}(min {crit['ft_gate_p']*1.3:.1f}) "
                     f"p2={p2_ft_marc:.1f}(min {crit['ft_gate_p']*1.3:.1f})")

        # Gate extra para ADRIATIC: liga com alta variância, exige mais
        if league_key == 'ADRIATIC':
            adj_p1 = p1_ft_marc >= crit['ft_gate_p'] * 1.2
            adj_p2 = p2_ft_marc >= crit['ft_gate_p'] * 1.2
            adj_lg = lg_avg_ft  >= crit['ft_gate_league'] * 1.1
            ft_gate_adriatic = adj_p1 and adj_p2 and adj_lg
            if ft_gate and not ft_gate_adriatic:
                skip(f"Gate FT ADRIATIC (×1.2): "
                     f"p1={p1_ft_marc:.1f}(min {crit['ft_gate_p']*1.2:.1f}) "
                     f"p2={p2_ft_marc:.1f}(min {crit['ft_gate_p']*1.2:.1f})")
                ft_gate_adj = False

        if not ft_gate or not ft_gate_adj:
            skip(f"Gate FT: liga={lg_avg_ft:.1f}(min {crit['ft_gate_league']}) "
                 f"p1={p1_ft_marc:.1f} p2={p2_ft_marc:.1f}(min {crit['ft_gate_p']})")
        else:
            # ── +1.5 FT | Placar: 0 gols ────────────────────────────
            c = crit['ft_15']
            if total_ft == 0:
                pct_ok = (p1_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0)
                          and p2_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0))
                if p1_ft_marc >= c['p1_marc'] and p2_ft_marc >= c['p2_marc'] and pct_ok:
                    odd = find_odd(open_lines, 'ft_total', 1.5, min_odd=min_odd)
                    if odd:
                        candidates['FT'].append({
                            'name': '⚽ +1.5 GOLS FT (TOTAL)',
                            'odd': odd, 'category': 'FT',
                            'score': (p1_ft_marc + p2_ft_marc) * (odd - 1),
                        })
                elif not pct_ok:
                    skip(f"+1.5 FT: inconsistência p1={p1_st.get('pct_over_ft2',0):.0%} p2={p2_st.get('pct_over_ft2',0):.0%} (min {c.get('pct_ft2',0):.0%})")
            else:
                skip(f"+1.5 FT: placar {hg}x{ag} total={total_ft} (precisa 0 gols)")

            # ── +2.5 FT | Placar: < 2 gols ─────────────────────────
            c = crit['ft_25']
            if total_ft < 2:
                pct_ok = (p1_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0)
                          and p2_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0))
                if p1_ft_marc >= c['p1_marc'] and p2_ft_marc >= c['p2_marc'] and pct_ok:
                    odd = find_odd(open_lines, 'ft_total', 2.5, min_odd=min_odd)
                    if odd:
                        candidates['FT'].append({
                            'name': '⚽ +2.5 GOLS FT (TOTAL)',
                            'odd': odd, 'category': 'FT',
                            'score': (p1_ft_marc + p2_ft_marc) * (odd - 1),
                        })
                elif not pct_ok:
                    skip(f"+2.5 FT: inconsistência p1={p1_st.get('pct_over_ft2',0):.0%} p2={p2_st.get('pct_over_ft2',0):.0%} (min {c.get('pct_ft2',0):.0%})")
            else:
                skip(f"+2.5 FT: placar {hg}x{ag} total={total_ft} (precisa <2)")

            # ── +3.5 FT | Placar: < 3 gols + HT >= 3 gols (ritmo alto) ─
            c = crit['ft_35']
            if total_ft < 3:
                # Exige ritmo alto no 1ºT: HT total >= 3 gols
                # Dados: todos os reds do +3.5 tiveram HT com 1-2 gols
                ht_ritmo_ok = total_ht >= 3
                pct_ok = (p1_st.get('pct_over_ft3', 1.0) >= c.get('pct_ft3', 0)
                          and p2_st.get('pct_over_ft3', 1.0) >= c.get('pct_ft3', 0))
                if p1_ft_marc >= c['p1_marc'] and p2_ft_marc >= c['p2_marc'] and pct_ok and ht_ritmo_ok:
                    odd = find_odd(open_lines, 'ft_total', 3.5, min_odd=min_odd)
                    if odd:
                        candidates['FT'].append({
                            'name': '⚽ +3.5 GOLS FT (TOTAL)',
                            'odd': odd, 'category': 'FT',
                            'score': (p1_ft_marc + p2_ft_marc) * (odd - 1),
                        })
                elif not ht_ritmo_ok:
                    skip(f"+3.5 FT: ritmo HT={total_ht} insuficiente (precisa >=3)")
                elif not pct_ok:
                    skip(f"+3.5 FT: inconsistência p1={p1_st.get('pct_over_ft3',0):.0%} p2={p2_st.get('pct_over_ft3',0):.0%} (min {c.get('pct_ft3',0):.0%})")
            else:
                skip(f"+3.5 FT: placar {hg}x{ag} total={total_ft} (precisa <3)")

            # ── +4.5 FT | Placar: <= 3 gols + HT >= 2 gols ───────────
            c = crit['ft_45']
            if total_ft <= 3:
                # Exige ritmo mínimo no 1ºT: HT total >= 2 gols
                # Dados: todos os reds do +4.5 tiveram HT = 0 ou 1 gol
                ht_ritmo_ok = total_ht >= 2
                pct_ok = (p1_st.get('pct_over_ft3', 1.0) >= c.get('pct_ft3', 0)
                          and p2_st.get('pct_over_ft3', 1.0) >= c.get('pct_ft3', 0))
                if p1_ft_marc >= c['p1_marc'] and p2_ft_marc >= c['p2_marc'] and pct_ok and ht_ritmo_ok:
                    odd = find_odd(open_lines, 'ft_total', 4.5, min_odd=min_odd)
                    if odd:
                        candidates['FT'].append({
                            'name': '⚽ +4.5 GOLS FT (TOTAL)',
                            'odd': odd, 'category': 'FT',
                            'score': (p1_ft_marc + p2_ft_marc) * (odd - 1),
                        })
                elif not ht_ritmo_ok:
                    skip(f"+4.5 FT: ritmo HT={total_ht} insuficiente (precisa >=2)")
                elif not pct_ok:
                    skip(f"+4.5 FT: inconsistência p1={p1_st.get('pct_over_ft3',0):.0%} p2={p2_st.get('pct_over_ft3',0):.0%} (min {c.get('pct_ft3',0):.0%})")
            else:
                skip(f"+4.5 FT: placar {hg}x{ag} total={total_ft} (precisa <=3)")

            # ── BTTS FT | Placar: <= 1 gol, NÃO ambos marcaram ─────
            c = crit['ft_btts']
            if total_ft <= 1 and not (hg > 0 and ag > 0):
                pct_ok = (p1_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0)
                          and p2_st.get('pct_over_ft2', 1.0) >= c.get('pct_ft2', 0))
                if (p1_ft_marc >= c['p1_marc'] and p1_ft_sof >= c['p1_sof'] and
                        p2_ft_marc >= c['p2_marc'] and p2_ft_sof >= c['p2_sof'] and pct_ok):
                    odd = find_odd(open_lines, 'ft_btts', min_odd=min_odd)
                    if odd:
                        # Odd alta em BTTS = mercado sabe que um player domina
                        # O único RED do BTTS foi com odd 6.75
                        if odd > 4.5:
                            skip(f"BTTS FT: odd={odd} muito alta (>4.5 indica domínio de 1 player)")
                        else:
                            candidates['FT'].append({
                                'name': '⚽ BTTS FT (TOTAL)',
                                'odd': odd, 'category': 'FT',
                                'score': (p1_ft_marc + p2_ft_marc) / 2 * (odd - 1),
                            })
            else:
                if total_ft > 1:
                    skip(f"BTTS FT: total={total_ft} (precisa <=1)")
                elif hg > 0 and ag > 0:
                    skip(f"BTTS FT: placar {hg}x{ag} — ambos já marcaram")

    # ── Debug: logar se HT não encontrou linhas ─────────────────
    if is_ht and not candidates['HT'] and ht_gate:
        mkt_names = list(set(ln.get('market_name', '') for ln in open_lines))
        has_ht_mkt = any(any(x in m.lower() for x in ['1º', '1ª', '1st', 'half', 'primeiro'])
                         for m in mkt_names)
        if not has_ht_mkt:
            skip(f"HT: nenhum mercado de 1ºT disponível na casa | mercados: {mkt_names[:5]}")
        else:
            skip(f"HT: mercados existem mas placar/critérios bloquearam")

    # ── Seleção: melhor HT + melhor FT ─────────────────────────────
    chosen = []
    for cat in ('HT', 'FT'):
        if candidates[cat]:
            candidates[cat].sort(key=lambda x: x['score'], reverse=True)
            chosen.append(candidates[cat][0])
            if len(candidates[cat]) > 1:
                print(f"    [{cat} descartados] {[c['name'] for c in candidates[cat][1:]]}")
    return chosen


# =============================================================================
# FORMATAÇÃO DE MENSAGENS
# =============================================================================
def _bar(val, max_val=6.0):
    """
    Barra visual de progresso.
    Usa '●' para preenchido e '○' para vazio — contraste claro no Telegram dark/light.
    """
    filled = min(10, round(val / max_val * 10))
    return "●" * filled + "○" * (10 - filled)


def format_tip(event, strategy, odd, p1_st, p2_st, lg_st):
    home   = event.get('homePlayer', '?')
    away   = event.get('awayPlayer', '?')
    timer  = event.get('timer', {}).get('formatted', '00:00')
    score  = event.get('scoreboard', '0-0')
    league = event.get('mappedLeague') or event.get('leagueName', '?')

    p1_ft  = p1_st.get('avg_ft', 0) if p1_st else 0
    p2_ft  = p2_st.get('avg_ft', 0) if p2_st else 0
    p1_ht  = p1_st.get('avg_ht', 0) if p1_st else 0
    p2_ht  = p2_st.get('avg_ht', 0) if p2_st else 0
    lg_ht  = lg_st.get('avg_ht', 0) if lg_st else 0
    lg_ft  = lg_st.get('avg_ft', 0) if lg_st else 0
    max_ft = max(p1_ft, p2_ft, 4.0)

    eid     = event.get('id', '')
    sb_link = event.get('superbetLink', '')
    if sb_link:
        link = f'🔗 <a href="{sb_link}">VER AO VIVO</a>'
    elif eid:
        clean = str(eid).replace('sb-', '')
        url   = f"https://www.estrelabet.bet.br/apostas-ao-vivo?page=liveEvent&eventId={clean}&sportId=66"
        link  = f'🔗 <a href="{url}">VER AO VIVO</a>'
    else:
        link  = ''

    # Linha de aposta limpa
    strat_clean = strategy.replace('⚽ ', '').replace('GOLS ', '').replace(' (TOTAL)', '')

    msg  = f"🎯 <b>{strat_clean}</b>  <code>@ {odd}</code>\n"
    msg += f"🏆 {league}\n"
    msg += f"⏱ {timer}   📊 {score}\n"
    msg += "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
    msg += f"<b>{home}</b>\n"
    msg += f"  HT {p1_ht:.1f}g  FT {p1_ft:.1f}g  {_bar(p1_ft, max_ft)}\n"
    msg += f"<b>{away}</b>\n"
    msg += f"  HT {p2_ht:.1f}g  FT {p2_ft:.1f}g  {_bar(p2_ft, max_ft)}\n"
    msg += "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
    msg += f"Liga: HT {lg_ht:.1f}g/j  ·  FT {lg_ft:.1f}g/j"
    if link:
        msg += f"\n{link}"
    return msg


def format_result(tip, ht_h, ht_a, ft_h, ft_a, result):
    emoji  = "✅" if result == 'green' else "❌"
    status = "GREEN" if result == 'green' else "RED"
    strat  = tip.get('strategy', '')
    league = tip.get('league', '')
    home   = tip.get('home_player', '?')
    away   = tip.get('away_player', '?')
    odd    = tip.get('sent_odd', '')
    ft_tot = ft_h + ft_a
    ht_tot = ht_h + ht_a
    strat_clean = strat.replace('⚽ ', '').replace('GOLS ', '').replace(' (TOTAL)', '')

    msg  = f"{emoji} <b>{status}</b> — {league}\n"
    msg += f"<b>{strat_clean}</b>"
    if odd:
        msg += f"  <code>@ {odd}</code>"
    msg += "\n"
    msg += "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
    msg += f"HT <b>{ht_h}-{ht_a}</b> ({ht_tot}g)  →  FT <b>{ft_h}-{ft_a}</b> ({ft_tot}g)\n"
    # Para apostas HT, mostrar só placar HT
    if 'HT' in strat and 'FT' not in strat:
        msg += f"1ºT: {home} {ht_h}g  ·  {away} {ht_a}g"
    # Para apostas individuais, destacar o player apostado
    elif '(TOTAL)' not in strat and 'BTTS' not in strat:
        home_nick = extract_nick(home)
        away_nick = extract_nick(away)
        strat_up  = strat.upper()
        if home_nick and home_nick in strat_up:
            msg += f"<b>{home}</b>: {ft_h}g  ·  {away}: {ft_a}g"
        elif away_nick and away_nick in strat_up:
            msg += f"{home}: {ft_h}g  ·  <b>{away}</b>: {ft_a}g"
        else:
            msg += f"{home}: {ft_h}g  ·  {away}: {ft_a}g"
    else:
        msg += f"{home}: {ft_h}g  ·  {away}: {ft_a}g"
    return msg

# =============================================================================
# MATCH DE RESULTADO
# =============================================================================
def find_result_match(tip, recent):
    """
    Encontra a partida finalizada correspondente a uma tip.
    Usa nome dos jogadores + janela temporal.
    """
    h_nick = extract_nick(tip.get('homeRaw') or tip.get('home_player', ''))
    a_nick = extract_nick(tip.get('awayRaw') or tip.get('away_player', ''))
    if not h_nick or not a_nick:
        return None

    tip_time = tip['sent_time']
    if isinstance(tip_time, str):
        tip_time = datetime.fromisoformat(tip_time)
    if tip_time.tzinfo is None:
        tip_time = tip_time.replace(tzinfo=MANAUS_TZ)

    best, best_diff = None, float('inf')

    for m in recent:
        m_h = extract_nick(m.get('home_player', '') or m.get('home_team', '') or
                           m.get('home_nick', ''))
        m_a = extract_nick(m.get('away_player', '') or m.get('away_team', '') or
                           m.get('away_nick', ''))

        # Aceita match parcial (nick contido)
        h_ok = h_nick == m_h or h_nick in m_h or m_h in h_nick
        a_ok = a_nick == m_a or a_nick in m_a or m_a in a_nick
        # Aceita também match cruzado (home/away trocados — raro mas possível)
        h_ok_inv = h_nick == m_a or h_nick in m_a or m_a in h_nick
        a_ok_inv = a_nick == m_h or a_nick in m_h or m_h in a_nick
        if not ((h_ok and a_ok) or (h_ok_inv and a_ok_inv)):
            continue

        # Verificar se tem placar final
        ft_h = m.get('home_score_ft')
        ft_a = m.get('away_score_ft')
        if ft_h is None or ft_a is None:
            continue

        # Janela temporal: entre -15 min e +90 min após envio da tip
        dt_str = m.get('started_at') or m.get('data_realizacao', '')
        try:
            if 'T' in str(dt_str) or 'Z' in str(dt_str):
                dt = datetime.fromisoformat(str(dt_str).replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(str(dt_str), '%d/%m/%Y %H:%M:%S')
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(MANAUS_TZ)
            delta = (dt - tip_time).total_seconds()
            # Janela: -45min a +90min
            # Negativa larga porque tips FT são enviadas no 2ºT —
            # o jogo já tem ht_dur minutos quando a tip sai.
            # Exemplo: ADRIATIC (10 min) — tip enviada no min 6 (2ºT),
            # logo started_at está 6 min antes da tip → delta = -360s.
            # Com -15min a janela não cobria, com -45min cobre todas as ligas.
            if -45 * 60 <= delta <= 90 * 60:
                diff = abs(delta)
                if diff < best_diff:
                    best_diff = diff
                    best = m
        except:
            continue

    return best

# =============================================================================
# ENVIO DE TIP
# =============================================================================
async def send_tip(bot, event, tip_info, p1_st, p2_st, lg_st):
    """Envia uma tip e registra no estado global."""
    global sent_tips, sent_keys

    strategy = tip_info['name']
    odd      = tip_info['odd']
    category = tip_info.get('category', 'FT')  # 'HT' ou 'FT'
    event_id = event.get('id')
    key      = f"{event_id}_{category}"

    if key in sent_keys:
        print(f"[SKIP] {key} já enviado")
        return

    # Verificar cooldown dos jogadores
    for raw in [event.get('homeRaw', ''), event.get('awayRaw', '')]:
        blocked, mins = is_player_blocked(raw)
        if blocked:
            print(f"[COOLDOWN] {extract_nick(raw)} bloqueado ({mins:.0f}min restantes)")
            return

    home  = event.get('homePlayer', '')
    away  = event.get('awayPlayer', '')
    timer = event.get('timer', {})

    for attempt in range(3):
        try:
            msg = format_tip(event, strategy, odd, p1_st, p2_st, lg_st)
            obj = await bot.send_message(
                chat_id=CHAT_ID, text=msg,
                parse_mode="HTML", disable_web_page_preview=True
            )
            sent_keys.add(key)
            sent_tips.append({
                'event_id':     event_id,
                'strategy':     strategy,
                'category':     category,
                'sent_time':    datetime.now(MANAUS_TZ),
                'status':       'pending',
                'message_id':   obj.message_id,
                'home_player':  home,
                'away_player':  away,
                'homeRaw':      event.get('homeRaw', ''),
                'awayRaw':      event.get('awayRaw', ''),
                'league':       event.get('mappedLeague', ''),
                'sent_minute':  timer.get('minute', 0),
                'sent_odd':     odd,
                'scoreboard':   event.get('scoreboard', '0-0'),
                'startDateRaw': event.get('startDateRaw', ''),
                'liveTimeRaw':  event.get('liveTimeRaw', ''),
            })
            save_state()
            print(f"[✓] TIP: {strategy} @ {odd} — {home} vs {away}")
            break
        except Exception as e:
            print(f"[send_tip] tentativa {attempt + 1}: {e}")
            if attempt < 2:
                await asyncio.sleep(2)

# =============================================================================
# VERIFICAÇÃO DE RESULTADOS
# =============================================================================
async def check_results(bot):
    global sent_tips, daily_stats, last_summary, last_daily_date

    try:
        # Usar o cache já atualizado pelo history_refresher em background
        recent = history_cache.get('matches', [])
        if not recent:
            recent = fetch_history(pages=20, use_cache=True)
        today  = datetime.now(MANAUS_TZ)
        today_str = today.strftime('%Y-%m-%d')

        # Manter apenas últimos 5 dias de tips
        cutoff = (today - timedelta(days=5)).date()
        sent_tips[:] = [
            t for t in sent_tips
            if (t['sent_time'].date() >= cutoff
                if isinstance(t['sent_time'], datetime) else True)
        ]

        for tip in sent_tips:
            if tip.get('status') != 'pending':
                continue

            elapsed = (datetime.now(MANAUS_TZ) - tip['sent_time']).total_seconds()

            # Aguardar tempo mínimo baseado no tipo e na duração da liga
            cat      = tip.get('category', 'FT')
            lg_key   = tip.get('league', '')
            prof     = get_profile(lg_key)
            duration = prof.get('duration', 8)   # minutos totais do jogo
            ht_dur   = prof.get('ht_dur', 4)
            # HT: aguardar o fim do HT + 1 min de margem para a API atualizar
            # FT: aguardar o fim do jogo inteiro + 1 min
            if cat == 'HT':
                min_wait = (ht_dur + 1) * 60
            else:
                min_wait = (duration + 2) * 60

            if elapsed < min_wait:
                continue

            matched = find_result_match(tip, recent)
            if not matched:
                if elapsed > 3600:
                    tip['status'] = 'expired'
                    h_nick = extract_nick(tip.get('homeRaw') or tip.get('home_player', ''))
                    a_nick = extract_nick(tip.get('awayRaw') or tip.get('away_player', ''))
                    # Log para diagnóstico: mostrar nicks buscados
                    print(f"[EXPIRADO] buscando '{h_nick}' vs '{a_nick}' | {tip.get('league')} | {elapsed/60:.0f}min")
                else:
                    print(f"[AGUARDANDO] {tip.get('home_player')} vs {tip.get('away_player')} "
                          f"| {tip.get('league')} | {elapsed/60:.0f}min (min_wait={min_wait//60}min)")
                continue

            strat  = tip['strategy']
            ht_h   = int(matched.get('home_score_ht', 0) or 0)
            ht_a   = int(matched.get('away_score_ht', 0) or 0)
            ft_h   = int(matched.get('home_score_ft', 0) or 0)
            ft_a   = int(matched.get('away_score_ft', 0) or 0)
            ht_tot = ht_h + ht_a
            ft_tot = ft_h + ft_a

            # home_score_ft = TOTAL do jogo (já inclui HT)
            result = None

            # ── HT ──────────────────────────────────────────────────────
            if   '+0.5 GOL HT'  in strat: result = 'green' if ht_tot >= 1 else 'red'
            elif '+1.5 GOLS HT' in strat: result = 'green' if ht_tot >= 2 else 'red'
            elif '+2.5 GOLS HT' in strat: result = 'green' if ht_tot >= 3 else 'red'
            elif 'BTTS HT'      in strat: result = 'green' if ht_h > 0 and ht_a > 0 else 'red'
            # ── FT TOTAL — checar (TOTAL) ANTES dos individuais ─────────
            elif '+1.5 GOLS FT (TOTAL)' in strat: result = 'green' if ft_tot >= 2 else 'red'
            elif '+2.5 GOLS FT (TOTAL)' in strat: result = 'green' if ft_tot >= 3 else 'red'
            elif '+3.5 GOLS FT (TOTAL)' in strat: result = 'green' if ft_tot >= 4 else 'red'
            elif '+4.5 GOLS FT (TOTAL)' in strat: result = 'green' if ft_tot >= 5 else 'red'
            elif 'BTTS FT (TOTAL)'      in strat: result = 'green' if ft_h > 0 and ft_a > 0 else 'red'
            # ── FT INDIVIDUAL — nick do player está no nome da estratégia
            elif '+1.5 GOLS FT' in strat or '+2.5 GOLS FT' in strat or '+3.5 GOLS FT' in strat:
                home_n   = extract_nick(tip.get('homeRaw') or tip.get('home_player', ''))
                away_n   = extract_nick(tip.get('awayRaw') or tip.get('away_player', ''))
                m_h      = extract_nick(matched.get('home_player', ''))
                strat_up = strat.upper()
                if home_n and home_n in strat_up:
                    gols = ft_h if home_n == m_h else ft_a
                elif away_n and away_n in strat_up:
                    gols = ft_a if home_n == m_h else ft_h
                else:
                    gols = ft_h if home_n == m_h else ft_a
                if   '+1.5 GOLS FT' in strat: result = 'green' if gols >= 2 else 'red'
                elif '+2.5 GOLS FT' in strat: result = 'green' if gols >= 3 else 'red'
                elif '+3.5 GOLS FT' in strat: result = 'green' if gols >= 4 else 'red'

            if result:
                tip['status'] = result
                try:
                    result_msg = format_result(tip, ht_h, ht_a, ft_h, ft_a, result)
                    await bot.edit_message_text(
                        chat_id=CHAT_ID,
                        message_id=tip['message_id'],
                        text=result_msg,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    print(f"[edit_result] {e}")

                save_result(tip, ht_h, ht_a, ft_h, ft_a)
                print(f"[{result.upper()}] {strat} | HT {ht_h}-{ht_a} FT {ft_h}-{ft_a}")

                # Atualizar LeagueManager
                lg = tip.get('league', '')
                if lg:
                    changed, lm_msg = league_manager.record(lg, result == 'green')
                    if changed and lm_msg:
                        try:
                            await bot.send_message(chat_id=CHAT_ID, text=lm_msg, parse_mode="HTML")
                        except:
                            pass

                # Cooldown do jogador
                for raw_key in ['homeRaw', 'awayRaw', 'home_player', 'away_player']:
                    update_cooldown(tip.get(raw_key, ''), result)

                # Daily stats
                dk = tip['sent_time'].strftime('%Y-%m-%d')
                daily_stats.setdefault(dk, {'green': 0, 'red': 0})
                daily_stats[dk][result] += 1

        # Limpar resolvidas
        sent_tips[:] = [t for t in sent_tips if t.get('status') == 'pending']

        # Resumo diário
        g = daily_stats.get(today_str, {}).get('green', 0)
        r = daily_stats.get(today_str, {}).get('red', 0)
        if g + r > 0:
            pct     = g / (g + r) * 100
            summary = f"<b>👑 RW TIPS</b>\n✅ {g}  ❌ {r}  📊 {pct:.1f}%"
            if summary != last_summary:
                try:
                    await bot.send_message(chat_id=CHAT_ID, text=summary, parse_mode="HTML")
                    last_summary = summary
                except:
                    pass

        # Virada de dia
        global last_daily_date
        if last_daily_date and last_daily_date != today_str:
            try:
                dates = sorted(daily_stats)[-7:]
                msg = "🚨 <b>Resumo Geral:</b>\n\n"
                for d in dates:
                    ds = daily_stats[d]
                    t  = ds['green'] + ds['red']
                    if t == 0: continue
                    p  = ds['green'] / t * 100
                    fd = datetime.strptime(d, '%Y-%m-%d').strftime('%d/%m')
                    msg += f"📅 {fd} → ✅ {ds['green']} | ❌ {ds['red']} | {p:.0f}%\n"
                await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                await bot.send_message(chat_id=CHAT_ID, text=league_manager.status(), parse_mode="HTML")
            except Exception as e:
                print(f"[virada de dia] {e}")

        if last_daily_date != today_str:
            last_daily_date = today_str
            last_summary = None   # resetar para o resumo do novo dia

        save_state()

    except Exception as e:
        print(f"[check_results] {e}")

# =============================================================================
# ATUALIZAÇÃO DE HISTÓRICO EM BACKGROUND
# =============================================================================
_last_history_refresh = 0
HISTORY_REFRESH_SEC   = 120   # atualizar histórico + stats a cada 2 min

async def history_refresher():
    """
    Atualiza o histórico e pré-computa stats em background a cada 2 min.
    Roda em paralelo com o main_loop — nunca bloqueia o loop de odds.
    """
    global _last_history_refresh
    print("[REFRESH] Iniciando atualizador de histórico...")
    await asyncio.sleep(5)   # aguarda o carregamento inicial terminar
    while True:
        try:
            now = time.time()
            if now - _last_history_refresh >= HISTORY_REFRESH_SEC:
                loop = asyncio.get_event_loop()
                # Rodar em thread para não bloquear o event loop
                await loop.run_in_executor(
                    None,
                    lambda: fetch_history(pages=30, use_cache=False)
                )
                _last_history_refresh = time.time()
                age = datetime.now(MANAUS_TZ).strftime('%H:%M:%S')
                print(f"[REFRESH] Histórico atualizado às {age} "
                      f"({len(history_cache.get('matches', []))} partidas, "
                      f"{len(stats_cache.get('players', {}))} players)")
        except Exception as e:
            print(f"[history_refresher] {e}")
        await asyncio.sleep(10)   # verifica se precisa atualizar a cada 10s


# =============================================================================
# LOOP PRINCIPAL
# =============================================================================
async def main_loop(bot):
    """
    Ciclo rápido a cada 10s:
      1. fetch_live_matches()         — ~600ms (2 requests paralelos)
      2. filtros rápidos (liga, keys) — O(1)
      3. fetch_markets em paralelo    — ~300ms × N eventos (ThreadPool)
      4. lookup stats do cache        — O(1) por player/liga
      5. evaluate_strategies          — puro cálculo, sem I/O
      6. send_tip se aprovado

    Histórico e stats são atualizados em background pelo history_refresher.
    """
    print("[LOOP] Iniciando...")
    while True:
        t_start = time.time()
        try:
            now_str = datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n[CICLO] {now_str}")

            # ── 1. Eventos ao vivo ─────────────────────────────────
            loop = asyncio.get_event_loop()
            live = await loop.run_in_executor(None, fetch_live_matches)
            _logged_liga_skips: set = set()

            if not live:
                print("[CICLO] Sem eventos ao vivo")
                await asyncio.sleep(10)
                continue

            # ── 2. Filtrar eventos que ainda precisam de tip ───────
            pending = []
            for event in live:
                eid    = event.get('id')
                mapped = event.get('mappedLeague', '')

                if not mapped or mapped == 'Unknown':
                    continue

                # Bloquear ligas não reconhecidas explicitamente no LEAGUE_PROFILES
                # Ex: "Cyber Live Arena" → mapeia para si mesma → DEFAULT → não é nossa liga
                if mapped not in LEAGUE_PROFILES or mapped == 'DEFAULT':
                    continue

                ht_done = f"{eid}_HT" in sent_keys
                ft_done = f"{eid}_FT" in sent_keys
                if ht_done and ft_done:
                    continue

                league_manager.register(mapped)
                active, reason = league_manager.is_active(mapped)
                if not active:
                    if mapped not in _logged_liga_skips:
                        print(f"  [SKIP liga] {mapped}: {reason}")
                        _logged_liga_skips.add(mapped)
                    continue

                pending.append(event)

            if not pending:
                print(f"[CICLO] {len(live)} eventos, todos já processados")
                await asyncio.sleep(10)
                continue

            print(f"[CICLO] {len(live)} ao vivo → {len(pending)} pendentes")

            # ── 3. Buscar mercados em paralelo ─────────────────────
            # Todos os eventos pendentes buscam odds ao mesmo tempo
            def _fetch_mkt(ev):
                return ev, fetch_markets(ev.get('id'))

            markets_map = {}   # {event_id: [lines]}
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
                futs = {ex.submit(_fetch_mkt, ev): ev for ev in pending}
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        ev, lines = fut.result()
                        if lines:
                            markets_map[ev.get('id')] = lines
                    except Exception as e:
                        print(f"  [fetch_mkt] {e}")

            print(f"[CICLO] Mercados: {len(markets_map)}/{len(pending)} com linhas abertas")

            # ── 4. Avaliar cada evento ─────────────────────────────
            for event in pending:
                eid    = event.get('id')
                mapped = event.get('mappedLeague', '')
                home_p = event.get('homePlayer', '')
                away_p = event.get('awayPlayer', '')

                lines = markets_map.get(eid)
                if not lines:
                    continue

                # Lookup O(1) no cache pré-computado
                p1_s = get_player_stats_cached(home_p)
                p2_s = get_player_stats_cached(away_p)
                if not p1_s or not p2_s:
                    # Diagnosticar: quantas partidas tem no histórico para cada player
                    hist_now = history_cache.get('matches', [])
                    def _count_games(nick):
                        n = extract_nick(nick)
                        return sum(1 for m in hist_now
                                   if extract_nick(m.get('home_player','')) == n
                                   or extract_nick(m.get('away_player','')) == n)
                    c1 = _count_games(home_p)
                    c2 = _count_games(away_p)
                    p1_label = f"{home_p}({'sem dados' if c1==0 else f'{c1}j<3'})"
                    p2_label = f"{away_p}({'sem dados' if c2==0 else f'{c2}j<3'})"
                    print(f"  [SKIP stats] {p1_label} | {p2_label}")
                    continue

                lg_s = get_league_stats_cached(mapped)

                sc = event.get('score', {})
                timer_now = event.get('timer', {})
                sc        = event.get('score', {})
                sc_h  = sc.get('home', 0)
                sc_a  = sc.get('away', 0)
                mn    = timer_now.get('minute', 0)
                per   = 'HT' if mn < get_profile(mapped).get('ht_dur', 4) else '2T'
                pref  = f"[{home_p[:6]}x{away_p[:6]}]"
                print(f"  {pref} {mapped} | {sc_h}-{sc_a} min {mn} ({per})")
                print(f"  {pref}   p1 {home_p}: HT={p1_s['avg_ht']:.1f} FT={p1_s['avg_ft']:.1f} "
                      f"trend={p1_s.get('trend',1.0):.1f} "
                      f"ft3={p1_s.get('pct_over_ft3',0):.0%} ({p1_s['games']}j)")
                print(f"  {pref}   p2 {away_p}: HT={p2_s['avg_ht']:.1f} FT={p2_s['avg_ft']:.1f} "
                      f"trend={p2_s.get('trend',1.0):.1f} "
                      f"ft3={p2_s.get('pct_over_ft3',0):.0%} ({p2_s['games']}j)")
                print(f"  {pref}   liga HT={lg_s['avg_ht']:.1f} FT={lg_s['avg_ft']:.1f} "
                      f"({lg_s['games']}j{' EST' if lg_s.get('estimated') else ''})")

                tips = evaluate_strategies(event, p1_s, p2_s, lg_s, lines)

                ht_done = f"{eid}_HT" in sent_keys
                ft_done = f"{eid}_FT" in sent_keys
                for tip_info in tips:
                    cat = tip_info.get('category', 'FT')
                    if cat == 'HT' and ht_done:
                        continue
                    if cat == 'FT' and ft_done:
                        continue
                    await send_tip(bot, event, tip_info, p1_s, p2_s, lg_s)
                    await asyncio.sleep(0.5)

            elapsed = time.time() - t_start
            print(f"[CICLO] Concluído em {elapsed:.1f}s — aguardando 10s...")
            save_state()
            await asyncio.sleep(10)

        except Exception as e:
            print(f"[main_loop] {e}")
            await asyncio.sleep(10)


async def results_checker(bot):
    print("[CHECKER] Iniciando verificador de resultados...")
    await asyncio.sleep(60)  # aguarda 1 min antes do primeiro check
    while True:
        try:
            await check_results(bot)
        except Exception as e:
            print(f"[results_checker] {e}")
        await asyncio.sleep(120)  # verifica a cada 2 minutos

# =============================================================================
# INICIALIZAÇÃO
# =============================================================================
async def main():
    print("=" * 65)
    print("🤖 RW TIPS — BOT FIFA v5.0 (CRITÉRIOS DIRETOS)")
    print("=" * 65)
    print(f"Horário: {datetime.now(MANAUS_TZ).strftime('%Y-%m-%d %H:%M:%S')} (Manaus)")
    print()
    c8  = CRIT_8MIN
    c12 = CRIT_12MIN
    print("=== CRITÉRIOS 6/8 min (Battle, H2H, Valkyrie, Volta) ===")
    print(f"  Gate HT: liga>={c8['ht_gate_league']} | p>={c8['ht_gate_p']}")
    print(f"  Gate FT: liga>={c8['ft_gate_league']} | p>={c8['ft_gate_p']}")
    print(f"  FT +1.5: p>={c8['ft_15']['p1_marc']} pct2>={c8['ft_15'].get('pct_ft2',0):.0%}")
    print(f"  FT +2.5: p>={c8['ft_25']['p1_marc']} pct2>={c8['ft_25'].get('pct_ft2',0):.0%}")
    print(f"  FT +3.5: p>={c8['ft_35']['p1_marc']} pct3>={c8['ft_35'].get('pct_ft3',0):.0%}")
    print(f"  FT +4.5: p>={c8['ft_45']['p1_marc']} pct3>={c8['ft_45'].get('pct_ft3',0):.0%}")
    print("=== CRITÉRIOS 10/12 min (GT, Valhalla, Battle 12, CLA, Adriatic) ===")
    print(f"  Gate HT: liga>={c12['ht_gate_league']} | p>={c12['ht_gate_p']}")
    print(f"  Gate FT: liga>={c12['ft_gate_league']} | p>={c12['ft_gate_p']}")
    print(f"  FT +2.5: p>={c12['ft_25']['p1_marc']} pct2>={c12['ft_25'].get('pct_ft2',0):.0%}")
    print(f"  FT +3.5: p>={c12['ft_35']['p1_marc']} pct3>={c12['ft_35'].get('pct_ft3',0):.0%}")
    print(f"  FT +4.5: p>={c12['ft_45']['p1_marc']} pct3>={c12['ft_45'].get('pct_ft3',0):.0%}")
    print("=" * 65)

    bot = Bot(
        token=BOT_TOKEN,
        request=HTTPXRequest(
            connection_pool_size=8,
            connect_timeout=30.0,
            read_timeout=30.0,
            write_timeout=30.0,
            pool_timeout=30.0,
        )
    )

    # Conectar
    for attempt in range(5):
        try:
            me = await bot.get_me()
            print(f"[✓] Bot conectado: @{me.username}")
            break
        except Exception as e:
            print(f"[CONN] tentativa {attempt+1}: {e}")
            if attempt < 4:
                await asyncio.sleep((attempt + 1) * 5)
            else:
                print("[CONN] Não foi possível conectar")
                return

    # Carregar estado
    load_state()

    # Pre-carregar histórico
    print("[INFO] Pré-carregando histórico...")
    hist = fetch_history(pages=15)

    # Registrar todas as ligas conhecidas antecipadamente
    for lg in LEAGUE_PROFILES:
        if lg != "DEFAULT":
            league_manager.register(lg)

    # Enviar status inicial
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=f"🤖 <b>BOT v5.0 ONLINE</b>\n{league_manager.status()}",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"[INFO] Não enviou status inicial: {e}")

    # Iniciar loops
    await asyncio.gather(
        main_loop(bot),
        results_checker(bot),
        history_refresher(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Bot encerrado pelo usuário")
    except Exception as e:
        print(f"[FATAL] {e}")