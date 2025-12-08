import os
import time
import requests
import asyncio
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from telegram import Bot
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================== CONFIGURAÇÃO ===========================
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

BOT_TOKEN = "6569266928:AAHm7pOJVsd3WKzJEgdVDez4ZYdCAlRoYO8"
CHAT_ID = "-1001981134607"

LIVE_API_URL = "https://app3.caveiratips.com.br/api/live-events/"
CONFRONTO_API = "https://app3.caveiratips.com.br/app3/api/confronto/"
ENDED_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"

# Token obrigatório da API do app3
AUTH_TOKEN_APP3 = "Bearer aeb42dcc7f15a3b06d478ef0d4465ed8a4e21c6d52b526285222a49d628ead94"
AUTH_TOKEN_GREEN = "Bearer oat_MTEyNTEx.aS1EdDJaNWw2dUkzREpqOGI3Mmo1eHdVeUZOZmZyQmZkclR2bE1RODM0ODg3NzEzODQ"

MANAUS_TZ = timezone(timedelta(hours=-4))

# =========================== CACHE E SESSÃO ===========================
player_cache = {}
h2h_cache = {}
CACHE_TTL = 300

session = requests.Session()
retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))

sent_matches = set()
sent_tips = []
last_league_message_id = None
last_league_summary = None

# =========================== BUSCA PARTIDAS AO VIVO ===========================
def fetch_live_matches():
    try:
        r = session.get(LIVE_API_URL, timeout=12)
        r.raise_for_status()
        events = r.json().get("events", [])
        live = [e for e in events if e.get("status") == "live"]
        matches = []
        for e in live:
            matches.append({
                'id': e['id'],
                'league_name': e['leagueName'],
                'homePlayer': e['homePlayer'],
                'awayPlayer': e['awayPlayer'],
                'home_name': e['homeTeam']['name'],
                'away_name': e['awayTeam']['name'],
                'timer': {'tm': e['timer']['minute'], 'ts': e['timer']['second']},
                'ss': e['scoreboard'],
                'stats': e.get('stats', {}),
                'bet365_id': e.get('bet365EventId')
            })
        logger.info(f"{len(matches)} partidas ao vivo")
        return matches
    except Exception as e:
        logger.error(f"Erro live: {e}")
        return []

# =========================== API CONFRONTO (RÁPIDA) ===========================
def get_player_and_h2h_data(p1, p2=None):
    headers = {"Authorization": AUTH_TOKEN_APP3}
    url = f"{CONFRONTO_API}?player1={p1}&interval=9999" + (f"&player2={p2}" if p2 else "")
    try:
        r = session.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("player1") != p1:
            return None, None

        matches = data.get("matches", [])[:10]
        if len(matches) < 5:
            return None, None

        # Stats do jogador 1
        ht_goals = []
        ft_goals = []
        ht_scored = []
        ht_conceded = []
        player_goals = []

        for m in matches:
            is_home = m['home_player'].lower() == p1.lower()
            ht_h = int(m.get('home_score_ht', 0))
            ht_a = int(m.get('away_score_ht', 0))
            ft_h = int(m.get('home_score_ft', 0))
            ft_a = int(m.get('away_score_ft', 0))

            total_ht = ht_h + ht_a
            total_ft = ft_h + ft_a

            ht_goals.append(total_ht)
            ft_goals.append(total_ft)

            if is_home:
                ht_scored.append(ht_h)
                ht_conceded.append(ht_a)
                player_goals.append(ft_h)
            else:
                ht_scored.append(ht_a)
                ht_conceded.append(ht_h)
                player_goals.append(ft_a)

        n = len(ht_goals)
        stats = {
            'over_0.5_ht': sum(g > 0 for g in ht_goals) / n * 100,
            'over_1.5_ht': sum(g > 1 for g in ht_goals) / n * 100,
            'over_2.5_ht': sum(g > 2 for g in ht_goals) / n * 100,
            'marcou_1.5_ht': sum(s >= 2 for s in ht_scored) / n * 100,
            'sofreu_1.5_ht': sum(c >= 2 for c in ht_conceded) / n * 100,
            'avg_goals': sum(player_goals) / n,
            'avg_ht_goals': sum(ht_goals) / n
        }

        # H2H se p2 foi passado
        h2h = None
        if p2 and data.get("player2") == p2:
            total = data.get('total_count', 1)
            h2h = {
                'avg_ht_goals': data.get('ht_goals_sum', 0) / total,
                'avg_ft_goals': data.get('ft_goals_sum', 0) / total,
                'player1_avg_goals': data.get('avg_goals_p1', 0),
                'player2_avg_goals': data.get('avg_goals_p2', 0),
                'player1_win_percentage': data.get('wins_p1_pct', 0),
                'player2_win_percentage': data.get('wins_p2_pct', 0),
                'over_0.5_ht_percentage': data.get('ht_over_0.5_pct', 0),
                'over_1.5_ht_percentage': data.get('ht_over_1.5_pct', 0),
                'over_2.5_ht_percentage': data.get('ht_over_2.5_pct', 0),
                'over_2.5_ft_percentage': data.get('over_2.5_pct', 0),
                'over_3.5_ft_percentage': data.get('over_3.5_pct', 0),
                'over_4.5_ft_percentage': data.get('over_4.5_pct', 0),
                'btts_ht_percentage': data.get('btts_summary', {}).get('ht', {}).get('pct', 0),
                'btts_ft_percentage': data.get('btts_summary', {}).get('ft', {}).get('pct', 0),
            }

        return stats, h2h
    except Exception as e:
        logger.error(f"Erro API confronto {p1}/{p2}: {e}")
        return None, None

# =========================== CACHE ===========================
def get_stats(p1, p2=None):
    key = (p1.lower(), p2.lower() if p2 else None)
    now = time.time()
    if key in player_cache and now - player_cache[key][1] < CACHE_TTL:
        return player_cache[key][0]
    stats, h2h = get_player_and_h2h_data(p1, p2)
    if stats or h2h:
        player_cache[key] = ((stats, h2h), now)
    return stats, h2h

# =========================== UTILIDADES ===========================
def tempo_min(match):
    t = match['timer']
    return t['tm'] + t['ts'] / 60

def placar(match):
    try:
        h, a = map(int, match['ss'].split('-'))
        return h, a
    except:
        return 0, 0

def da_rate(match, t):
    if t <= 0: return 0.0
    da = match.get('stats', {}).get('dangerous_attacks', [0, 0])
    return (int(da[0]) + int(da[1])) / t

def format_thermometer(perc):
    bars = 10
    green = round(perc / 10)
    return 'Green' * green + 'Red' * (bars - green) + f" {perc:.0f}%"

def msg(match, h2h, estrategia):
    liga = match['league_name']
    p1 = match['homePlayer']
    p2 = match['awayPlayer']
    t = f"{match['timer']['tm']}:{match['ts']:02d}"
    ss = match['ss']
    bet_id = match.get('bet365_id')

    mensagem = f"\n\n<b>Trophy {liga}</b>\n\n<b>Target {estrategia}</b>\n\nTime: {t}\n\n"
    mensagem += f"Player {p1} vs {p2}\n"
    mensagem += f"Goal {ss}\n"

    if h2h:
        mensagem += f"\nWinner <i>{h2h.get('player1_win_percentage',0):.0f}% vs {h2h.get('player2_win_percentage',0):.0f}%</i>\n\n"
        mensagem += f"Diamond Média gols: <i>{h2h.get('player1_avg_goals',0):.2f}</i> vs <i>{h2h.get('player2_avg_goals',0):.2f}</i>\n\n"
        mensagem += "<b>Chart H2H HT (últimos 10 jogos):</b>\n\n"
        mensagem += f"Goal +0.5: <i>{h2h.get('over_0.5_ht_percentage',0):.0f}%</i> | +1.5: <i>{h2h.get('over_1.5_ht_percentage',0):.0f}%</i> | +2.5: <i>{h2h.get('over_2.5_ht_percentage',0):.0f}%</i>\n\n"
        mensagem += f"Goal BTTS HT: <i>{h2h.get('btts_ht_percentage',0):.0f}%</i>\n"

    if bet_id:
        mensagem += f"\n\nGlobe <a href='https://www.bet365.bet.br/#/IP/EV{bet_id}'>Link Bet365</a>\n\n"

    return mensagem

async def enviar(bot, mid, texto, strat):
    if mid in sent_matches: return
    try:
        msg = await bot.send_message(chat_id=CHAT_ID, text=texto, parse_mode="HTML", disable_web_page_preview=True)
        sent_matches.add(mid)
        sent_tips.append({'match_id': mid, 'strategy': strat, 'sent_time': datetime.now(MANAUS_TZ), 'status': 'pending', 'message_id': msg.message_id, 'message_text': texto})
        logger.info(f"TIP ENVIADA → {strat}")
    except Exception as e:
        logger.error(f"Erro envio: {e}")

# =========================== GREEN / RED ===========================
async def checker_green_red(bot):
    while True:
        await asyncio.sleep(120)
        try:
            r = session.get(ENDED_API_URL, headers={"Authorization": AUTH_TOKEN_GREEN}, timeout=15)
            ended = r.json().get("items", [])
            ended_dict = {}
            for m in ended:
                eid = m.get('eventID') or m.get('betsApiEventId')
                if eid: ended_dict[str(eid)] = m

            for tip in sent_tips[:]:
                if tip['status'] != 'pending': continue
                m = ended_dict.get(tip['match_id'])
                if not m: continue

                ht_h = m.get('scoreHT', {}).get('home', 0)
                ht_a = m.get('scoreHT', {}).get('away', 0)
                ft_h = m.get('score', {}).get('home', 0)
                ft_a = m.get('score', {}).get('away', 0)

                strat = tip['strategy']
                win = False

                if 'GOLS HT' in strat:
                    line = float(re.search(r'\+([\d.]+)', strat).group(1))
                    win = (ht_h + ht_a) > line
                elif 'GOLS FT' in strat:
                    line = float(re.search(r'\+([\d.]+)', strat).group(1))
                    win = (ft_h + ft_a) > line
                elif 'GOLS' in strat and 'JOGADOR' not in strat:
                    line = float(re.search(r'\+([\d.]+)', strat).group(1))
                    player = tip['message_text'].split('vs')[0].strip().split()[-1]
                    win = (ft_h if player == tip['message_text'].split()[3] else ft_a) > line
                elif 'BTTS' in strat:
                    win = (ht_h > 0 and ht_a > 0) if 'HT' in strat else (ft_h > 0 and ft_a > 0)

                status = 'green' if win else 'red'
                emoji = "GreenGreenGreenGreenGreen" if status == 'green' else "RedRedRedRedRed"
                await bot.edit_message_text(chat_id=CHAT_ID, message_id=tip['message_id'], text=tip['message_text'] + f"\n\n{emoji}")
                tip['status'] = status
        except Exception as e:
            logger.error(f"Erro checker: {e}")

# =========================== RESUMO DE LIGAS (EXATAMENTE COMO ERA) ===========================
async def league_summary(bot):
    global last_league_message_id, last_league_summary
    while True:
        await asyncio.sleep(300)
        try:
            r = session.get(ENDED_API_URL, headers={"Authorization": AUTH_TOKEN_GREEN}, timeout=15)
            ended = r.json().get("items", [])
            league_games = defaultdict(list)

            for m in ended:
                league = m.get('leagueName', 'Unknown')
                if league == 'Unknown': continue
                ft_home = m.get('score', {}).get('home', 0)
                ft_away = m.get('score', {}).get('away', 0)
                ft_goals = ft_home + ft_away
                league_games[league].append(ft_goals)

            stats = {}
            for league, gols in league_games.items():
                last5 = gols[-5:]
                if len(last5) < 5: continue
                stats[league] = {
                    '0.5': sum(g > 0.5 for g in last5) / 5 * 100,
                    '1.5': sum(g > 1.5 for g in last5) / 5 * 100,
                    '2.5': sum(g > 2.5 for g in last5) / 5 * 100,
                    '3.5': sum(g > 3.5 for g in last5) / 5 * 100,
                    '4.5': sum(g > 4.5 for g in last5) / 5 * 100,
                }

            if not stats:
                await asyncio.sleep(60)
                continue

            league_over_avg = {l: sum(s.values()) / 5 for l, s in stats.items()}
            most_over = max(league_over_avg, key=league_over_avg.get)
            most_under = min(league_over_avg, key=league_over_avg.get)

            msg = "<b>Resumo das Ligas (últimos 5 jogos)</b>\n\n"
            for league, s in stats.items():
                msg += f"<b>{league}</b>\n"
                for line in ['0.5', '1.5', '2.5', '3.5', '4.5']:
                    p = s[line]
                    msg += f"OVER {line}\n{format_thermometer(p)}\n"
                msg += "\n"

            msg += f"<b>Trophy Liga mais OVER: {most_over} ({league_over_avg[most_over]:.1f}%)</b>\n"
            msg += f"<b>Prohibited Evitar: {most_under} ({league_over_avg[most_under]:.1f}%)</b>\n"

            if msg != last_league_summary:
                if last_league_message_id:
                    try: await bot.delete_message(CHAT_ID, last_league_message_id)
                    except: pass
                new = await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                last_league_summary = msg
                last_league_message_id = new.message_id
        except Exception as e:
            logger.error(f"Erro league summary: {e}")

# =========================== LOOP PRINCIPAL COM TODAS AS ESTRATÉGIAS ===========================
async def main():
    bot = Bot(token=BOT_TOKEN)
    processed = set()

    asyncio.create_task(checker_green_red(bot))
    asyncio.create_task(league_summary(bot))

    logger.info("BOT INICIADO - TODAS ESTRATÉGIAS + RESUMO DE LIGAS + GREEN/RED")

    while True:
        try:
            for match in fetch_live_matches():
                if match['id'] in processed: continue
                processed.add(match['id'])

                liga = match['league_name']
                p1 = match['homePlayer']
                p2 = match['awayPlayer']
                t = tempo_min(match)
                g1, g2 = placar(match)
                total = g1 + g2
                da = da_rate(match, t)

                stats1, _ = get_stats(p1)
                stats2, _ = get_stats(p2)
                _, h2h = get_stats(p1, p2)
                if not stats1 or not stats2 or not h2h: continue

                # ========= TODAS AS SUAS ESTRATÉGIAS AQUI =========
                if "8 mins play" in liga:
                    if t < 4:
                        if g1 == 0 and g2 == 0 and t > 1 and t <= 3:
                            if h2h['avg_ht_goals'] >= 3.0 and h2h['btts_ht_percentage'] >= 100 and h2h['over_2.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+2.5 GOLS HT"), "+2.5 HT")
                            if h2h['avg_ht_goals'] >= 2.5 and h2h['btts_ht_percentage'] >= 80 and h2h['over_1.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+1.5 GOLS HT"), "+1.5 HT")
                            if h2h['avg_ht_goals'] >= 2.0 and h2h['btts_ht_percentage'] >= 80 and h2h['over_0.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+0.5 GOL HT"), "+0.5 HT")

                        if total == 1 and t >= 2 and t <= 3:
                            if h2h['avg_ht_goals'] >= 3.0 and h2h['btts_ht_percentage'] >= 100 and h2h['over_2.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+1.5 GOLS HT"), "+1.5 HT")

                    if t >= 2 and t < 6:
                        if stats1['avg_goals'] >= 3.0 and h2h['over_2.5_ft_percentage'] == 100 and g1 == 0 and h2h['player1_win_percentage'] >= 60:
                            await enviar(bot, match['id'], msg(match, h2h, f"+1.5 GOLS {p1}"), f"+1.5 {p1}")
                        if stats1['avg_goals'] >= 4.0 and h2h['over_3.5_ft_percentage'] == 100 and g1 == 0 and h2h['player1_win_percentage'] >= 60:
                            await enviar(bot, match['id'], msg(match, h2h, f"+2.5 GOLS {p1}"), f"+2.5 {p1}")

                elif "12 mins play" in liga:
                    if t < 6:
                        if g1 == 0 and g2 == 0:
                            if h2h['avg_ht_goals'] >= 3.5 and da >= 1.0 and h2h['btts_ht_percentage'] >= 100 and h2h['over_2.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+2.5 GOLS HT 12min"), "+2.5 HT 12")
                            if h2h['avg_ht_goals'] >= 2.5 and da >= 1.0 and h2h['btts_ht_percentage'] >= 90 and h2h['over_1.5_ht_percentage'] == 100:
                                await enviar(bot, match['id'], msg(match, h2h, "+1.5 GOLS HT 12min"), "+1.5 HT 12")

                elif "Volta - 6 mins" in liga and g1 == 0 and g2 == 0 and t >= 1 and t < 3:
                    if h2h['avg_ft_goals'] >= 5.5 and h2h['btts_ft_percentage'] == 100 and h2h['over_4.5_ft_percentage'] == 100:
                        await enviar(bot, match['id'], msg(match, h2h, "+4.5 GOLS FT"), "+4.5 FT")
                    if h2h['avg_ft_goals'] >= 3.5 and h2h['btts_ft_percentage'] == 100 and h2h['over_3.5_ft_percentage'] == 100:
                        await enviar(bot, match['id'], msg(match, h2h, "+3.5 GOLS FT"), "+3.5 FT")

        except Exception as e:
            logger.error(f"Erro loop: {e}")
        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())