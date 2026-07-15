"""
puxar_dados.py — Reconstrói tips_results.json, league_performance.json
e exibe bot_state (daily_stats) lendo direto do Firestore.

Uso:
    python puxar_dados.py
    python puxar_dados.py --dias 7        # só últimos 7 dias
    python puxar_dados.py --so-estado     # só mostra resumo, não salva
"""

import os, sys, json, argparse, io
from datetime import datetime, timezone, timedelta
from collections import deque

# Fix encoding no Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ── Inicializa Firebase ────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "serviceAccountKey.json")

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    print("[ERRO] firebase-admin não instalado. Rode: pip install firebase-admin")
    sys.exit(1)

def init_firebase():
    if not firebase_admin._apps:
        if not os.path.exists(FIREBASE_KEY_PATH):
            print(f"[ERRO] serviceAccountKey.json não encontrado em {FIREBASE_KEY_PATH}")
            sys.exit(1)
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ── Configurações do bot ───────────────────────────────────────────────────────
BOT_USER_EMAIL = "reldery1422@gmail.com"
MANAUS_TZ      = timezone(timedelta(hours=-4))

# Mesmos parâmetros do LeagueManager do bot
LEAGUE_WINDOW   = 30
LEAGUE_MIN_TIPS = 20
LEAGUE_RELOCK   = 45
LEAGUE_UNLOCK   = 60


# ── Funções de reconstrução ────────────────────────────────────────────────────

def reconstruir_tips_results(db, dias=None):
    """
    Lê a coleção 'apostas' do Firestore e reconstrói o tips_results.json.
    Só inclui apostas com resultado diferente de 'aguardando'.
    """
    print("\n[1/3] Lendo apostas do Firestore...")

    query = db.collection("apostas").where("userEmail", "==", BOT_USER_EMAIL)
    docs  = query.stream()

    data = {}
    total = 0
    for doc in docs:
        d = doc.to_dict()
        resultado = d.get("resultado", "aguardando")
        if resultado == "aguardando":
            continue

        # Data no fuso de Manaus
        ts = d.get("timestamp")
        if ts is None:
            continue
        if hasattr(ts, "tzinfo"):
            dt = ts.astimezone(MANAUS_TZ)
        else:
            dt = datetime.fromtimestamp(ts.timestamp(), tz=MANAUS_TZ)

        # Filtro de dias
        if dias:
            cutoff = datetime.now(MANAUS_TZ) - timedelta(days=dias)
            if dt < cutoff:
                continue

        dk = dt.strftime("%Y-%m-%d")
        data.setdefault(dk, []).append({
            "strategy": d.get("mercado", ""),
            "status":   resultado,
            "league":   d.get("liga", ""),
            "home":     d.get("jogador1", ""),
            "away":     d.get("jogador2", ""),
            "odds":     d.get("odds"),
            "units":    d.get("units"),
            "lucro":    d.get("lucro"),
        })
        total += 1

    print(f"    {total} apostas liquidadas encontradas em {len(data)} dia(s).")

    return dict(sorted(data.items()))


def reconstruir_league_performance(tips_results):
    """
    Reconstrói o league_performance.json a partir do tips_results.
    Simula a janela deslizante do LeagueManager.
    """
    print("\n[2/3] Reconstruindo league_performance...")

    leagues = {}
    # Processa em ordem cronológica
    for dk in sorted(tips_results.keys()):
        for tip in tips_results[dk]:
            league = tip.get("league", "Unknown")
            if not league:
                continue
            if league not in leagues:
                leagues[league] = {
                    "active": True,
                    "window": deque(maxlen=LEAGUE_WINDOW),
                    "total":  0,
                }
            green = tip.get("status") in ("green", "meio-green")
            leagues[league]["window"].append(1 if green else 0)
            leagues[league]["total"] += 1

            # Aplica lógica de lock/unlock
            n = len(leagues[league]["window"])
            if n >= LEAGUE_MIN_TIPS:
                pct = sum(leagues[league]["window"]) / n * 100
                if leagues[league]["active"] and pct < LEAGUE_RELOCK:
                    leagues[league]["active"] = False
                elif not leagues[league]["active"] and pct >= LEAGUE_UNLOCK:
                    leagues[league]["active"] = True

    # Serializa (deque → list)
    result = {}
    for lg, d in leagues.items():
        result[lg] = {
            "active": d["active"],
            "window": list(d["window"]),
            "total":  d["total"],
        }
    print(f"    {len(result)} liga(s) processada(s).")
    return result


def calcular_daily_stats(tips_results):
    """Calcula o daily_stats a partir do tips_results."""
    daily = {}
    for dk, tips in tips_results.items():
        g  = sum(1 for t in tips if t.get("status") == "green")
        mg = sum(1 for t in tips if t.get("status") == "meio-green")
        r  = sum(1 for t in tips if t.get("status") == "red")
        mr = sum(1 for t in tips if t.get("status") == "meio-red")
        daily[dk] = {"green": g + mg, "red": r + mr}
    return daily


def salvar_json(nome, dados, backup=True):
    if backup and os.path.exists(nome):
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = f"{nome}.backup_{ts}"
        os.rename(nome, bak)
        print(f"    Backup: {bak}")
    with open(nome, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=2, ensure_ascii=False)
    print(f"    Salvo: {nome} ({os.path.getsize(nome)} bytes)")


def exibir_resumo(tips_results, daily_stats, league_perf):
    print("\n" + "=" * 55)
    print("  RESUMO DOS DADOS PUXADOS")
    print("=" * 55)

    print("\n[DAILY STATS] ultimos 7 dias:")
    dias_ord = sorted(daily_stats.keys())[-7:]
    for dk in dias_ord:
        vals  = daily_stats[dk]
        g     = vals.get("green", 0)
        r     = vals.get("red", 0)
        total = g + r
        pct   = (g / total * 100) if total else 0
        barra = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"   {dk}: {g:2d}G / {r:2d}R  {pct:4.0f}%  {barra}")

    print(f"\n[LIGAS] {len(league_perf)} no total:")
    for lg, d in sorted(league_perf.items()):
        n   = len(d["window"])
        st  = "[ON] " if d["active"] else "[OFF]"
        if n >= LEAGUE_MIN_TIPS:
            pct = sum(d["window"]) / n * 100
            info = f"{pct:.0f}% | {n} recentes | {d['total']} total"
        else:
            info = f"coletando ({n}/{LEAGUE_MIN_TIPS}) | {d['total']} total"
        print(f"   {st} {lg}: {info}")

    # Totais gerais
    all_tips = [t for tips in tips_results.values() for t in tips]
    g_total  = sum(1 for t in all_tips if t.get("status") in ("green", "meio-green"))
    r_total  = sum(1 for t in all_tips if t.get("status") in ("red", "meio-red"))
    total    = g_total + r_total
    pct_geral = (g_total / total * 100) if total else 0
    print(f"\n[TOTAL] {g_total}G / {r_total}R de {total} tips ({pct_geral:.1f}%)")
    print("=" * 55)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Puxa dados atualizados do Firestore.")
    parser.add_argument("--dias",       type=int, default=None,
                        help="Filtra por N últimos dias (padrão: todos)")
    parser.add_argument("--so-estado",  action="store_true",
                        help="Só exibe resumo, não salva arquivos")
    args = parser.parse_args()

    print("[*] Conectando ao Firestore...")
    db = init_firebase()
    print("    Conectado!\n")

    # 1. tips_results
    tips_results = reconstruir_tips_results(db, dias=args.dias)

    # 2. league_performance
    league_perf = reconstruir_league_performance(tips_results)

    # 3. daily_stats
    daily_stats = calcular_daily_stats(tips_results)

    print("\n[3/3] Salvando arquivos...")
    if not args.so_estado:
        salvar_json("tips_results.json",      tips_results)
        salvar_json("league_performance.json", league_perf)

        # Atualiza o daily_stats dentro do bot_state.json existente
        state_file = "bot_state.json"
        state = {}
        if os.path.exists(state_file):
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)
        state["daily_stats"] = daily_stats
        salvar_json(state_file, state, backup=True)
    else:
        print("   (--so-estado ativo: nenhum arquivo foi salvo)")

    exibir_resumo(tips_results, daily_stats, league_perf)


if __name__ == "__main__":
    main()
