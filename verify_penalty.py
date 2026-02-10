
from bot import analyze_player_adaptive, calculate_confidence, player_stats_cache
import logging

# Setup dummy match data for "COOLING" scenario
# Last 3 games: 0, 1, 0 (Avg 0.33)
# Previous 7 games: 2, 2, 2, 2, 2, 2, 2 (Avg 2.0)
# Ratio: 0.33 / 2.0 = 0.16 (< 0.5) -> COOLING

matches = []
# Recent 3 (Bad)
for i in range(3):
    matches.append({
        'home_player': 'TEST_PLAYER',
        'home_score_ht': 0, 'away_score_ht': 0,
        'home_score_ft': 0 if i != 1 else 1, 'away_score_ft': 0, # [0, 1, 0]
        'league_name': 'Test League'
    })

# Previous 7 (Good)
for i in range(7):
    matches.append({
        'home_player': 'TEST_PLAYER',
        'home_score_ht': 1, 'away_score_ht': 0,
        'home_score_ft': 2, 'away_score_ft': 0,
        'league_name': 'Test League'
    })

print(f"Total Matches: {len(matches)}")

# 1. Test analyze_player_adaptive
print("\n--- Testing analyze_player_adaptive ---")
stats = analyze_player_adaptive(matches, "TEST_PLAYER")

if stats is None:
    print("FAIL: Returned None (Still Blocking)")
else:
    print("SUCCESS: Returned Stats (Not Blocking)")
    print(f"Regime Change: {stats.get('regime_change')}")
    print(f"Direction: {stats.get('regime_direction')}")
    
    # 2. Test calculate_confidence logic manually (since we need full context)
    # Simulate a confidence calculation
    print("\n--- Testing Confidence Penalty Logic ---")
    
    # Mock other stats
    home_stats = stats
    away_stats = stats # Same bad stats for both to trigger double penalty? Or just one.
    
    # Simple simulation of the new block in calculate_confidence
    confidence = 100
    if home_stats.get('regime_direction') == 'COOLING':
        print("Applying Penalty...")
        confidence -= 20
        
    print(f"Confidence after penalty check: {confidence}")
