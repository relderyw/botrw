from bot import fetch_player_individual_stats, fetch_h2h_data, fetch_recent_matches, global_matches_cache
import time

def verify_system():
    print("=== START VERIFICATION ===")
    
    # 1. First call - should trigger Global Fetch
    start = time.time()
    print("\n[TEST 1] Fetching Player Stats (should trigger global download)...")
    p1_stats = fetch_player_individual_stats('Yerema', use_cache=False)
    print(f"Time taken: {time.time() - start:.2f}s")
    
    if p1_stats and p1_stats['matches']:
        print(f"Fetched {p1_stats['total_count']} matches for Yerema.")
        print(f"Sample match: {p1_stats['matches'][0]['home_player']} vs {p1_stats['matches'][0]['away_player']}")
        # Verify filtering
        m = p1_stats['matches'][0]
        if 'YEREMA' not in [m['home_player'].upper(), m['away_player'].upper()]:
            print("❌ FILTERING FAILED: Match does not involve Yerema!")
        else:
            print("✅ Filtering Verified.")
    else:
        print("❌ Failed to fetch player stats.")

    # 2. Second call - should be INSTANT (from cache)
    start = time.time()
    print("\n[TEST 2] Fetching DIFFERENT Player Stats (should remain fast)...")
    p2_stats = fetch_player_individual_stats('Andrew', use_cache=False) # use_cache=False for function level, but Global is internal
    print(f"Time taken: {time.time() - start:.2f}s")
    if time.time() - start > 5:
         print("❌ CACHE NOT WORKING (Took > 5s)")
    else:
         print("✅ Cache Verified (Fast).")

    # 3. H2H Test
    print("\n[TEST 3] Fetching H2H (Yerema vs Andrew)...")
    h2h = fetch_h2h_data('Yerema', 'Andrew')
    if h2h:
        print(f"H2H Matches: {h2h['total_matches']}")
        print(f"Avg Goals: {h2h.get('avg_total_goals', 0):.2f}")
    else:
        print("❌ H2H Failed.")
        
if __name__ == "__main__":
    verify_system()
