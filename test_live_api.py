from bot import fetch_live_matches, extract_player_name

def test_extraction():
    print("Testing player name extraction...")
    assert extract_player_name("River Plate (Sheva)") == "Sheva"
    assert extract_player_name("Real Madrid (Groma)") == "Groma"
    assert extract_player_name("Sheva") == "Sheva"
    assert extract_player_name("") == ""
    print("✓ Extraction logic passed")

def test_fetch():
    print("\nTesting fetch_live_matches...")
    events = fetch_live_matches()
    print(f"Events found: {len(events)}")
    
    if events:
        first = events[0]
        print("First event sample:")
        print(first)
        
        # Validation
        assert 'leagueName' in first
        assert 'homePlayer' in first
        assert 'score' in first
        assert 'timer' in first
        assert 'bet365EventId' in first
        
        print("\nStructure Check:")
        print(f"League: {first['leagueName']}")
        print(f"Home: {first['homePlayer']}")
        print(f"Away: {first['awayPlayer']}")
        print(f"Score: {first['score']}")
        print(f"Time: {first['timer']}")
        
        # Check if player names are clean (no parentheses)
        if '(' in first['homePlayer'] or ')' in first['homePlayer']:
             print("WARNING: Home player name might not be clean")
        else:
             print("✓ Home player name clean")

if __name__ == "__main__":
    test_extraction()
    test_fetch()
