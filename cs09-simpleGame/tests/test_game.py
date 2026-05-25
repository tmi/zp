from src.simple_game.game import Level, GameSession, Point

def test_level_loading(tmp_path):
    level_file = tmp_path / "test_level.txt"
    level_file.write_text("# Treasure: 1\n# Traps: 1\nXXXXX\nX P X\nX T X\nX D X\nXXXXX")

    level = Level(str(level_file))
    assert level.width == 5
    assert level.height == 5
    assert level.player_start == Point(1, 2)
    assert Point(2, 2) in level.treasures
    assert Point(3, 2) in level.traps
    assert Point(0, 0) in level.walls

def test_random_placement(tmp_path):
    level_file = tmp_path / "test_random.txt"
    level_file.write_text("# Treasure: 5\n# Traps: 5\nXXXXXXXXXXXXXXX\nX P           X\nXXXXXXXXXXXXXXX")

    level = Level(str(level_file))
    assert len(level.treasures) == 5
    assert len(level.traps) == 5

def test_player_movement(tmp_path):
    level_file = tmp_path / "test_move.txt"
    level_file.write_text("XXXXX\nX P X\nXXXXX")
    level = Level(str(level_file))
    session = GameSession(level)

    session.move_player(0, 1)
    assert session.player_pos == Point(1, 3)

    # Hit wall
    session.move_player(0, 1)
    assert session.player_pos == Point(1, 3)

    session.move_player(0, -1)
    assert session.player_pos == Point(1, 2)

def test_treasure_collection(tmp_path):
    level_file = tmp_path / "test_treasure.txt"
    level_file.write_text("XXXXX\nXP T X\nXXXXX")
    level = Level(str(level_file))
    session = GameSession(level)

    session.move_player(0, 2) # Move to T
    assert len(session.treasures_collected) == 1
    assert session.is_finished

def test_trap_collision(tmp_path):
    level_file = tmp_path / "test_trap.txt"
    level_file.write_text("XXXXX\nXP D X\nXXXXX")
    level = Level(str(level_file))
    session = GameSession(level)

    session.move_player(0, 2) # Move to D
    assert session.reset_needed
