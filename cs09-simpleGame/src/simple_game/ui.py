import json
import os
from typing import Dict, List, Any
from .engine import Engine

class UI:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.strings: Dict[str, str] = {}
        self.logo: List[str] = []
        self.victory: List[str] = []
        self.load_assets()

    def load_assets(self) -> None:
        base_path = os.path.join(os.path.dirname(__file__), "..", "..", "assets")

        with open(os.path.join(base_path, "strings.json"), "r") as f:
            self.strings = json.load(f)

        with open(os.path.join(base_path, "logo.txt"), "r") as f:
            self.logo = [line.rstrip('\n') for line in f.readlines()]

        with open(os.path.join(base_path, "victory.txt"), "r") as f:
            self.victory = [line.rstrip('\n') for line in f.readlines()]

    def draw_intro(self) -> None:
        self.engine.clear()
        h, w = self.engine.get_screen_size()

        # Draw logo on the left
        logo_y = (h - len(self.logo)) // 2
        logo_x = w // 4 - max(len(line) for line in self.logo) // 2
        self.engine.draw_ascii_art(logo_y, logo_x, self.logo)

        # Draw menu on the right
        menu_y = h // 2 - 1
        menu_x = 3 * w // 4 - 10
        self.engine.draw_text(menu_y, menu_x, self.strings["intro_new_game"], "menu", True)
        self.engine.draw_text(menu_y + 2, menu_x, self.strings["intro_quit"], "menu", True)

        self.engine.refresh()

    def draw_level_select(self, levels: List[str]) -> None:
        self.engine.clear()
        h, w = self.engine.get_screen_size()

        # Draw logo on the left
        logo_y = (h - len(self.logo)) // 2
        logo_x = w // 4 - max(len(line) for line in self.logo) // 2
        self.engine.draw_ascii_art(logo_y, logo_x, self.logo)

        # Draw level list on the right
        menu_y = h // 2 - len(levels)
        menu_x = 3 * w // 4 - 10
        self.engine.draw_text(menu_y - 2, menu_x, self.strings["level_select_title"], "menu", True)

        for i, level_name in enumerate(levels):
            self.engine.draw_text(menu_y + i * 2, menu_x, f"{i + 1} - {level_name}", "menu")

        self.engine.refresh()

    def draw_game(self, session: Any) -> None:
        self.engine.clear()
        h, w = self.engine.get_screen_size()

        # Center the map
        offset_y = (h - session.level.height) // 2
        offset_x = (w - session.level.width) // 2

        # Draw walls
        for wall in session.level.walls:
            self.engine.draw_text(offset_y + wall.y, offset_x + wall.x, "X", "wall")

        # Draw traps
        for trap in session.level.traps:
            self.engine.draw_text(offset_y + trap.y, offset_x + trap.x, "D", "trap")

        # Draw treasures (only those not collected)
        for treasure in session.level.treasures:
            if treasure not in session.treasures_collected:
                self.engine.draw_text(offset_y + treasure.y, offset_x + treasure.x, "T", "treasure")

        # Draw player
        self.engine.draw_text(offset_y + session.player_pos.y, offset_x + session.player_pos.x, "P", "player")

        # Draw controls at the bottom
        self.engine.draw_text(h - 1, (w - len(self.strings["game_controls"])) // 2, self.strings["game_controls"], "default")

        self.engine.refresh()

    def draw_victory(self) -> None:
        self.engine.clear()
        h, w = self.engine.get_screen_size()

        # Draw victory art on the left
        logo_y = (h - len(self.victory)) // 2
        logo_x = w // 4 - max(len(line) for line in self.victory) // 2
        self.engine.draw_ascii_art(logo_y, logo_x, self.victory, "logo")

        # Draw menu on the right
        menu_y = h // 2 - 2
        menu_x = 3 * w // 4 - 10
        self.engine.draw_text(menu_y - 2, menu_x, self.strings["victory_congrats"], "menu", True)
        self.engine.draw_text(menu_y, menu_x, self.strings["victory_next"], "menu")
        self.engine.draw_text(menu_y + 2, menu_x, self.strings["victory_restart"], "menu")
        self.engine.draw_text(menu_y + 4, menu_x, self.strings["victory_quit"], "menu")

        self.engine.refresh()
