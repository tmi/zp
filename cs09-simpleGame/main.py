import curses
import os
from enum import Enum, auto
from typing import List, Optional

from src.simple_game.engine import Engine
from src.simple_game.game import Level, GameSession
from src.simple_game.ui import UI

class State(Enum):
    INTRO = auto()
    LEVEL_SELECT = auto()
    GAMEPLAY = auto()
    VICTORY = auto()
    QUIT = auto()

class App:
    def __init__(self) -> None:
        self.engine = Engine()
        self.ui: Optional[UI] = None
        self.state = State.INTRO
        self.levels: List[str] = ["level1.txt", "level2.txt", "level3.txt"]
        self.current_level_idx = 0
        self.session: Optional[GameSession] = None

    def run(self, stdscr: curses.window) -> None:
        self.engine.start(stdscr)
        self.ui = UI(self.engine)

        while self.state != State.QUIT:
            if self.state == State.INTRO:
                self.handle_intro()
            elif self.state == State.LEVEL_SELECT:
                self.handle_level_select()
            elif self.state == State.GAMEPLAY:
                self.handle_gameplay()
            elif self.state == State.VICTORY:
                self.handle_victory()

    def handle_intro(self) -> None:
        if self.ui:
            self.ui.draw_intro()
        key = self.engine.get_input()
        if key in [ord('n'), ord('N')]:
            self.state = State.LEVEL_SELECT
        elif key in [ord('q'), ord('Q')]:
            self.state = State.QUIT

    def handle_level_select(self) -> None:
        if self.ui:
            self.ui.draw_level_select(self.levels)
        key = self.engine.get_input()
        if ord('1') <= key <= ord(str(len(self.levels))):
            self.current_level_idx = key - ord('1')
            self.start_level()
        elif key in [ord('q'), ord('Q')]:
            self.state = State.QUIT

    def start_level(self) -> None:
        level_path = os.path.join(os.path.dirname(__file__), "levels", self.levels[self.current_level_idx])
        level = Level(level_path)
        self.session = GameSession(level)
        self.state = State.GAMEPLAY

    def handle_gameplay(self) -> None:
        if not self.session or not self.ui:
            return

        self.ui.draw_game(self.session)
        key = self.engine.get_input()

        if key == curses.KEY_UP:
            self.session.move_player(-1, 0)
        elif key == curses.KEY_DOWN:
            self.session.move_player(1, 0)
        elif key == curses.KEY_LEFT:
            self.session.move_player(0, -1)
        elif key == curses.KEY_RIGHT:
            self.session.move_player(0, 1)
        elif key in [ord('r'), ord('R')]:
            self.start_level()
        elif key in [ord('q'), ord('Q')]:
            self.state = State.QUIT
        elif key in [ord('n'), ord('N')]:
            self.state = State.INTRO

        if self.session.reset_needed:
            self.start_level()
        elif self.session.is_finished:
            self.state = State.VICTORY

    def handle_victory(self) -> None:
        if self.ui:
            self.ui.draw_victory()
        key = self.engine.get_input()
        if key in [ord('n'), ord('N')]:
            if self.current_level_idx + 1 < len(self.levels):
                self.current_level_idx += 1
                self.start_level()
            else:
                self.state = State.INTRO
        elif key in [ord('r'), ord('R')]:
            self.start_level()
        elif key in [ord('q'), ord('Q')]:
            self.state = State.QUIT

if __name__ == "__main__":
    app = App()
    curses.wrapper(app.run)
