import curses
from typing import Any, List, Optional, Tuple

class Engine:
    def __init__(self) -> None:
        self.stdscr: Optional[Any] = None
        self.colors: dict[str, int] = {}

    def start(self, stdscr: Any) -> None:
        self.stdscr = stdscr
        curses.curs_set(0)  # Hide cursor
        curses.start_color()
        curses.use_default_colors()

        # Define color pairs
        curses.init_pair(1, curses.COLOR_WHITE, -1)     # Default
        curses.init_pair(2, curses.COLOR_YELLOW, -1)    # Player / Treasure
        curses.init_pair(3, curses.COLOR_RED, -1)       # Trap
        curses.init_pair(4, curses.COLOR_BLUE, -1)      # Wall
        curses.init_pair(5, curses.COLOR_GREEN, -1)     # Success / Menu
        curses.init_pair(6, curses.COLOR_CYAN, -1)      # Logo

        self.colors = {
            "default": curses.color_pair(1),
            "player": curses.color_pair(2),
            "treasure": curses.color_pair(2) | curses.A_BOLD,
            "trap": curses.color_pair(3),
            "wall": curses.color_pair(4),
            "menu": curses.color_pair(5),
            "logo": curses.color_pair(6),
        }

    def clear(self) -> None:
        if self.stdscr:
            self.stdscr.clear()

    def refresh(self) -> None:
        if self.stdscr:
            self.stdscr.refresh()

    def get_input(self) -> int:
        if self.stdscr:
            return self.stdscr.getch()
        return -1

    def draw_text(self, y: int, x: int, text: str, color: str = "default", bold: bool = False) -> None:
        if not self.stdscr:
            return
        attr = self.colors.get(color, self.colors["default"])
        if bold:
            attr |= curses.A_BOLD
        try:
            self.stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass

    def draw_ascii_art(self, start_y: int, start_x: int, lines: List[str], color: str = "logo") -> None:
        for i, line in enumerate(lines):
            self.draw_text(start_y + i, start_x, line, color)

    def get_screen_size(self) -> Tuple[int, int]:
        if self.stdscr:
            return self.stdscr.getmaxyx()
        return (0, 0)
