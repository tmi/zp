import random
from dataclasses import dataclass
from typing import List, Set

@dataclass(frozen=True)
class Point:
    y: int
    x: int

class Level:
    def __init__(self, filepath: str) -> None:
        self.walls: Set[Point] = set()
        self.treasures: Set[Point] = set()
        self.traps: Set[Point] = set()
        self.player_start: Point = Point(0, 0)
        self.width: int = 0
        self.height: int = 0
        self.target_treasures: int = 0
        self.target_traps: int = 0
        self.load(filepath)

    def load(self, filepath: str) -> None:
        with open(filepath, 'r') as f:
            lines = f.readlines()

        grid_lines: List[str] = []
        for line in lines:
            line = line.rstrip('\n')
            if line.startswith('#'):
                if 'Treasure:' in line:
                    self.target_treasures = int(line.split(':')[1].strip())
                elif 'Traps:' in line:
                    self.target_traps = int(line.split(':')[1].strip())
                continue
            if line:
                grid_lines.append(line)

        self.height = len(grid_lines)
        self.width = max(len(line) for line in grid_lines) if grid_lines else 0

        walkable: List[Point] = []

        for y, line in enumerate(grid_lines):
            for x, char in enumerate(line):
                p = Point(y, x)
                if char == 'X':
                    self.walls.add(p)
                elif char == 'P':
                    self.player_start = p
                    walkable.append(p)
                elif char == 'T':
                    self.treasures.add(p)
                elif char == 'D':
                    self.traps.add(p)
                elif char == ' ':
                    walkable.append(p)

        # Randomly place treasures if needed
        if len(self.treasures) < self.target_treasures:
            needed = self.target_treasures - len(self.treasures)
            potential = [p for p in walkable if p != self.player_start and p not in self.treasures and p not in self.traps]
            if len(potential) >= needed:
                chosen = random.sample(potential, needed)
                for p in chosen:
                    self.treasures.add(p)

        # Randomly place traps if needed
        if len(self.traps) < self.target_traps:
            needed = self.target_traps - len(self.traps)
            potential = [p for p in walkable if p != self.player_start and p not in self.treasures and p not in self.traps]
            if len(potential) >= needed:
                chosen = random.sample(potential, needed)
                for p in chosen:
                    self.traps.add(p)

class GameSession:
    def __init__(self, level: Level) -> None:
        self.level = level
        self.player_pos = level.player_start
        self.treasures_collected: Set[Point] = set()
        self.is_finished = False
        self.reset_needed = False

    def move_player(self, dy: int, dx: int) -> None:
        new_pos = Point(self.player_pos.y + dy, self.player_pos.x + dx)
        if new_pos not in self.level.walls:
            self.player_pos = new_pos
            self.check_collisions()

    def check_collisions(self) -> None:
        if self.player_pos in self.level.treasures:
            self.treasures_collected.add(self.player_pos)
            if len(self.treasures_collected) == len(self.level.treasures):
                self.is_finished = True

        if self.player_pos in self.level.traps:
            self.reset_needed = True
