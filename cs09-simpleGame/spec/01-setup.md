You will implement a simple dungeon crawling game, something like PacMan.
Use Python, the motivation is to create a simple, extensible, clear code -- rather than striving for performance, small binary size, portability, etc.
The audience is small kids, like 5 years -- make it pleasant, vibrant, cozy. But not epileptic crazy.
And make this Retro, command-line, 80s dos game style.

I would like the game to look as follows:
1. There is Intro screen, with an image (use some random logo placeholder, think 128 x 128) and menu to the right of it, with New Game and Quit
2. The New Game takes you to the Level selection, and there will be say 3 levels to choose from at the start (and I will be adding more). The layout will be similar to the previous screen, ie, keep the logo, and have the levels on the right as a list.
3. Each level will be increasing in complexity, ie, adding new elements and rules, as the kid will learn the game.

In the Intro screen, N takes you to New Game, and Quits (which just closes the whole app).

The game itself will basically be just map.
Lets start with ascii for now, ie, P can be player, T a treasure, D a trap.
The first level should be about finding a single "treasure", which is static and does not move. There are no enemies or anything, just rooms and corridors.
The second level should be about finding three treasures.
The third level should additionally add "traps" -- upon hitting the trap, the whole level restarts.

When a level is completed, show a victory screen -- some placeholder image + "congratulations!" label, same layout as the Intro screen.
Then label "For next level, press N, to play the same level, press R, to quit press Q".
During playing the game, Arrows move the player avatar, R restarts, Q quits, N takes you to Intro screen.


Also, regarding strings -- make sure there is a single asset file or something, so that I can change each label or text without locating the place in code.
Similarly for the logo files -- I will change the placeholder images later.

The levels are static in layout, probably make them same asset again, like txt files which I can just edit, for like X being a wall and Space being a walkable place?
And P will be player starting position, T a treasure, D a trap.
Each such asset would start with desired treasure count and trap count.
If there is less than fixed number of traps or treasure, they are randomly placed.
