# Part 2

## 3.4 Player-Centric Functionality

### 1. View Another Player’s Match History
- **Key:** `player:{user_id}:games`
- **Value:** `[{game_id_1}, {game_id_2}, ...]`
- **Write / Update:**  
  When a new game is created, extract `white_player_id` and `black_player_id`, and add `game_id` to `player:{user_id}:games` for both players.
- **Read:**  
  Read `player:{target_player_id}:games` and return the list of game IDs.
- **Interactions:**  
  - 2 writes on game creation  
  - 1 read on view

---

### 2. List All Future Games a Player Is Scheduled to Play
- **Key:** `player:{player_id}:scheduled_games`
- **Value:** `[{game_id_1}, {game_id_2}, ...]`
- **Write / Update:**  
  When a future game is scheduled, add `game_id` to `player:{user_id}:scheduled_games` for both players.  
  A helper function automatically removes the `game_id` after the scheduled time expires.
- **Read:**  
  Read `player:{target_user_id}:scheduled_games` and return the list of scheduled game IDs.
- **Interactions:**  
  - 2 writes on schedule  
  - 1 read on view

---

### 3. Check if a Friend Is a Member by Email (Bloom Filter)
- **Key:** `email_user:{email_id}`
- **Value:** `{user_id}`
- **Write / Update:**  
  When a user registers:
  - `SET email_user:{email} = {user_id}`
  - Add the email to the Bloom Filter (`BF.ADD`)
- **Read:**  
  - Check Bloom Filter first  
  - If true, check `email_user:{email}` and return `user_id` if it exists
- **Interactions:**  
  - Registration: 2 writes (`SET`, `BF.ADD`)  
  - Lookup: 1–2 reads (`BF.EXISTS`, optional `GET`)

---

### 4. View Match History Between Two Players
- **Key:** `player_versus:{playerA_id}:{playerB_id}`
- **Value:** `[{game_id_1}, {game_id_2}, ...]`
- **Write / Update:**  
  On new game creation, determine `playerA` and `playerB` from `white_player_id` and `black_player_id`, then add `game_id` to the key.
- **Read:**  
  Return the union of:
  - `player_versus:{playerA}:{playerB}`
  - `player_versus:{playerB}:{playerA}`
- **Interactions:**  
  - 1 write on game creation  
  - 2 reads on view
- **Note:**  
  Players are not stored lexicographically to allow future filtering by white/black roles.

---

### 5. View Most Used Opening by a Player
- **Keys:**  
  - `player:{user_id}:opening:{opening_eco}:count`  
  - `player:{user_id}:openings`
- **Values:**  
  - Opening usage count (integer)  
  - Set of opening ECO codes
- **Write / Update:**  
  On each new game:
  - Increment opening count for both players  
  - Add opening to player’s opening set  
  - Increment global opening counter
- **Read:**  
  - Retrieve all player openings  
  - Fetch usage count for each  
  - Return opening with highest count
- **Interactions:**  
  - Reads: N (based on unique openings used)  
  - Writes: 5 per game insert

---

## 3.5 Leaderboard Functionality

### 1. Top 10 Most Successful Players (Most Wins)
- **Keys:**  
  - `leaderboard:wins:{player_id}`  
  - `leaderboard:top_players`
- **Values:**  
  - Integer win count  
  - Ordered list of player IDs
- **Write / Update:**  
  On decisive game outcome:
  - Increment winner’s win count  
  - Update leaderboard ordering
- **Read:**  
  Return top 10 players with their win counts.
- **Interactions:**  
  - Writes:  
    - 1 `INCR`  
    - 1 `LRANGE`  
    - Up to 10 `GET`  
    - 1 `DELETE` + up to 10 `RPUSH`
  - Reads:  
    - 1 `LRANGE`  
    - Up to 10 `GET`

---

### 2. Bottom 10 Least Successful Players (Most Losses)
- **Keys:**  
  - `leaderboard:losses:{player_id}`  
  - `leaderboard:bottom_players`
- **Values:**  
  - Integer loss count  
  - Ordered list of player IDs
- **Write / Update:**  
  On decisive game outcome:
  - Increment loser’s loss count  
  - Update leaderboard ordering
- **Read:**  
  Return bottom 10 players with their loss counts.
- **Interactions:**  
  Same as Top 10 leaderboard, using loss counters.

---

## 3.6 Game Functionality

### 1. Search for a Three-Move Sequence by Player
- **Keys:**  
  - `sequence:{move1}>{move2}>{move3}`  
  - `sequence:{move1}>{move2}>{move3}:games`  
  - `player:{user_id}:sequences`
- **Values:**  
  - Sequence count  
  - Set of game IDs  
  - Set of sequences used by player
- **Write / Update:**  
  For each game with N moves:
  - Process up to `(N - 2)` three-move sequences  
  - Increment sequence count  
  - Add game ID to sequence set  
  - Add sequence to each player’s set
- **Read:**  
  - Get games containing the sequence  
  - Get games the player participated in  
  - Return intersection
- **Interactions:**  
  - Writes: Up to `4(N - 2)` operations  
  - Reads: 1 `SMEMBERS`

---

### 2. Search for a Three-Move Sequence Globally
- **Keys:**  
  - `sequence:{move1}>{move2}>{move3}`  
  - `sequence:{move1}>{move2}>{move3}:games`
- **Values:**  
  - Sequence count  
  - Set of game IDs
- **Write / Update:**  
  Same sequence processing as player-specific search (without player sets).
- **Read:**  
  Retrieve all games containing the sequence.
- **Interactions:**  
  - Writes: Up to `(N - 2)` per game  
  - Reads: 1 `SMEMBERS`

---

## 3.7 Analytics

### 1. Most Common Three-Move Sequence
- **Keys:**  
  - `analytics:most_common_sequence`  
  - `sequence:{move1}>{move2}>{move3}`
- **Values:**  
  - Sequence string  
  - Sequence count
- **Write / Update:**  
  Compare updated sequence counts and update most common if needed.
- **Read:**  
  - `GET analytics:most_common_sequence`  
  - `GET sequence:{result}`
- **Interactions:**  
  - Writes: Up to `(N - 2)` per game  
  - Reads: 2 `GET`

---

### 2. Least Common Three-Move Sequence (Non-Zero)
- **Keys:**  
  - `analytics:least_common_sequence`  
  - `sequence:{move1}>{move2}>{move3}`
- **Write / Update:**  
  Compare updated counts and update least common if needed.
- **Read:**  
  Same as most common sequence.
- **Interactions:**  
  Same as most common sequence.

---

### 3. Shortest Game by Number of Moves
- **Keys:**  
  - `analytics:shortest_game`  
  - `analytics:shortest_game_turns`
- **Values:**  
  - Game ID  
  - Number of turns
- **Write / Update:**  
  Update if current game has fewer moves.
- **Read:**  
  Read both keys.
- **Interactions:**  
  - Writes: Up to 2 `SET`  
  - Reads: 2 `GET`

---

### 4. Number of Checks in a Game
- **Key:** `game:{game_id}:analytics:check_count`
- **Value:** Integer count of checks
- **Write / Update:**  
  Count `'+'` symbols in moves and store result.
- **Read:**  
  Retrieve check count for game.
- **Interactions:**  
  - Writes: 1 `SET`  
  - Reads: 1 `GET`

---

### 5. Most Frequently Used Opening Overall
- **Keys:**  
  - `analytics:most_frequent_opening`  
  - `opening:{eco_code}`
- **Values:**  
  - ECO code  
  - Opening count
- **Write / Update:**  
  Increment opening count and update most frequent if needed.
- **Read:**  
  Retrieve most frequent opening and its count.
- **Interactions:**  
  - Writes: 1 `INCR`, up to 1 `SET`  
  - Reads: 2 `GET`

---

## Setup & Execution Guide

### 1. Create a Virtual Environment

**macOS / Linux**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows** 
```bash
python -m venv venv
venv\Scripts\activate
```
## 2. Install Required Dependencies

pip install -r requirements.txt


## 3. Ensure Redis Is Running

### macOS (Homebrew)

brew install redis
brew services start redis


### Linux

sudo apt install redis
sudo service redis start


### Docker (Recommended for Bloom Filter support)

docker pull redis:latest
docker run -d --name redis-server -p 6379:6379 redis


## 4. Load Data into Redis

python3 load_transform.py

You will see:
This will delete all Redis data. Continue? (y/n):

Type y and press Enter.

This will:
- Flush existing Redis data
- Load players, schedules, and game records from data/
- Initialize analytics, leaderboards, and expiration timers

Expected output:
INFO — Data loading complete!
INFO — The loader will remain alive to manage scheduled games.
Use ^C to exit (WARNING: Scheduled games will not be managed upon exit).

Note: Keep this terminal open while testing.


## 5. Test Each Functional Module

Activate the virtual environment:

macOS / Linux:
source venv/bin/activate

Windows:
venv\Scripts\activate

Run the test scripts:

python3 graph_functions.py
python3 player_functions.py
python3 leaderboard_functions.py
python3 game_functions.py
python3 analytics_functions.py