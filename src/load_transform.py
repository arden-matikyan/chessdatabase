from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from config import Config
from datetime import datetime, timedelta
import ast
import csv
import heapq
import json
import logging
import math
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def json_serialize(obj) -> str:
    """Serialize json objects into string.
    """
    return json.dumps(obj)

def json_deserialize(string) -> dict:
    """Deserialize json-serialized strings into dict.
    """
    return json.loads(string)

class RedisChessLoader:
    def __init__(self, players_path: str, schedule_path: str, game_records_path: str):
        self.players_path = players_path
        self.schedule_path = schedule_path
        self.game_records_path = game_records_path
        # Create redis connection
        self.r = Config.get_redis_connection()
        # Create and start a scheduler to manage scheduled games.
        executors = {'default': ThreadPoolExecutor(5)}
        self.scheduler = BackgroundScheduler(executors=executors)
        self.scheduler.start()
        # Confirm before deleting all data
        confirm = input("This will delete all Redis data. Continue? (y/n): ")
        if confirm.lower() == 'y':
            self.r.flushdb()
            # Create a bloom filter for email
            self.r.execute_command('BF.RESERVE', 'email_filter', 0.01, 20000)


    def add_player(self, user_id: str, email: str) -> None:
        """Add player to the Redis database.
        """
        logger.info('Adding player %s', user_id)
        self.r.set(f'player:{user_id}', email)
        # Add email to the email-bloom-filter
        self.r.execute_command('BF.ADD', 'email_filter', email)
        self.r.set(f'email_user:{email}', user_id)
        self.r.sadd('players', user_id)

    def remove_scheduled_game(self, game_id: str, player_1: str, player_2: str) -> None:
        """Remove scheduled game from the Redis database.
        """
        self.r.delete(f'scheduled_game:{game_id}')
        self.r.srem("scheduled_games", game_id)
        self.r.srem(f'player:{player_1}:scheduled_games', game_id)
        self.r.srem(f'player:{player_2}:scheduled_games', game_id)

    def add_schedule(self, game_id: str, player_1: str, player_2: str) -> None:
        """Add scheduled game to the Redis database.
        """
        logger.info('Adding schedule %s', game_id)
        self.r.set(f'scheduled_game:{game_id}', json_serialize([player_1, player_2]))
        self.r.sadd('scheduled_games', game_id)
        self.r.sadd(f'player:{player_1}:scheduled_games', game_id)
        self.r.sadd(f'player:{player_2}:scheduled_games', game_id)
        # Set an expiry timer for the scehduled game
        self.r.expire(f"scheduled_game:{game_id}", 72 * 3600)
        # Add job to update all data relevant to the scheduled_game after 72 hours
        self.scheduler.add_job(
            self.remove_scheduled_game,
            trigger='date',
            run_date=datetime.now() + timedelta(hours=72),
            args=[game_id, player_1, player_2],
            id=f"remove_{game_id}"
        )
        logger.info("Scheduled removal of game %s for players %s and %s", game_id, player_1, player_2)

    def add_game_record(self, game_id: str, moveset: list, winner: str, victory_status: str, number_of_turns: str, white_player_id: str, black_player_id: str, opening_eco: str) -> None:
        """Add game record to the Redis database.
        """
        logger.info('Adding game record %s', game_id)
        self.r.sadd(f'game:{game_id}', json_serialize({
            'moveset': moveset,
            'winner': winner,
            'victory_status': victory_status,
            'number_of_turns': number_of_turns,
            'white_player_id': white_player_id,
            'black_player_id': black_player_id,
            'opening_eco': opening_eco
        }))
        self.r.sadd(f'player:{white_player_id}:games', game_id)
        self.r.sadd(f'player:{black_player_id}:games', game_id)
        self.r.sadd(f'player_versus:{white_player_id}:{black_player_id}', game_id)
        winning_player = losing_player = None
        if winner.lower() == 'white':
            winning_player = white_player_id
            losing_player = black_player_id
        elif winner.lower() == 'black':
            winning_player = black_player_id
            losing_player = white_player_id
        # Update leaderboard status when either white or black wins
        if winning_player is not None and losing_player is not None:
            self.r.incr(f'leaderboard:wins:{winning_player}')
            self.r.incr(f'leaderboard:losses:{losing_player}')
            heap = [] # Track the top 10 wins
            player_in_leaderboard_wins = False
            for player in self.r.lrange('leaderboard:top_players', 0, -1):
                wins = int(self.r.get(f'leaderboard:wins:{player}'))
                if player == winning_player:
                    player_in_leaderboard_wins = True
                heapq.heappush(heap, (-wins, player))
            if not player_in_leaderboard_wins:
                heapq.heappush(heap, (-int(self.r.get(f'leaderboard:wins:{winning_player}')), winning_player))
            self.r.delete('leaderboard:top_players')
            for i in range(min(10, len(heap))):
                self.r.rpush('leaderboard:top_players', heapq.heappop(heap)[1])
            heap = [] # Track the top 10 losses
            player_in_leaderboard_losses = False
            for player in self.r.lrange(f'leaderboard:bottom_players', 0, -1):
                losses = int(self.r.get(f'leaderboard:losses:{player}'))
                if player == losing_player:
                    player_in_leaderboard_losses = True
                heapq.heappush(heap, (-losses, player))
            if not player_in_leaderboard_losses:
                heapq.heappush(heap, (-int(self.r.get(f'leaderboard:losses:{losing_player}')), losing_player))
            self.r.delete('leaderboard:bottom_players')
            for i in range(min(10, len(heap))):
                self.r.rpush('leaderboard:bottom_players', heapq.heappop(heap)[1])
        # Add openings to the set of openings of each player and increment their count
        self.r.sadd(f'player:{white_player_id}:openings', opening_eco)
        self.r.sadd(f'player:{black_player_id}:openings', opening_eco)
        self.r.incr(f'player:{white_player_id}:opening:{opening_eco}:count')
        self.r.incr(f'player:{black_player_id}:opening:{opening_eco}:count')
        self.r.incr(f'opening:{opening_eco}')
        # Update the most frequent opening if necessary
        try:
            most_frequent_opening = self.r.get(f'analytics:most_frequent_opening')
            if int(self.r.get(f'opening:{opening_eco}')) >= int(self.r.get(f'opening:{most_frequent_opening}')):
                self.r.set(f'analytics:most_frequent_opening', opening_eco)
        except TypeError:
            self.r.set(f'analytics:most_frequent_opening', opening_eco)
        count_of_checks = 0 # Track the count of checks in the game
        sequence_counts = dict() # Track the count of times a sequence was played throughout all games including this
        for i in range(len(moveset) - 2):
            sequence = f'{moveset[i]}>{moveset[i+1]}>{moveset[i+2]}'
            if sequence in sequence_counts:
                sequence_counts[sequence] += 1
            else:
                try:
                    sequence_counts[sequence] = 1 + int(self.r.get(f'sequence:{sequence}'))
                except TypeError:
                    sequence_counts[sequence] = 1
            # Increment `count_of_checks` if the current move resulted in a check
            if '+' in moveset[i]:
                count_of_checks += 1
        max_count, max_count_sequence = 0, '' # Track the sequence with the maximum count
        min_count, min_count_sequence = math.inf, '' # Track the sequence with the minimum count
        for sequence, count in sequence_counts.items():
            if max_count < count:
                max_count = count
                max_count_sequence = sequence
            if min_count > count:
                min_count = count
                min_count_sequence = sequence
            # Update count of sequence globally and per player
            self.r.set(f'sequence:{sequence}', count)
            self.r.sadd(f'sequence:{sequence}:games', game_id)
            self.r.sadd(f'player:{white_player_id}:sequences', sequence)
            self.r.sadd(f'player:{black_player_id}:sequences', sequence)
        # Update the most common sequence if necessary
        current_most_common_sequence = self.r.get('analytics:most_common_sequence')
        try:
            current_most_common_sequence_count = sequence_counts.get(current_most_common_sequence, int(self.r.get(f'sequence:{current_most_common_sequence}')))
        except TypeError:
            current_most_common_sequence_count = 0
        if max_count >= current_most_common_sequence_count:
            self.r.set(f'analytics:most_common_sequence', max_count_sequence)
        # Update the least common sequence if necessary
        current_least_common_sequence = self.r.get('analytics:least_common_sequence')
        try:
            current_least_common_sequence_count = sequence_counts.get(current_least_common_sequence, int(self.r.get(f'sequence:{current_least_common_sequence}')))
        except TypeError:
            current_least_common_sequence_count = math.inf
        if min_count <= current_least_common_sequence_count:
            self.r.set(f'analytics:least_common_sequence', min_count_sequence)
        # Check for checks in the last 2 moves as they are not considered in the loop for sequences
        for move in moveset[-2:]:
            if '+' in move:
                count_of_checks += 1
        # Set the count of checks for the game
        self.r.set(f'game:{game_id}:analytics:check_count', count_of_checks)
        # Update the game with the shortest turns if necessary
        if not self.r.exists('analytics:shortest_game_turns') or number_of_turns <= int(self.r.get('analytics:shortest_game_turns')):
            self.r.set('analytics:shortest_game_turns', number_of_turns)
            self.r.set('analytics:shortest_game', game_id)
        # Remove the game from scheduled games after adding to the records
        self.remove_scheduled_game(game_id, white_player_id, black_player_id)

    def load_players(self) -> None:
        """Load players into Redis
        """
        logger.info('Loading players...')
        with open(self.players_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.add_player(row['user_id'], row['email'])

    def load_schedules(self) -> None:
        """Load schedules into Redis
        """
        logger.info('Loading schedules...')
        with open(self.schedule_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.add_schedule(row['game_id'], row['player_1'], row['player_2'])

    def load_game_records(self) -> None:
        """Load game records into Redis"""
        logger.info('Loading game records...')
        with open(self.game_records_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.add_game_record(
                    game_id = row['game_id'],
                    moveset = ast.literal_eval(row['moveset']),
                    winner = row['winner'],
                    victory_status = row['victory_status'],
                    number_of_turns = int(row['number_of_turns']),
                    white_player_id = row['white_player_id'],
                    black_player_id = row['black_player_id'],
                    opening_eco = row['opening_eco']
                )


if __name__ == "__main__":
    loader = RedisChessLoader(
        players_path = Config.players_path,
        schedule_path = Config.schedule_path,
        game_records_path = Config.game_records_path
    )
    logger.info("\nData loading...")
    loader.load_players()
    loader.load_schedules()
    loader.load_game_records()
    logger.info("\nData loading complete!")
    logger.info("The loader will remain alive to manage scheduled games. Use ^C to exit (WARNING: Scheduled games will not be managed upon exit).")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        loader.scheduler.shutdown()
        logger.info("Scheduler shut down")
