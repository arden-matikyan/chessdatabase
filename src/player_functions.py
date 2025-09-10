from config import Config
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class PlayerFunctions:
    def __init__(self):
        self.r = Config.get_redis_connection()
    
    def view_match_history(self, user_id):
        return self.r.smembers(f'player:{user_id}:games')

    def view_scheduled_games(self, user_id):
        return self.r.smembers(f'player:{user_id}:scheduled_games')
    
    def find_player_by_email(self, email):  
        return self.r.execute_command('BF.EXISTS', 'email_filter', email)

    def get_games_between_players(self, player1_id, player2_id):
        return self.r.smembers(f'player_versus:{player1_id}:{player2_id}').union(self.r.smembers(f'player_versus:{player2_id}:{player1_id}'))

    def get_player_most_used_opening(self, user_id):
        openings = self.r.smembers(f'player:{user_id}:openings')
    
        # Handle case where player has no recorded games
        if not openings:
            return None
        
        # Python 3 may need decoding
        if isinstance(next(iter(openings)), bytes):
            openings = [opening.decode('utf-8') for opening in openings]
        
        # Find the opening with the highest count
        max_count = 0
        most_used_opening = None
        
        for opening in openings:
            # Get the count for this opening
            count_key = f'player:{user_id}:opening:{opening}:count'
            count = int(self.r.get(count_key) or 0)
            
            if count > max_count:
                max_count = count
                most_used_opening = opening
        
        return most_used_opening

if __name__ == "__main__":
    
    user_id = input("Player functions. \n enter username: ")
    player_functions = PlayerFunctions()

    choice = input("Choose an option: \n 1. View other player match history \n 2. View scheduled games \n 3. View your match history \n 4. Find player by email \n 5. Get games played with a player \n 6. Get most used opening of another player\n")
    if choice == '1':
        player2_id = input("Enter other player's user_id: ")
        match_history = player_functions.view_match_history(player2_id)
        print("Match History: ", match_history)
    elif choice == '2':
        scheduled_games = player_functions.view_scheduled_games(user_id)
        print("Scheduled Games: ", scheduled_games)
    elif choice == '3':
        match_history = player_functions.view_match_history(user_id)
        print("Match History: ", match_history)
    elif choice == '4':
        email = input("Enter email: ")
        player_exists = player_functions.find_player_by_email(email)
        if player_exists:
            print("Player exists")
        else:
            print("Player does not exist")
    elif choice == '5':
        player2_id = input("Enter second player's user_id: ")
        games = player_functions.get_games_between_players(user_id, player2_id)
        print("Games between players: ", games)
    elif choice == '6':
        player2_id = input("Enter second player's user_id: ")
        most_used_opening = player_functions.get_player_most_used_opening(player2_id)
        print("Most used opening: ", most_used_opening)
    else:
        print("Invalid choice")
