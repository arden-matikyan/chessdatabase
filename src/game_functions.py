from config import Config

class GameFunctions:
    def __init__(self):
        self.r = Config.get_redis_connection()

    def search_sequence_in_player_games(self, player_id, move1, move2, move3):
        sequence = f'{move1}>{move2}>{move3}'

        all_games_with_sequence = self.r.smembers(f'sequence:{sequence}:games')
        player_games = self.r.smembers(f'player:{player_id}:games')
        
        result_games = all_games_with_sequence.intersection(player_games)
        return list(result_games)
    
    def search_sequence_in_all_games(self, move1, move2, move3):

        sequence = f'{move1}>{move2}>{move3}'
        games = self.r.smembers(f'sequence:{sequence}:games')
        
        return list(games)
    
if __name__ == "__main__":
    
    user_id = input("Player functions. \n enter username: ")
    game_functions = GameFunctions()

    choice = input("Choose an option: \n 1. Search for a sequence in a player's games, \n 2. Search for a sequence in all games \n")
    if choice == '2':
        move1 = input("Enter the first move: ")
        move2 = input("Enter the second move: ")
        move3 = input("Enter the third move: ")
        
        games_with_sequence = game_functions.search_sequence_in_all_games(move1, move2, move3)
        print(f"Games with sequence {move1} > {move2} > {move3}: {', '.join(games_with_sequence)}")
    
    elif choice == '1':
        # Get the player ID from the user
        user_id = input("Enter the player ID: ")
        move1 = input("Enter the first move: ")
        move2 = input("Enter the second move: ")
        move3 = input("Enter the third move: ")
        
        games_with_sequence = game_functions.search_sequence_in_player_games(user_id, move1, move2, move3)
        print(f"Games with sequence {move1} > {move2} > {move3}: {', '.join(games_with_sequence)}")
    else:
        print("Invalid choice.")