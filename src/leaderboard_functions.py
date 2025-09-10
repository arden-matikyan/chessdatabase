from config import Config

class LeaderboardFunctions:
    def __init__(self):
        self.r = Config.get_redis_connection()
    
    def get_top_players(self, limit=10):
        top_players = self.r.lrange('leaderboard:top_players', 0, limit-1)
        
        # Format the results as a list of dictionaries
        result = []
        for player_id in top_players:
            wins = int(self.r.get(f'leaderboard:wins:{player_id}') or 0)
            result.append({
                'player_id': player_id,
                'wins': wins
            })
        
        return result

    def get_bottom_players(self, limit=10):
        bottom_players = self.r.lrange('leaderboard:bottom_players', 0, limit-1)
        
        # Format the results as a list of dictionaries
        result = []
        for player_id in bottom_players:
            losses = int(self.r.get(f'leaderboard:losses:{player_id}') or 0)
            result.append({
                'player_id': player_id,
                'losses': losses
            })
        
        return result


if __name__ == "__main__":
    
    leaderboard_functions = LeaderboardFunctions()
    choice = input("Choose a leaderboard to view: \n 1. Top players \n 2. Bottom players \n")
    if choice == '1':
        top_players = leaderboard_functions.get_top_players()
        print("Top players:")
        for player in top_players:
            print(f"Player ID: {player['player_id']}, Wins: {player['wins']}")
    elif choice == '2':
        bottom_players = leaderboard_functions.get_bottom_players()
        print("Bottom players:")
        for player in bottom_players:
            print(f"Player ID: {player['player_id']}, Losses: {player['losses']}")
    else:
        print("Invalid choice.")