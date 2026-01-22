import json
from collections import deque
from config import Config  

class GraphQueries:
    def __init__(self):
        self.r = Config.get_redis_connection() # This is to initialize Redis connection
    
    # Returns a list of friends of friends for a given user
    def get_friends_of_friends(self, user_id):
        direct_opponents = set()
        
        # get all direct opponents from user's games
        for game_id in self.r.smembers(f'player:{user_id}:games'):
            game_data = list(self.r.smembers(f'game:{game_id}'))
            if not game_data:
                continue
            game = json.loads(game_data[0])
            opponent = game['white_player_id'] if game['white_player_id'] != user_id else game['black_player_id']
            direct_opponents.add(opponent)

        # to get opponents of direct opponents 
        fof = set()
        for opponent in direct_opponents:
            for game_id in self.r.smembers(f'player:{opponent}:games'):
                game_data = list(self.r.smembers(f'game:{game_id}'))
                if not game_data:
                    continue
                game = json.loads(game_data[0])
                candidate = game['white_player_id'] if game['white_player_id'] != opponent else game['black_player_id']
                if candidate != user_id and candidate not in direct_opponents:
                    fof.add(candidate)
        return list(fof)
    
    #only those with more wins than the user.
    def stronger_foaf(self, user_id):
        try:
            user_wins = int(self.r.get(f'leaderboard:wins:{user_id}') or 0)
        except:
            user_wins = 0
        
        print(f"\nUser: {user_id} has {user_wins} wins")
            
        stronger_players = []
        for player in self.get_friends_of_friends(user_id):
            try:
                wins = int(self.r.get(f'leaderboard:wins:{player}') or 0)
                print(f"{player} has {wins} wins")
                if wins > user_wins:
                    stronger_players.append(player)
            except:
                continue
        return stronger_players


    """
    To calculate the size of the largest connected component in the player graph.
    A connection exists between players if they have played at least one match together and here
    Breadth-first search  is used .
    """
    def longest_connected_component(self):
        all_players = {player for player in self.r.smembers('players')}
        visited = set()
        max_size = 0

        for player in all_players:
            if player not in visited:
                queue = deque([player])
                component_size = 0
                
                while queue:
                    current = queue.popleft()
                    if current not in visited:
                        visited.add(current)
                        component_size += 1
                        
                        # get all opponents of the curremt player
                        for game_id in self.r.smembers(f'player:{current}:games'):
                            game = json.loads(list(self.r.smembers(f'game:{game_id}'))[0])
                            opponent = game['white_player_id'] if game['white_player_id'] != current else game['black_player_id']
                            if opponent not in visited:
                                queue.append(opponent)
                
                max_size = max(max_size, component_size)
        return max_size
    
if __name__ == "__main__":
    print("\n=== Test Graph Queries ===")
    gq = GraphQueries()
    
    sample_player = list(gq.r.smembers('players'))[0]  
    
    print(f"\nTesting with player: {sample_player}")
    print("Friends of friends:", gq.get_friends_of_friends(sample_player))
    print("Stronger FoF:", gq.stronger_foaf(sample_player))
    print("Largest network component:", gq.longest_connected_component())    