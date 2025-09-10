from config import Config

class AnalyticsFunctions:
    def __init__(self):
        self.r = Config.get_redis_connection()

    def shortest_game(self):
        return {
            'game_id': self.r.get(f'analytics:shortest_game'),
            'number_of_turns': self.r.get(f'analytics:shortest_game_turns')
        }

    def number_of_checks(self, game_id):
        return int(self.r.get(f'game:{game_id}:analytics:check_count') or 0)

    def most_frequent_opening(self):
        opening = self.r.get('analytics:most_frequent_opening')
        count = self.r.get(f'opening:{opening}')
        
        if opening and count:
            return {
                'opening': opening,
                'count': int(count)
            }
        return {'opening': None, 'count': 0}

    def most_common_three_move_sequence(self):
        sequence = self.r.get('analytics:most_common_sequence')
        count = self.r.get(f'sequence:{sequence}')
        
        if sequence and count:
            return {
                'sequence': sequence,
                'count': int(count)
            }
        return {'sequence': None, 'count': 0}
    
    def least_common_three_move_sequence(self):
        sequence = self.r.get('analytics:least_common_sequence')
        count = self.r.get(f'sequence:{sequence}')
        
        if sequence and count:
            return {
                'sequence': sequence,
                'count': int(count)
            }
        return {'sequence': None, 'count': 0}

if __name__ == "__main__":
    analytics_functions = AnalyticsFunctions()
   
    choice = input("Choose an analytics function to run: \n 1. Shortest game \n 2. Number of checks \n 3. Most frequent opening \n 4. Most common three-move sequence \n 5. Least common three-move sequence \n")
    if choice == '1':
        shortest_game = analytics_functions.shortest_game()
        print(f"Shortest game: {shortest_game['game_id']}, Turns: {shortest_game['number_of_turns']}")
    elif choice == '2':
        game_id = input("Enter the game ID: ")
        number_of_checks = analytics_functions.number_of_checks(game_id)
        print(f"Number of checks in game {game_id}: {number_of_checks}")
    elif choice == '3':
        most_frequent_opening = analytics_functions.most_frequent_opening()
        print(f"Most frequent opening: {most_frequent_opening['opening']}, Count: {most_frequent_opening['count']}")
    elif choice == '4':
        most_common_three_move_sequence = analytics_functions.most_common_three_move_sequence()
        print(f"Most common three-move sequence: {most_common_three_move_sequence['sequence']}, Count: {most_common_three_move_sequence['count']}")
    elif choice == '5':
        least_common_three_move_sequence = analytics_functions.least_common_three_move_sequence()
        print(f"Least common three-move sequence: {least_common_three_move_sequence['sequence']}, Count: {least_common_three_move_sequence['count']}")
    else:
        print("Invalid choice.")