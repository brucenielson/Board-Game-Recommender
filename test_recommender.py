from unittest import TestCase
import Recommender

class TestRecommender(TestCase):
    def test_fantasy_set(self):
        recommender = Recommender.Recommender(reload=False, top_games=2000, num_users=150000)
        id = recommender.add_user()
        recommender.add_game(id, 'Gloomhaven', 10)
        recommender.add_game(id, 17226, 10)
        recommender.add_game(id, 66356, 10)
        results = recommender.get_game_ratings_by_name(id)
        test = (results == {'Gloomhaven': 10, 'Descent: Journeys in the Dark': 10, 'Dungeons & Dragons: Wrath of Ashardalon Board Game': 10})
        self.assertTrue(test)
        recommendations = recommender.get_recommendations(id, top=10)
        test = (recommendations == [[8.0, 205059, 'Mansions of Madness: Second Edition', 110], [8.0, 180263, 'The 7th Continent', 90], [8.0, 161936, 'Pandemic Legacy: Season 1', 116], [8.0, 96848, 'Mage Knight Board Game', 191], [7.9, 205637, 'Arkham Horror: The Card Game', 115], [7.9, 164153, 'Star Wars: Imperial Assault', 134], [7.9, 72125, 'Eclipse', 106], [7.8, 167791, 'Terraforming Mars', 123], [7.8, 59946, 'Dungeons & Dragons: Castle Ravenloft Board Game', 195], [7.8, 12493, 'Twilight Imperium (Third Edition)', 117]])
        self.assertTrue(test)

    def test_lovecraft_set(self):
        recommender = Recommender.Recommender(reload=False, top_games=2000, num_users=150000)
        id = recommender.add_user()
        recommender.add_game(id, 'Elder Sign', 10)
        recommender.add_game(id, 'Arkham Horror', 10)
        recommender.add_game(id, 83330, 10)
        results = recommender.get_game_ratings_by_name(id)
        test = (results == {'Elder Sign': 10, 'Arkham Horror': 10, 'Mansions of Madness': 10})
        self.assertTrue(test)
        recommendations = recommender.get_recommendations(id, top=10)
        test = (recommendations == [[8.2, 205059, 'Mansions of Madness: Second Edition', 128], [8.0, 205637, 'Arkham Horror: The Card Game', 115], [8.0, 12333, 'Twilight Struggle', 84], [7.9, 164153, 'Star Wars: Imperial Assault', 77], [7.9, 146021, 'Eldritch Horror', 261], [7.9, 121921, 'Robinson Crusoe: Adventures on the Cursed Island', 110], [7.9, 12493, 'Twilight Imperium (Third Edition)', 97], [7.8, 124742, 'Android: Netrunner', 120], [7.8, 37111, 'Battlestar Galactica: The Board Game', 182], [7.7, 150376, 'Dead of Winter: A Crossroads Game', 137]])
        self.assertTrue(test)


    def test_strategy_set(self):
        recommender = Recommender.Recommender(reload=False, top_games=2000, num_users=150000)
        id = recommender.add_user()
        recommender.add_game(id, 'Chess', 10)
        recommender.add_game(id, 'Backgammon', 10)
        recommender.add_game(id, 'Poker', 10)
        results = recommender.get_game_ratings_by_name(id)
        test = (results == {'Chess': 10, 'Backgammon': 10, 'Poker': 10})
        self.assertTrue(test)
        recommendations = recommender.get_recommendations(id, top=10)
        test = (recommendations == [[7.3, 3076, 'Puerto Rico', 217], [7.2, 12333, 'Twilight Struggle', 87], [7.2, 188, 'Go', 205], [7.1, 36218, 'Dominion', 208], [7.1, 2651, 'Power Grid', 158], [7.1, 483, 'Diplomacy', 111], [7.1, 42, 'Tigris & Euphrates', 106], [7.0, 31260, 'Agricola', 148], [7.0, 14996, 'Ticket to Ride: Europe', 174], [7.0, 13, 'Catan', 384]])
        self.assertTrue(test)

    def test_casual_set(self):
        recommender = Recommender.Recommender(reload=False, top_games=2000, num_users=150000)
        id = recommender.add_user()
        recommender.add_game(id, "Telestrations", 10)
        recommender.add_game(id, 'Sushi Go!', 10)
        recommender.add_game(id, 'The Mind', 10)
        results = recommender.get_game_ratings_by_name(id)
        test = (results == {'Telestrations': 10, 'Sushi Go!': 10, 'The Mind': 10})
        self.assertTrue(test)
        recommendations = recommender.get_recommendations(id, top=10)
        test = (recommendations == [[7.7, 148228, 'Splendor', 18], [7.7, 68448, '7 Wonders', 22], [7.7, 9209, 'Ticket to Ride', 16], [7.6, 230802, 'Azul', 20], [7.5, 129622, 'Love Letter', 19], [7.4, 14996, 'Ticket to Ride: Europe', 17], [7.4, 822, 'Carcassonne', 18], [7.4, 13, 'Catan', 22], [7.3, 178900, 'Codenames', 26], [7.3, 70323, 'King of Tokyo', 17]])
        self.assertTrue(test)
