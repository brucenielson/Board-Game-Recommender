import math
import os
import pickle
import statistics
import time

import mysql.connector
import numpy as np

DEBUG = False

# Pickle Functions
def get_pickled_list(list_name):
    # f = open(os.path.dirname(__file__) + "\\" + list_name + '.txt')
    f = open(list_name + '.pkl', "rb")
    sl = pickle.load(f)
    f.close()
    return sl


def pickle_list(list_data, list_name):
    # file_name = os.path.dirname(__file__) + "\\" + str(list_name) + '.txt'
    file_name = list_name + '.pkl'
    # noinspection PyPep8
    try:
        os.remove(file_name)
    except:
        pass

    # f = open(os.path.dirname(__file__) + "\\" + str(list_name) + '.txt', 'wb')
    f = open(list_name + '.pkl', 'wb')
    pickle.dump(list_data, f)
    f.close()


def file_exists(list_name):
    list_name = list_name + ".pkl"
    return os.path.exists(list_name) and os.path.isfile(list_name)



# Database / SQL Class
class GameDB:

    # Connect to the database and query a game.
    # Make class variable
    mydb = mysql.connector.connect(
        host="dallas113.arvixeshared.com",
        user="nikkala_genie",
        passwd="RepMjhaqfM9y58m",
        database="nikkala_boardgamegenie",
        auth_plugin='mysql_native_password'
    )

    @staticmethod
    def execute_sql(sql, execute_all=True):
        if DEBUG:
            start = time.time()
            print(sql)
        mycursor = GameDB.mydb.cursor()
        mycursor.execute(sql)
        if execute_all:
            results = mycursor.fetchall()
            if DEBUG:
                end = time.time()
                # noinspection PyUnboundLocalVariable
                print(len(results),"rows returned in "+str(round(end-start,2))+" seconds")
            return results
        else:
            return mycursor


    def get_ratings(self, game_ids, user_ids):
        games_str = ','.join(map(str, game_ids))
        user_str = ','.join(map(str, user_ids))
        sql = "SELECT user, game, rating FROM rating WHERE game in ("+games_str+") and user in ("+user_str+") ORDER BY user"
        game_ratings = self.execute_sql(sql)
        user_ratings = {}
        for i in range(len(game_ratings)):
            # count += 1
            # if DEBUG and count % 10000 == 0:
            #     print(count)
            user = int(game_ratings[i][0])
            game = int(game_ratings[i][1])
            rating = int(game_ratings[i][2])
            if user not in user_ratings:
                user_ratings[user] = {}
            user_ratings[user][game] = int(rating)
        return user_ratings


    # Gets the top games that have at least some number of votes (required_votes) and returns them ordered by weighted rating
    # Numpy array returned is in this column order: Game ID, Game Name, Rating, # Votes, Weighted Rating
    def get_top_games(self, top_games):
        # Get average rating
        sql = "SELECT avg(rating) FROM rating, (SELECT game.game as id, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game ORDER BY rating DESC) game_rating WHERE rating.game = game_rating.id LIMIT "+str(top_games)
        c = self.execute_sql(sql)[0][0]

        sql = "SELECT game.game, game.name, AVG(rating.rating) as rating, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game ORDER BY votes DESC, rating DESC LIMIT "+str(top_games)
        result = self.execute_sql(sql)
        game_ratings = np.array(result, dtype=object)#dtype=([('id', int), ('name', '<U256'), ('raw_rating', float), ('votes', int)]))

        # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C - from https://stats.stackexchange.com/questions/6418/rating-system-taking-account-of-number-of-votes
        weighted_rating = (game_ratings[:,3:4] / (game_ratings[:,3:4] + top_games)) * game_ratings[:, 2:3].astype(float) + (top_games / ((game_ratings[:, 3:4]) + top_games)) * float(c)
        game_ratings = np.hstack((game_ratings, weighted_rating))
        # Sort by weighted rating. Python, lamely, can't do a sort in numpy easily. But this trick worked: https://stackoverflow.com/questions/16486252/is-it-possible-to-use-argsort-in-descending-order
        game_ratings = game_ratings[(-game_ratings[:, 4]).argsort()]
        return game_ratings[:,0].tolist(), game_ratings[:,0:2].tolist()


    def get_all_games(self):
        sql = "SELECT game.game, game.name FROM game ORDER BY game.game DESC"
        return self.execute_sql(sql)


    # Get a list of every single user id
    def get_all_users(self):
        sql = "SELECT user.user FROM user"
        result = self.execute_sql(sql)
        user_ids = np.array(result)
        return user_ids.flatten().tolist()


    # Get a list of distinct users in our list of top games
    def get_users(self, top_users=None, game_ids=None):
        sql = "SELECT distinct user FROM (SELECT distinct rating.user as user, count(*) as num_ratings FROM rating"

        if game_ids is not None:
            games_str = ','.join(map(str, game_ids))
            games_str = " WHERE game in ("+ games_str +")"
            sql = sql + games_str

        sql = sql + " GROUP BY rating.user ORDER BY num_ratings ASC, rating.user ASC) rating_counts WHERE num_ratings >= 5"

        if top_users is not None:
            sql = sql + " LIMIT " + str(top_users)

        result = self.execute_sql(sql)

        user_ids = np.array(result)
        return user_ids.flatten().tolist()


# Main Recommonder Class
class Recommender:
    db = GameDB()
    def __init__(self, top_games=2000, num_users=100000, reload=False):
        # top_games is how many of the 2000 games to work with
        # num_users is the number of users to use -- note: starting with those with fewest ratings but 5 or more ratings
        # reload: If this flag is set to True, reload data from the database instead of from pickled files. However, if pickle files are missing, load from database regardless.

        # Try loading from pickle first unless asked not to
        # Pickle file names
        fn_game_ids = "game_ids"
        fn_game_names = "game_names"

        # Basic pattern I reuse here: if reload flag is set or if file doesn't exist, then load data via sql. Otherise use pickle file to get data.
        if reload or not (file_exists(fn_game_ids) and file_exists(fn_game_names)):
            # load file from database
            self.game_ids, self.game_names = Recommender.db.get_top_games(top_games)
            # pickle it for next time
            pickle_list(self.game_ids,fn_game_ids)
            pickle_list(self.game_names,fn_game_names)
        else:
            self.game_ids = get_pickled_list(fn_game_ids)
            self.game_names = get_pickled_list(fn_game_names)

        file_name = "user_ids"
        if reload or not file_exists(file_name):
            # load file from database
            self.user_ids = Recommender.db.get_users(num_users, self.game_ids)
            # pickle it for next time
            pickle_list(self.user_ids, file_name)
        else:
            self.user_ids = get_pickled_list(file_name)

        file_name = "user_ratings"
        if reload or not file_exists(file_name):
            # load file from database
            self.user_ratings = Recommender.db.get_ratings(self.game_ids, self.user_ids)
            # pickle it for next time
            pickle_list(self.user_ratings, file_name)
        else:
            self.user_ratings = get_pickled_list(file_name)

        # Remake game names into a dictionary
        game_id_to_name = {}
        game_name_to_id = {}
        for game in self.game_names:
            game_id = game[0]
            name = game[1]
            game_id_to_name[game_id] = name
            game_name_to_id[name] = game_id

        self.game_id_to_name = game_id_to_name
        self.game_name_to_id = game_name_to_id


    # Given two users, determine the euclidean distance they are from each other and then return that inverted
    # so that users closer together get higher score and users farther apart get lower scores
    def euclidean_distance(self, user_id1, user_id2):
        # Get the list of mutual items
        user1_ratings = self.user_ratings[user_id1]
        user2_ratings = self.user_ratings[user_id2]

        # Create list of mutual ratings
        mutal_ratings = {}
        for game_id in user1_ratings:
            if game_id in user2_ratings:
                mutal_ratings[game_id] = True

        # if nothing in common, correlation is 0.0
        mutal_count = len(mutal_ratings)
        if mutal_count == 0:
            return 0.0, 0

        # Add up the squares of all the differences
        sum_of_squares = sum([ (user1_ratings[game_id] - user2_ratings[game_id])**2.0 for game_id in mutal_ratings] )

        ret = (1 / (1 + math.sqrt(sum_of_squares)), mutal_count)
        return ret


    # Given a specific user, get recommendations for them.
    def get_recommendations(self, user_id, top=5, min_users = 50, sim_func = None):
        # user_id is the id of the user we want to get recommendations for. You must first create a user using the other functions.
        # top = number of recommendations to return
        # min_users = the minimum number of users we want to base our recommendations on. If less than this, it will expand the search until it finds enough.
        # You may pass your own sim_function or it will default to euclidean distance
        if sim_func is None:
            sim_func = self.euclidean_distance

        # Start lists and dictionaries we'll use
        totals = {}
        sim_sum = {}
        user_ratings = self.user_ratings
        scores = []
        users = []
        mutual = []

        # Create a list of mutual rated games, i.e. games that user_id and user2 both rated.
        for user2 in user_ratings:
            if user2 == user_id: continue
            # Get similarity score using our prefered similarity function
            sim_score, mutual_count = sim_func(user_id, user2)
            if sim_score <= 0.0: continue
            # Creating 3 indexed lists: scores, users, number of mutual ratings
            scores.append(sim_score)
            users.append(user2)
            mutual.append(mutual_count)

        if DEBUG:
            assert len(users) == len(scores)

        # Get mean and standard deviation of all scores we're looking at to use as cut off points because we don't want users too uncorrelated with our interests.
        # This seems wrong to me. We should really take mean and std from the final set, not the full set. But I tried that and the results didn't seem quite as good. Needs more research.
        mean_score = np.mean(scores)
        std_score = np.std(scores)

        # Determine the minimum number of mutual games in the data set that we accept
        # We ideally want 5 matches, but if the user has less than 5 then go with the minimum number of matches as a first try.
        max_mutual = max(mutual)
        min_mutual = min(max_mutual, 5)
        if DEBUG:
            print("min votes", min_mutual, "max", max_mutual)

        # How many users in our database do in fact have all the same ratings (up to 5) as we do?
        user_count = sum(mutual[i] >= min_mutual for i in range(len(users)))
        # See if the number of users with this number of mutual matched ratings is enough to fulfill the minimum.
        # If not, then reduce requirement by 1 and repeat until we find enough.
        while user_count < min_users and min_mutual > 0:
            min_mutual = max(min_mutual - 1 , 0)
            user_count = sum(mutual[i] >= min_mutual for i in range(len(scores)))

        # # Get new set of scores, medians, standard deviation now that we know the exact users and ratings we intend to use.
        # reduced_scores = [scores[i] for i in range(len(scores)) if mutual[i] >= min_mutual]
        # mean_score = np.mean(reduced_scores)
        # std_score = np.std(reduced_scores)

        vote_count = {}
        # Now that we have a list of users with the most possible mutually matching ratings, but enough to fufill the minimum, loop over them and do the math
        for i in range(len(users)):
            user2 = users[i]
            sim_score = scores[i] # R
            mutual_count = mutual[i] # v
            if mutual_count < min_mutual: continue

            # Drop people too different in tastes, i.e. more than one standard deviation below the mean score
            if sim_score < mean_score - (std_score * 1.0): continue

            # For each user, loop over the games they've rated and track the sum of the scores as well as the number of votes cast
            for game_id in user_ratings[user2]:
                # Only score items I haven't yet
                if game_id not in user_ratings[user_id]:
                    # Similarity * Score
                    totals.setdefault(game_id,0)
                    totals[game_id]+=user_ratings[user2][game_id]*sim_score
                    # sum of similarities
                    sim_sum.setdefault(game_id,0)
                    sim_sum[game_id] += sim_score
                    vote_count[game_id] = vote_count.setdefault(game_id,0) + 1

        # Create an indexed list of votes counts (previously a dictionary) and use that to get a max_count of votes,
        # and a threshold of the number of counts needed to be included.
        vote_count2 = [vote_count[item] for item in vote_count]
        max_count = max(vote_count2)
        threshold = min((statistics.median(vote_count2) + max_count)/2, len(vote_count)*0.05)

        # Create normalized list
        rankings=[[round(total/sim_sum[item],2), item, self.game_id_to_name[item], vote_count[item]] for item, total in totals.items() if vote_count[item] > float(threshold)]

        # Weight based on number of votes so that games with more votes are normalized against games with fewer votes
        ratings = [rank[0] for rank in rankings] # R
        avg_rate = sum(ratings) / float(len(ratings)) # C
        votes = [rank[3] for rank in rankings]
        min_votes = min(votes)
        if DEBUG:
            print("Min Votes:", min_votes)

        if DEBUG:
            assert len(rankings) == len(ratings) == len(votes)

        # Create weighted ratings
        for i in range(len(rankings)):
            # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
            weighted_score = (votes[i] / (votes[i]+min_votes)) * ratings[i] + (min_votes / (votes[i]+min_votes)) * avg_rate
            rankings[i][0] = round(weighted_score,1)


        # Return sorted list
        rankings.sort(reverse=True)
        return rankings[0:top]


    # Given a user_id, return a list of game ratings that have names of games rather than game_id
    def get_game_ratings_by_name(self, user_id):
        game_ratings = {}
        for game_id in self.user_ratings[user_id]:
            name = self.game_id_to_name[game_id]
            game_ratings[name] = self.user_ratings[user_id][game_id]

        return game_ratings


    # Call this method to add an empty user into the Recommender class. The new id is returned for use with other functions.
    def add_user(self):
        user_ratings = self.user_ratings
        id_list = [user_id for user_id in user_ratings]
        next_id = max(id_list)
        user_ratings[next_id] = {}
        return next_id

    # Given a user_id, and a game_id, and a rating, enter this into our dictionary of game ratings.
    def add_game(self, user_id, game_id, rating):
        if type(game_id) == int:
            self.user_ratings[user_id][game_id] = rating
        elif type(game_id) == str:
            game_id = self.game_name_to_id[game_id]
            self.user_ratings[user_id][game_id] = rating
        else:
            raise Exception("'game' must be integer id or string name.")


# Finally, a function to print out the recommendations returned. Useful for debugging, but not for actual web use.
def print_recommendations(recommendations):
    for i in range(len(recommendations)):
        print("#", i+1, "Predicted Rating:", recommendations[i][0], "Game:", recommendations[i][2])


def main():
    recommender = Recommender(reload=False, top_games=2000, num_users=150000)
    # user_matches = recommender.top_user_matches(14791, top=20)
    # print(user_matches)

    # recommendations = recommender.get_recommendations(14791, top=25)
    # print(recommendations)
    # print(recommender.get_game_ratings_by_name(14791))

    # Test Set: Fantasy
    print("")
    print("")
    print("Test Set: Fantasy")
    fantasy_id = recommender.add_user()
    recommender.add_game(fantasy_id, 'Gloomhaven', 10)
    recommender.add_game(fantasy_id, 17226, 10)
    recommender.add_game(fantasy_id, 66356, 10)
    # print("User Matches:")
    # print(recommender.top_user_matches(fantasy_id))
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(fantasy_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(fantasy_id, top=10)
    print(recommendations)
    print_recommendations(recommendations)


    # Test Set: Lovecraft
    print("")
    print("")
    print("Test Set: Lovecraft")
    lovecraft_id = recommender.add_user()
    # recommender.add_game(lovecraft_id, 205059, 10)
    recommender.add_game(lovecraft_id, 'Elder Sign', 10)
    recommender.add_game(lovecraft_id, 'Arkham Horror', 10)
    recommender.add_game(lovecraft_id, 83330, 10)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(lovecraft_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(lovecraft_id, top=10)
    print(recommendations)
    print_recommendations(recommendations)

    # Strategy Game
    print("")
    print("")
    print("Test Set: Abstract Strategy")
    strategy_id = recommender.add_user()
    recommender.add_game(strategy_id, 'Chess', 10)
    recommender.add_game(strategy_id, 'Backgammon', 10)
    recommender.add_game(strategy_id, 'Poker', 10)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(strategy_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(strategy_id, top=10)
    print(recommendations)
    print_recommendations(recommendations)


    # Casual Game
    print("")
    print("")
    print("Test Set: Casual Gamer")
    casual_id = recommender.add_user()
    recommender.add_game(casual_id, "Telestrations", 10)
    recommender.add_game(casual_id, 'Sushi Go!', 10)
    recommender.add_game(casual_id, 'The Mind', 10)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(casual_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(casual_id, top=10)
    print(recommendations)
    print_recommendations(recommendations)

    # Other
    print("")
    print("")
    print("Test Set: Just One Game")
    other_id = recommender.add_user()
    recommender.add_game(other_id, 'Mansions of Madness: Second Edition', 10)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(other_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(other_id, top=10)
    print(recommendations)
    print_recommendations(recommendations)
