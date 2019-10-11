import mysql.connector
import sys
import numpy as np
import time
import math
import pickle
import os

DEBUG = True


def get_pickled_list(list_name):
    # f = open(os.path.dirname(__file__) + "\\" + list_name + '.txt')
    f = open(list_name + '.pkl', "rb")
    sl = pickle.load(f)
    f.close()
    return sl


def pickle_list(list_data, list_name):
    # file_name = os.path.dirname(__file__) + "\\" + str(list_name) + '.txt'
    file_name = list_name + '.pkl'
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

    def execute_sql(self, sql, all=True):
        if DEBUG:
            start = time.time()
            print(sql)
        mycursor = GameDB.mydb.cursor()
        mycursor.execute(sql)
        if all:
            results = mycursor.fetchall()
            if DEBUG:
                end = time.time()
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
        count = 0
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
    def get_top_games(self, required_votes):
        # Get average rating
        sql = "SELECT avg(rating) FROM rating, (SELECT game.game as id, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game  HAVING votes > 5000 ORDER BY rating DESC) game_rating WHERE rating.game = game_rating.id"
        c = self.execute_sql(sql)[0][0]

        sql = "SELECT game.game, game.name, AVG(rating.rating) as rating, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game HAVING votes > "+str(required_votes)+" ORDER BY rating DESC"
        result = self.execute_sql(sql)
        game_ratings = np.array(result, dtype=object)#dtype=([('id', int), ('name', '<U256'), ('raw_rating', float), ('votes', int)]))

        # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C - from https://stats.stackexchange.com/questions/6418/rating-system-taking-account-of-number-of-votes
        weighted_rating = (game_ratings[:,3:4] / (game_ratings[:,3:4]+required_votes)) * game_ratings[:,2:3].astype(float) + (required_votes / ((game_ratings[:,3:4])+required_votes)) * float(c)
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
        sql = "SELECT user FROM (SELECT distinct rating.user as user, count(*) as num_ratings FROM rating"

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


class Recommender:
    db = GameDB()
    def __init__(self, required_votes=5000, top_users=25000, reload=False):
        # Try loading from pickle first unless asked not to
        fn_game_ids = "game_ids"
        fn_game_names = "game_names"

        if reload or not (file_exists(fn_game_ids) and file_exists(fn_game_names)):
            # load file from database
            self.game_ids, self.game_names = Recommender.db.get_top_games(required_votes)
            # pickle it for next time
            pickle_list(self.game_ids,fn_game_ids)
            pickle_list(self.game_names,fn_game_names)
        else:
            self.game_ids = get_pickled_list(fn_game_ids)
            self.game_names = get_pickled_list(fn_game_names)

        file_name = "user_ids"
        if reload or not file_exists(file_name):
            # load file from database
            self.user_ids = Recommender.db.get_users(top_users, self.game_ids)
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
            id = game[0]
            name = game[1]
            game_id_to_name[id] = name
            game_name_to_id[name] = id

        self.game_id_to_name = game_id_to_name
        self.game_name_to_id = game_name_to_id


    def pearson_correlation(self, user_id1, user_id2):
        # Get ratings for each user
        if DEBUG:
            if user_id1 not in self.user_ratings:
                print("ERROR!")
                print(self.user_ratings)

            if user_id1 not in self.user_ratings:
                print("ERROR!")
                print(self.user_ratings)

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
            return (0.0, 0)

        # Add up all ratings
        sum1 = sum([user1_ratings[id] for id in mutal_ratings])
        sum2 = sum([user2_ratings[id] for id in mutal_ratings])

        # Sum of Squares
        sum_sq_1 = sum([user1_ratings[id]**2 for id in mutal_ratings])
        sum_sq_2 = sum([user2_ratings[id]**2 for id in mutal_ratings])

        # Sum of the products
        product_sum = sum([user1_ratings[id] * user2_ratings[id] for id in mutal_ratings])

        # Calculate Pearson score
        numerator = product_sum-(sum1*sum2/mutal_count)
        denominator = math.sqrt( (sum_sq_1 - (sum1**2)/mutal_count) * (sum_sq_2 - (sum2**2)/mutal_count) )

        if denominator == 0:
            return (0.0, 0)

        return ((numerator / denominator), mutal_count)



    def top_user_matches(self, user, top=5):

        def sort_correlation(row):
            return row[1][0]

        start = time.time()
        user_ratings = self.user_ratings
        scores = [(other, self.pearson_correlation(user, other)) for other in user_ratings if user != other]

        # Sort the list
        scores.sort(key=sort_correlation, reverse=True)
        if DEBUG:
            end = time.time()
            print("Found matches in "+str(round(end-start, 2))+" seconds")

        return scores[0:top]


    def get_recommendations(self, user, top=5):
        totals = {}
        sim_sum = {}
        user_ratings = self.user_ratings
        scores = []
        users = []
        mutual = []
        for other in user_ratings:
            if other == user: continue
            sim_score, mutual_count = self.pearson_correlation(user, other)
            if sim_score <= 0.0: continue
            scores.append(sim_score)
            users.append(other)
            mutual.append(mutual_count)

        if DEBUG:
            assert len(users) == len(scores)

        # Determine values for weighted score
        max_count = max(mutual) # m
        min_count = min(max_count, 5)
        print("min votes", min_count, "max", max_count)
        avg_score = np.mean(scores) # C
        std_score = np.std(scores)
        count = sum(mutual[i] >= min_count for i in range(len(scores)))
        while count < 10 and min_count > 0:
            min_count = max(min_count - 1 , 0)
            count = sum(mutual[i] >= min_count for i in range(len(scores)))

        for i in range(len(users)):
            other = users[i]
            raw_score = scores[i] # R
            mutual_count = mutual[i] # v
            if mutual_count < min_count: continue

            # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
            if max_count > min_count * 5:
                sim_score = (mutual_count / (mutual_count+min_count)) * raw_score + (min_count / (mutual_count+min_count)) * avg_score
            else:
                sim_score = raw_score

            # Drop people too different in tastes
            if sim_score < avg_score - (std_score * 1.0): continue

            for item in user_ratings[other]:

                # Only score items I haven't yet
                if item not in user_ratings[user]:
                    # Similarity * Score
                    totals.setdefault(item,0)
                    totals[item]+=user_ratings[other][item]*sim_score
                    # sum of similarities
                    sim_sum.setdefault(item,0)
                    sim_sum[item] += sim_score

        # Create normalized list
        rankings=[( round(total/sim_sum[item],2), item, self.game_id_to_name[item]) for item, total in totals.items()]

        # Return sorted list
        rankings.sort(reverse=True)
        return rankings[0:top]


    def get_game_ratings_by_name(self, user):
        game_ratings = {}
        for game_id in self.user_ratings[user]:
            name = self.game_id_to_name[game_id]
            game_ratings[name] = self.user_ratings[user][game_id]

        return game_ratings


    def add_user(self):
        user_ratings = self.user_ratings
        id_list = [id for id in user_ratings]
        next = max(id_list)
        user_ratings[next] = {}
        return next

    def add_game(self, user_id, game, rating):
        if type(game) == int:
            self.user_ratings[user_id][game] = rating
        elif type(game) == str:
            id = self.game_name_to_id[game]
            self.user_ratings[user_id][id] = rating
        else:
            raise Exception("'game' must be integer id or string name.")


def main():
    recommender = Recommender(reload=False, top_users=50000)
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
    recommender.add_game(fantasy_id, 66356, 9)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(fantasy_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(fantasy_id, top=5)
    print(recommendations)


    # Test Set: Lovecraft
    print("")
    print("")
    print("Test Set: Lovecraft")
    lovecraft_id = recommender.add_user()
    # recommender.add_game(lovecraft_id, 205059, 10)
    recommender.add_game(lovecraft_id, 'Elder Sign', 9)
    recommender.add_game(lovecraft_id, 'Arkham Horror', 10)
    recommender.add_game(lovecraft_id, 83330, 10)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(lovecraft_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(lovecraft_id, top=5)
    print(recommendations)

    # Casual Gamer
    print("")
    print("")
    print("Test Set: Casual")
    casual_id = recommender.add_user()
    recommender.add_game(casual_id, 'Chess', 10)
    recommender.add_game(casual_id, 'Poker', 10)
    recommender.add_game(casual_id, 2397, 9)
    print("User's Games:")
    print(recommender.get_game_ratings_by_name(casual_id))
    print("Game Recommendations:")
    recommendations = recommender.get_recommendations(casual_id, top=5)
    print(recommendations)


