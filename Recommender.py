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

        # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
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

        sql = sql + " GROUP BY rating.user ORDER BY num_ratings DESC, rating.user ASC) rating_counts"

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
        for other in user_ratings:
            if other == user: continue
            sim_score = self.pearson_correlation(user, other)[0]

            if sim_score == 0: continue
            for item in user_ratings[other]:

                # Only score items I haven't yet
                if item not in user_ratings[user] or user_ratings[user][item] == 0:
                    # Similarity * Score
                    totals.setdefault(item,0)
                    totals[item]+=user_ratings[other][item]*sim_score
                    # sum of similarities
                    sim_sum.setdefault(item,0)
                    sim_sum[item] += sim_score

            # Create normalized list
            rankings=[(total/sim_sum[item],item) for item, total in totals.items()]

            # Return sorted list
            rankings.sort(reverse=True)
            return rankings[0:top]


def main():
    recommender = Recommender(reload=False)
    user_matches = recommender.top_user_matches(14791, top=20)
    print(user_matches)
    recommendations = recommender.get_recommendations(14791)
    print(recommendations)




