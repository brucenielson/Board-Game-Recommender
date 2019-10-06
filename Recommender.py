import mysql.connector
import sys
import numpy as np


DEBUG = True

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
            print(sql)
        mycursor = GameDB.mydb.cursor()
        mycursor.execute(sql)
        if all:
            result = mycursor.fetchall()
            return result
        else:
            return mycursor


    def get_game_ratings(self, gameids):
        for gameid in gameids:
            sql = "SELECT game.name, rating.user, rating.rating FROM game, rating WHERE game.game = rating.game and game.game = " + str(gameid);
            return self.execute_sql(sql)


    # Gets the top games that have at least some number of votes (required_votes) and returns them ordered by weighted rating
    def get_top_games(self, required_votes):
        # Get average rating
        sql = "SELECT avg(rating) FROM rating, (SELECT game.game as id, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game  HAVING votes > 5000 ORDER BY rating DESC) game_rating WHERE rating.game = game_rating.id"
        c = self.execute_sql(sql)[0][0]

        sql = "SELECT game.game, game.name, AVG(rating.rating) as rating, count(rating.rating) as votes FROM game, rating WHERE game.game = rating.game GROUP BY game.game HAVING votes > "+str(required_votes)+" ORDER BY rating DESC"
        result = self.execute_sql(sql)
        print(result)
        game_ratings = np.array(result, dtype=object)#dtype=([('id', int), ('name', '<U256'), ('raw_rating', float), ('votes', int)]))

        # (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
        weighted_rating = (game_ratings[:,3:4] / (game_ratings[:,3:4]+required_votes)) * game_ratings[:,2:3].astype(float) + (required_votes / ((game_ratings[:,3:4])+required_votes)) * float(c)
        game_ratings = np.hstack((game_ratings, weighted_rating))
        # Sort by weighted rating. Python, lamely, can't do a sort in numpy easily. But this trick worked: https://stackoverflow.com/questions/16486252/is-it-possible-to-use-argsort-in-descending-order
        game_ratings = game_ratings[(-game_ratings[:, 4]).argsort()]
        return game_ratings



    def get_all_game_ratings(self):
        sql = "SELECT game.game, game.name, rating.rating FROM game, rating WHERE game.game = rating.game ORDER BY game.game, rating.rating DESC LIMIT 1000"
        return self.execute_sql(sql, all=False)

    def get_all_games(self):
        sql = "SELECT game.game, game.name FROM game ORDER BY game.game DESC"
        return self.execute_sql(sql)

    def get_all_ratings(self):
        sql = "SELECT game, user, rating FROM rating ORDER BY game LIMIT 1000000"
        return self.execute_sql(sql)


def main():
    gameids = [152]
    db = GameDB()
    # result=db.get_game_ratings(gameids)
    # print(result)
    top_games = db.get_top_games(5000)
    print(len(top_games))
    print(top_games)
    # cursor = db.get_all_game_ratings()
    # row = cursor.fetchone()
    # while row is not None:
    #     print(row)
    #     row = cursor.fetchone()
    # print(result)

    # result = db.get_all_games()
    # print(len(result))
    # print(result)
    #
    # result = db.get_all_ratings()
    # print(sys.getsizeof(result))
    # print(len(result))

