import mysql.connector

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


    def get_game_ratings(self, gameids):
        for gameid in gameids:

            mycursor = GameDB.mydb.cursor()

            sql = "SELECT game.name, rating.user, rating.rating FROM game, rating WHERE game.game = rating.game and game.game = " + str(gameid);
            mycursor.execute(sql)
            result = mycursor.fetchall()
            return result


def main():
    gameids = [152]
    db = GameDB()
    result=db.get_game_ratings(gameids)
    print(result)