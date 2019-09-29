import mysql.connector

# Connect to the database and query a game.
mydb = mysql.connector.connect(
    host="dallas113.arvixeshared.com",
    user="nikkala_genie",
    passwd="RepMjhaqfM9y58m",
    database="nikkala_boardgamegenie",
    auth_plugin='mysql_native_password'
)

gameids = [152]

for gameid in gameids:

    mycursor = mydb.cursor()

    sql = "SELECT game.name, rating.user, rating.rating FROM game, rating WHERE game.game = rating.game and game.game = " + str(gameid);
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print("The results for game number " + str(gameid) + "are: ")
    for x in result:
        print(x)

