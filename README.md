# Board-Game-Recommender
Board game recommender system

To use this system, you must use Recommender.py. 

The main() function gives several examples how to use this class correctly. But here are the basics:

1. Instantiate the the recommender: 
recommender = Recommender(reload=False, top_games=2000, num_users=150000)
This uses all 2000 top games in the database and grabs 150,000 users and their ratings. This will then collect the data.
If pickle files are available, it will use those (unless reload = True). Otherwise it will query the database and get fresh data. 
Consider setting up a service that reloads fresh from the database once per day at night.

2. Create a user and stick it into the dataset and then add games and ratings. Note: this does NOT add it to the database. In real life, 
you may want to create the user in the database so that in the future the user is already there. 
fantasy_id = recommender.add_user()
recommender.add_game(fantasy_id, 'Gloomhaven', 10)
recommender.add_game(fantasy_id, 17226, 10)
recommender.add_game(fantasy_id, 66356, 10)

Note: you can add_game using either a game name or game_id

3. Finally, get recommendations for this new user:
recommendations = recommender.get_recommendations(fantasy_id, top=10)
What is returned is a list of lists. Each entry in the main list is a list with the following data:
Rating of this game, game_id, game_name, number of votes for this game (used for debugging primarily.)

That's it. 

Good luck with your site, ladies! I'm really looking forward to using it myself. And I hope to eventually come back and
improve the recommendation system.

Side note: this is a public git and the password to your database is available. We need to fix that. We could move this code over to
BitBucket where it can be made private (though I'm not sure how to use BitBucket with github desktop.)

Or better yet, maybe we need to remove the password (and change yours) and then modify my code to accept a password.

Also, the directory venv includes a python virtual environment. You may want to remove that and user your own.

Oh, one more thing. test_recommender is a unit test. It's a bad unit test. It just checks to make sure my test sets are 
still getting the results I expected. That way you can modify my code without breaking anything. However, any attempt to 
actually improve the recommender will necessarily break the unit test, which makes it not a very good unit test. But I needed
it for refactoring my code without breaking things. 