(SELECT game.game as id, count(rating.rating) as votes 
FROM game, rating 
WHERE game.game = rating.game AND min_age < 13 AND min_playtime < 30  # OR game.name = 'Gloomhaven' OR game.game = 17226 OR game.game = 66356 
GROUP BY game.game 
ORDER BY rating DESC)
UNION DISTINCT
(SELECT game.game as id, count(rating.rating) as votes 
FROM game, rating 
WHERE game.name = 'Gloomhaven' OR game.game = 17226 OR game.game = 66356 
GROUP BY game.game 
ORDER BY rating DESC)