SELECT * FROM (SELECT distinct rating.user, count(*) as num_ratings FROM rating GROUP BY rating.user order by num_ratings DESC) rating_counts LIMIT 25000
