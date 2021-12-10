SELECT MIN(rating), AVG(rating), MAX(rating) FROM movies_by_genre_rating
WHERE genre='Sci-Fi' and year > 1999 and year < 2100;