SELECT COUNT(*) FROM movies_with_genre_index
WHERE genres CONTAINS 'Horror' and year > 1979 and year < 1991
ALLOW FILTERING;