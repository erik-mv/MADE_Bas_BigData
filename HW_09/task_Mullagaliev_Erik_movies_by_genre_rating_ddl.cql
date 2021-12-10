CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
    genre text,
    year int,
    movieid int,
    title text,
    rating float,
    PRIMARY KEY (genre, year, rating, movieid)
 ) WITH CLUSTERING ORDER BY (year ASC, rating DESC);
