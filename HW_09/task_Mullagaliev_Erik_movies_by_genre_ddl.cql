CREATE TABLE IF NOT EXISTS movies_by_genre (
    genre text,
    year int,
    movieid int,
    title text,
    PRIMARY KEY (genre, year, movieid)
 ) WITH CLUSTERING ORDER BY (year DESC);