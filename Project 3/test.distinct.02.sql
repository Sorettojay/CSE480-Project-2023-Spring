CREATE TABLE movies (name TEXT, genre TEXT, rating REAL);
INSERT INTO movies VALUES ('The Matrix', 'action', 5.0);
INSERT INTO movies VALUES ('James Bond', 'action', 6.0);
INSERT INTO movies VALUES ('The Conjuring', 'children', 0.0);
INSERT INTO movies VALUES ('La La Land', 'romance', 3.2);
INSERT INTO movies VALUES ('The Matrix', 'action', 7.8);
INSERT INTO movies VALUES ('James Bond', 'action', 9.4);
SELECT DISTINCT genre FROM movies ORDER BY genre;
