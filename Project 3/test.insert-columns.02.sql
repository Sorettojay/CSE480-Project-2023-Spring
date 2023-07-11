CREATE TABLE foods (name TEXT, is_good INTEGER, cost REAL);
INSERT INTO foods VALUES ('Carrot', 1, 4.5);
INSERT INTO foods VALUES ('Curry', 1, 11.4), ('Melon', 0, 0.54);
INSERT INTO foods (is_good, cost,  name) VALUES (0, 645.0, 'Turnip'), (1, 56.2, 'Cake');
SELECT * FROM foods ORDER BY cost;
