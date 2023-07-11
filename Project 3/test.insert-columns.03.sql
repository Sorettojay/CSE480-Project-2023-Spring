CREATE TABLE foods (name TEXT, is_good INTEGER, cost REAL);
INSERT INTO foods VALUES ('Carrot', 1, 4.5);
INSERT INTO foods VALUES ('Curry', 1, 11.4), ('Melon', 0, 0.54);
INSERT INTO foods (name, cost) VALUES ('Turnip', 645.0), ('Cake', 56.2);
SELECT * FROM foods ORDER BY  cost;
