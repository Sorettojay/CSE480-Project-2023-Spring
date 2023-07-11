CREATE TABLE coolstudent ( isaname TEXT,  grade  REAL, piazza INTEGER);
INSERT INTO coolstudent VALUES ('James', 4.0, 1);
INSERT INTO coolstudent VALUES ('Yaxin', 4.0, 2);
INSERT INTO coolstudent VALUES ('Li', 3.2, 2);
SELECT * FROM coolstudent ORDER BY piazza,  grade ;
