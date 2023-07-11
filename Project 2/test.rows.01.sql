CREATE TABLE student (name TEXT, grade REAL);
INSERT INTO student VALUES ('James', 1.5);
INSERT INTO student VALUES ('Yaxin', 4.0);
INSERT INTO student VALUES ('Li', 3.2);
SELECT name, grade FROM student ORDER BY name;
