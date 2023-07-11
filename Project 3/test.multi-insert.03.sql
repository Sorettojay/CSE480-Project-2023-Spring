CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);
INSERT INTO student VALUES ('James', 4.0, 1), ('Yaxin', 4.0, 2), ('Joe', 1.5, 1), ('John', 3.99, 2);
INSERT INTO student (piazza, grade) VALUES (2, 3.2), (0, 0.0);
SELECT * FROM student ORDER BY piazza, grade;
