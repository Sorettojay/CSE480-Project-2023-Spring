CREATE TABLE student_123 (name TEXT, grade REAL, piazza INTEGER);
INSERT INTO student_123 VALUES ('James', 4.0, 1);
INSERT INTO student_123 VALUES ('Yaxin', 4.0, 2);
INSERT INTO student_123 VALUES ('Li', 3.2, 2);
SELECT * FROM student_123 ORDER BY piazza, grade;
