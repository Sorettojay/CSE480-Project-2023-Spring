1: CREATE TABLE students (name TEXT);
1: INSERT INTO students VALUES ('James');
1: BEGIN TRANSACTION;
1: SELECT * FROM students ORDER BY name;
1: COMMIT TRANSACTION;
1: SELECT * FROM students ORDER BY name;
