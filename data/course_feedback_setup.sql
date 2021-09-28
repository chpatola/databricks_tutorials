CREATE TABLE COURSE_FEEDBACK (
    ID INT NOT NULL IDENTITY PRIMARY KEY,
    COURSE VARCHAR(50) NOT NULL,
    YEAR SMALLINT NOT NULL,
    MONTH SMALLINT NOT NULL,
    STUDENT SMALLINT NOT NULL,
    SETUP SMALLINT,
    MATERIAL SMALLINT,
    TEACHER SMALLINT
);

INSERT INTO COURSE_FEEDBACK
(COURSE, YEAR, MONTH, STUDENT, SETUP, MATERIAL, TEACHER) VALUES
('Calculus1',2021,09,1,8,6,9),
('Calculus1',2021,09,2,8,4,9),
('Calculus1',2021,09,3,8,6,9),
('Calculus1',2021,09,4,8,9,9),
('Calculus1',2021,09,5,8,6,10),
('Calculus1',2021,09,6,8,6,8),
('Calculus1',2021,09,7,8,6,9),
('Calculus1',2021,08,8,8,6,9),
('Calculus1',2021,08,9,8,10,9),
('Calculus1',2021,08,10,8,6,9),
('Calculus1',2021,08,11,8,9,9),
('Calculus1',2021,08,12,8,7,6),
('Calculus1',2021,08,13,8,6,8),
('Calculus1',2021,08,14,7,6,9);