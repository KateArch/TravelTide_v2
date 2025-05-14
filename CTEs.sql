
WITH employee (id, pay_month, salary)
AS (VALUES
(1, 1, 20),
(2, 1, 20),
(1, 2, 30),
(2, 2, 30),
(3, 2, 40),
(1, 3, 40),
(3, 3, 60),
(1, 4, 60),
(3, 4, 70))

SELECT * FROM employee

-- rank or row_num for the pay_months for each id to exclude the latest
-- SUM (salary) for each id ordered by rank or row_num







