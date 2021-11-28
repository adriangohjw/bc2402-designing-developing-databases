-----------------
-- 1) What is the total population in Asia?
-----------------

SELECT 
    SUM(population) AS total_population
FROM
    location_indicators
        LEFT JOIN
    location ON location_indicators.location = location.location
WHERE
    location.continent = 'Asia';


-----------------
-- 2) What is the total population among the ten ASEAN countries?
-----------------

 SELECT 
        SUM(population) 
    FROM 
        location_indicators
    WHERE 
        location IN ('Brunei', 'Myanmar' , 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Philippines', 'Singapore', 'Thailand' , 'Vietnam');


-----------------
-- 3) Generate a list of unique data sources (source_name)
-----------------

SELECT
    DISTINCT `source_name`
FROM
    `country_vaccinations`;
    
    
-----------------
-- 4) Specific to Singapore, display the daily total_vaccinations 
-- starting (inclusive) March-1 2021 through (inclusive) May-31 2021
-----------------

select 
	`date`,`total_vaccinations` 
from 
	(select * from `covid_vaccinations` where `location` = "Singapore") as x 
where 
	`date` between '2021-03-01' and '2021-05-31';
    
    
-----------------
-- 5) When is the first batch of vaccinations recorded in Singapore?
-----------------

SELECT 
    MIN(date)
FROM
    covid_vaccinations
WHERE
    location = 'Singapore'
        AND total_vaccinations <> '';


-----------------
-- 6) Based on the date identified in (5), specific to Singapore, 
-- compute the total number of new cases thereafter. For instance, 
-- if the date identified in (5) is Jan-1 2021, the total number of new cases will be 
-- the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.
-----------------

SELECT 
    SUM(new_cases) 
FROM  
    covid_cases
WHERE 
    location = 'Singapore' 
    AND date >= (SELECT MIN(date) FROM covid_vaccinations
                WHERE
                    location = 'Singapore'
                    AND total_vaccinations IS NOT NULL
                    AND total_vaccinations <> '');
                    

-----------------
-- 7) Compute the total number of new cases in Singapore before the date identified in (5).
-- For instance, if the date identified in (5) is Jan-1 2021 and the first date recorded (in Singapore) 
-- in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases starting from (inclusive) 
-- Feb-1 2020 through (inclusive) Dec- 31 2020.
-----------------

SELECT
    SUM(`new_cases`) AS `total_cases_before_vaccine`
FROM
    `covid_cases`
WHERE 
    `location` = "Singapore"
    AND `date` < (SELECT MIN(`date`) 
				  FROM `covid_vaccinations`
                  WHERE `location` = 'Singapore'
				  AND `total_vaccinations` > 0);


-----------------
-- 8) Herd immunity estimation. On a daily basis, specific to Germany, calculate 
--    the percentage of new cases (i.e., percentage of new cases = new cases / populations)
--    and total vaccinations on each available vaccine in relation to its population. 
--    (Total of each type of vaccination as a percentage of the population)
-----------------

SELECT 
	`date`,`population`,`new_cases`, (`new_cases`/`population` * 100) as `% new cases of population`,`vaccine`,(`total_vaccinations`/`population` * 100) as `% vaccines`
FROM
	(SELECT 
		`y`.`date`,`y`.`new_cases`,`z`.`population`,`x`.`vaccine`,`x`.`total_vaccinations` 
	FROM 
		(SELECT * FROM `covid_cases` WHERE `location` = "Germany") y
	LEFT JOIN  
		(SELECT * FROM country_vaccinations_by_manufacturer WHERE location = "Germany") x 
        ON `x`.`date` = `y`.`date`
	LEFT JOIN
		(SELECT `location`, `population` FROM `location_indicators` WHERE `location` = "Germany") z
        ON `y`.`location` = `z`.`location`
	) a;


-----------------
-- 9) Vaccination Drivers. Specific to Germany, based on each daily new case, 
-- display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.
-----------------

WITH daily_new_cases AS (
	SELECT 
		location, date, new_cases
	FROM
		covid_cases
	WHERE
		location = 'Germany'
), vaccinations_by_manufacturer AS (
	SELECT 
		date, vaccine, total_vaccinations
	FROM
		country_vaccinations_by_manufacturer
	WHERE
		location = 'Germany'
), distinct_vaccines AS (
	SELECT DISTINCT
		vaccine
	FROM
		vaccinations_by_manufacturer
)

SELECT 
    daily_new_cases_with_vaccines.*,
    VBM_20.date,
    VBM_20.total_vaccinations,
    VBM_30.date,
    VBM_30.total_vaccinations,
    VBM_40.date,
    VBM_40.total_vaccinations
FROM
    (SELECT 
        *
    FROM
        daily_new_cases, distinct_vaccines) daily_new_cases_with_vaccines
        LEFT JOIN
    vaccinations_by_manufacturer VBM_20 ON DATE_ADD(daily_new_cases_with_vaccines.date,
        INTERVAL 20 DAY) = VBM_20.date
        AND daily_new_cases_with_vaccines.vaccine = VBM_20.vaccine
        LEFT JOIN
    vaccinations_by_manufacturer VBM_30 ON DATE_ADD(daily_new_cases_with_vaccines.date,
        INTERVAL 30 DAY) = VBM_30.date
        AND daily_new_cases_with_vaccines.vaccine = VBM_30.vaccine
        LEFT JOIN
    vaccinations_by_manufacturer VBM_40 ON DATE_ADD(daily_new_cases_with_vaccines.date,
        INTERVAL 40 DAY) = VBM_40.date
        AND daily_new_cases_with_vaccines.vaccine = VBM_40.vaccine
ORDER BY daily_new_cases_with_vaccines.date ASC , daily_new_cases_with_vaccines.vaccine ASC;


-----------------
-- 10) Vaccination Effects. Specific to Germany, on a daily basis, 
-- based on the total number of accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), 
-- generate the daily new cases after 21 days, 60 days, and 120 days.
-----------------

SELECT 
    T1.date,
	total_accumulated_vaccinations,
    T2.new_cases AS daily_cases_after_21_days,
    T3.new_cases AS daily_cases_after_60_days,
    T4.new_cases AS daily_cases_after_120_days
FROM
    (SELECT 
        location, date,  SUM(total_vaccinations) AS total_accumulated_vaccinations 
    FROM 
        country_vaccinations_by_manufacturer
    WHERE 
        location = 'Germany'
    GROUP BY 
        location, date) T1
INNER JOIN 
    (SELECT 
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T2
ON T2.date = date_add(T1.date, interval 21 day)
LEFT JOIN
    (SELECT 
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T3
ON T3.date = date_add(T1.date, interval 60 day)
LEFT JOIN
    (SELECT 
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T4
ON T4.date = date_add(T1.date, interval 120 day);