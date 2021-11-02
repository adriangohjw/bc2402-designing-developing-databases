-----------------
-- 1) What is the total population in Asia?
-----------------

WITH location_latest_date AS (
    SELECT 
        location, MAX(date) as max_date
    FROM
        covid19data
    WHERE
        continent = 'Asia'
    GROUP BY location
)

SELECT 
    *
FROM
    covid19data
        LEFT JOIN
    location_latest_date ON covid19data.location = location_latest_date.location
        AND covid19data.date = location_latest_date.max_date
WHERE
    location_latest_date.max_date IS NOT NULL;


-----------------
-- 2) What is the total population among the ten ASEAN countries?
-----------------

SELECT 
    SUM(population) as total_population_among_ASEAN_countries 
FROM(
    SELECT 
        population 
    FROM 
        covid19data
    WHERE 
        location IN ('Brunei', 'Myanmar' , 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Philippines', 'Singapore', 'Thailand' , 'Vietnam')
    GROUP BY location) T;


-----------------
-- 3) Generate a list of unique data sources (source_name)
-----------------

SELECT
    DISTINCT `source_name`
FROM
    `country_vaccinations`;


-----------------
-- 5) When is the first batch of vaccinations recorded in Singapore?
-----------------

-- Solution 1: Using country_vaccinations table
-- NOTE: Result is in MM/DD/YYYY instead of DD/MM/YYYY

SELECT 
    MIN(date)
FROM
    country_vaccinations
WHERE
    country = 'Singapore'
        AND total_vaccinations <> 0;

-- Solution 2: Using covid19data table

SELECT 
    MIN(date)
FROM
    covid19data
WHERE
    location = 'Singapore'
        AND total_vaccinations IS NOT NULL
        AND total_vaccinations <> ''


-----------------
-- 6) Based on the date identified in (5), specific to Singapore, 
-- compute the total number of new cases thereafter. For instance, 
-- if the date identified in (5) is Jan-1 2021, the total number of new cases will be 
-- the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.
-----------------

SELECT 
    SUM(new_cases) 
FROM  
    covid19data
WHERE 
    location = 'Singapore' 
    AND date >= (SELECT MIN(date) FROM covid19data
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
    `covid19data`
WHERE 
    `iso_code` = "SGP"
    AND `date`<"2021-01-11";


-----------------
-- 9) Vaccination Drivers. Specific to Germany, based on each daily new case, 
-- display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.
-----------------

WITH germany_vaccinations_by_manufacturer AS (
	SELECT 
		vaccine, date, total_vaccinations
	FROM
		country_vaccinations_by_manufacturer
	WHERE
		location = 'Germany'
        AND total_vaccinations <> 0
), germany_first_vaccine_date_by_manufacturer AS (
	SELECT 
		vaccine, MIN(date) AS date
	FROM
		germany_vaccinations_by_manufacturer
	GROUP BY vaccine
)

SELECT 
    vaccine_data.vaccine,
    vaccine_data.date,
    vaccine_data.total_vaccinations
FROM
    germany_vaccinations_by_manufacturer vaccine_data
        LEFT JOIN
    germany_first_vaccine_date_by_manufacturer first_vaccine_date ON vaccine_data.vaccine = first_vaccine_date.vaccine
WHERE
	vaccine_data.date IN (
		DATE_ADD(first_vaccine_date.date, INTERVAL 20 DAY),
    DATE_ADD(first_vaccine_date.date, INTERVAL 30 DAY),
    DATE_ADD(first_vaccine_date.date, INTERVAL 40 DAY)
	)
ORDER BY 
	vaccine_data.vaccine,
    vaccine_data.date;


-----------------
-- 10) Vaccination Effects. Specific to Germany, on a daily basis, 
-- based on the total number of accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), 
-- generate the daily new cases after 21 days, 60 days, and 120 days.
-----------------

SELECT 
    T1.date,
	total_accumulated_vaccinations,
    T2.new_cases AS daily_cases_after_21_days,
    T3.new_cases AS daily_cases_after_60_day,
    T4.new_cases AS daily_cases_after_120_day
FROM
    (SELECT 
        location, date,  SUM(total_vaccinations) AS total_accumulated_vaccinations 
    FROM 
        country_vaccinations_by_manufacturer
    WHERE 
        location = 'Germany'
    GROUP BY 
        location, date) T1
JOIN 
    (SELECT 
        date, new_cases FROM covid19data
    WHERE 
        location = 'Germany') T2
ON T2.date = date_add(T1.date, interval 21 day)
JOIN
    (SELECT 
        date, new_cases FROM covid19data
    WHERE 
        location = 'Germany') T3
ON T3.date = date_add(T1.date, interval 60 day)
JOIN
    (SELECT 
        date, new_cases FROM covid19data
    WHERE 
        location = 'Germany') T4
ON T4.date = date_add(T1.date, interval 120 day);