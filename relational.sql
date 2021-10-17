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
