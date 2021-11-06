-----------------
-- 2) What is the total population among the ten ASEAN countries?
-----------------

SELECT 
    SUM(population) as total_population_among_ASEAN_countries 
FROM(
    SELECT 
        population 
    FROM 
        location_indicators
    WHERE 
        location IN ('Brunei', 'Myanmar' , 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Philippines', 'Singapore', 'Thailand' , 'Vietnam')
    GROUP BY location) T;
 
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
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T2
ON T2.date = date_add(T1.date, interval 21 day)
JOIN
    (SELECT 
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T3
ON T3.date = date_add(T1.date, interval 60 day)
JOIN
    (SELECT 
        date, new_cases FROM covid_cases
    WHERE 
        location = 'Germany') T4
ON T4.date = date_add(T1.date, interval 120 day);