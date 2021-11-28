-- CREATE location TABLE
CREATE TABLE location
AS (SELECT 
		location, 
		iso_code, 
		continent
    FROM 
		covid19data
    GROUP BY 
		location);

-- Create TABLE location_indicators
CREATE TABLE location_indicators 
AS (SELECT 
		location, 
		population, 
		population_density, 
		median_age, aged_65_older, 
		aged_70_older, 
		gdp_per_capita, 
		extreme_poverty, 
		cardiovasc_death_rate, 
		diabetes_prevalence, 
		female_smokers, 
		male_smokers, 
		handwashing_facilities, 
		hospital_beds_per_thousand, 
		life_expectancy, 
		human_development_index
    FROM covid19data
    WHERE 
		(population, 
		population_density, 
		median_age, 
		aged_65_older, 
		aged_70_older, 
		gdp_per_capita, 
		extreme_poverty, 
		cardiovasc_death_rate, 
		diabetes_prevalence, 
		female_smokers, 
		male_smokers, 
    	handwashing_facilities, 
		hospital_beds_per_thousand, 
		life_expectancy, 
		human_development_index) <> ('','','','','','','','','','','','','','','')
    GROUP BY 
		location);

-- CREATE covid_cases TABLE
CREATE TABLE covid_cases
AS (SELECT 
		location, 
		date, 
		total_cases, 
		new_cases, 
		new_cases_smoothed, 
		total_cases_per_million, 
		new_cases_per_million, 
		new_cases_smoothed_per_million, 
		reproduction_rate
	FROM covid19data
    WHERE 
		(total_cases, 
		new_cases, 
		new_cases_smoothed, 
		total_cases_per_million, 
		new_cases_per_million, 
		new_cases_smoothed_per_million, 
		reproduction_rate) <> ('','','','','','','')
	GROUP BY 
		location, date);
    

-- CREATE covid_deaths TABLE
CREATE TABLE covid_deaths
AS (SELECT 
		location, 
		date, 
		total_deaths, 
		new_deaths, 
		new_deaths_smoothed, 
		total_deaths_per_million, 
		new_deaths_per_million, 
		new_deaths_smoothed_per_million, 
		excess_mortality
	FROM covid19data
    WHERE 
		(total_deaths, 
		new_deaths, 
		new_deaths_smoothed, 
		total_deaths_per_million, 
		new_deaths_per_million, 
		new_deaths_smoothed_per_million, 
		excess_mortality) <> ('','','','','','','')
    GROUP BY 
		location, 
		date);

-- CREATE covid_hospitalisation TABLE
CREATE TABLE covid_hospitalisation
AS (SELECT 
		location, 
		date, 
		icu_patients, 
		icu_patients_per_million, 
		hosp_patients, 
		hosp_patients_per_million, 
		weekly_icu_admissions, 
		weekly_icu_admissions_per_million, 
		weekly_hosp_admissions, 
		weekly_hosp_admissions_per_million
	FROM covid19data
	WHERE 
		(icu_patients, 
		icu_patients_per_million, 
		hosp_patients, 
		hosp_patients_per_million, 
		weekly_icu_admissions,
		weekly_icu_admissions_per_million, 
		weekly_hosp_admissions, 
		weekly_hosp_admissions_per_million) <> ('','','','','','','','')
	GROUP BY 
		location, 
		date);

-- CREATE covid_tests TABLE
CREATE TABLE covid_tests
AS (SELECT 
		location, 
		date, 
		new_tests, 
		total_tests, 
		total_tests_per_thousand, 
		new_tests_per_thousand, 
		new_tests_smoothed, 
		new_tests_smoothed_per_thousand, 
		positive_rate, 
		tests_per_case, 
		tests_units
    FROM covid19data
    WHERE 
		(new_tests, 
		total_tests, 
		total_tests_per_thousand, 
		new_tests_per_thousand, 
		new_tests_smoothed, 
		new_tests_smoothed_per_thousand,
		positive_rate, 
		tests_per_case, 
		tests_units) <> ('','','','','','','','','')
    GROUP BY 
		location, 
		date);

-- CREATE covid_vaccinations TABLE
CREATE TABLE covid_vaccinations 
AS (SELECT 
	location,
    date,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    new_vaccinations,
    new_vaccinations_smoothed,
    total_vaccinations_per_hundred,
    people_vaccinated_per_hundred,
    people_fully_vaccinated_per_hundred,
    new_vaccinations_smoothed_per_million FROM
    covid19data
	WHERE
		(total_vaccinations , 
		people_vaccinated,
        people_fully_vaccinated,
        new_vaccinations,
        new_vaccinations_smoothed,
        total_vaccinations_per_hundred,
        people_vaccinated_per_hundred,
        people_fully_vaccinated_per_hundred,
        new_vaccinations_smoothed_per_million) <> ('' , '', '', '', '', '', '', '', '')
	GROUP BY 
		location , 
		date);
    
-- CREATE covid_measures TABLE
CREATE TABLE covid_measures AS (SELECT location, date, stringency_index FROM
    covid19data
WHERE
    stringency_index <> ''
GROUP BY location , date);
