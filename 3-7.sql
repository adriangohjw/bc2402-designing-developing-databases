#3
SELECT DISTINCT `source_name`
FROM `country_vaccinations`;

#5
SELECT MIN(`date`) AS `first_recorded_date`
FROM `country_vaccinations`
WHERE `country` = "Singapore" AND `total_vaccinations`>0; #1/11/2021 mm/dd/yyyy

#7
SELECT SUM(`new_cases`) AS `total_cases_before_vaccine`
FROM `covid19data`
WHERE `iso_code` = "SGP" AND `date`<"2021-01-11";
/*SELECT `total_cases` AS `total_cases_before_vaccine`
FROM `covid19data`
WHERE `iso_code` = "SGP" AND `date`="2021-01-10";*/