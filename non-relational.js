// q1. Display a list of total vaccinations per day in Singapore.
// [source table: country_vaccinations]

db.country_vaccinations.aggregate([{
    $match: {
        country: "Singapore"
    }
}, {
    $project: {
        date: 1,
        daily_vaccinations: 1
    }
}])

// q2. Display the sum of daily vaccinations among ASEAN countries.
// [source table: country_vaccinations]
db.country_vaccinations.aggregate(
   [
        {$match :
               {
                   country : {$in : ["Brunei", "Burma", "Cambodia", "Timor", "Indonesia", "Laos", "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam", "Myanmar"]}
               }
        },
        {$group:
             {
               _id: { "country" : "$country" },
               "sum": {$sum: {$convert:{input: "$daily_vaccinations", to: "int"}}}
             }
        }
   ]
);

// q3. Identify the maximum daily vaccinations per million of each country. 
// Sort the list based on daily vaccinations per million in a descending order.
// [source table: country_vaccinations]

db.country_vaccinations.aggregate([
    { $group: { _id: "$country", "max_daily_vaccinations_per_million": { $max: { $convert: { input: "$daily_vaccinations_per_million", to: "int" } } } } },
    { $sort: { max_daily_vaccinations_per_million: -1 } }
])


// q4. Which is the most administrated vaccine? Display a list of total administration 
// (i.e., sum of total vaccinations) per vaccine.
// [source table: country_vaccinations_by_manufacturer]

db.country_vaccinations_by_manufacturer.aggregate([
    {$group: {_id:{groupByVaccine: "$vaccine"}, total_administrated:{$max: {$convert:{input: "$total_vaccinations", to: "double"}}}}},
    {$project: {_id:1 ,total_administrated:1}},
    {$sort: {total_administrated : -1}}
])


// q5: Italy has commenced administrating various vaccines to its populations as a vaccine becomes available.
// Identify the first dates of each vaccine being administrated, 
// then compute the difference in days between the earliest date and the 4th date.
// [source table: country_vaccinations_by_manufacturer]

// Identify the first dates of each vaccine being administrated
db.country_vaccinations_by_manufacturer.aggregate([{
    $match: {
        location: "Italy",
        total_vaccinations: {
            $ne: 0
        }
    }
}, {
    $group: {
        _id: {
            vaccine: "$vaccine"
        },
        first_date: {
            $min: "$date"
        }
    }
}])

// compute the difference in days between the earliest date and the 4th date.
db.country_vaccinations_by_manufacturer.aggregate([{
    $match: {
        location: "Italy",
        total_vaccinations: {
            $ne: 0
        }
    }
}, {
    $group: {
        _id: {
            vaccine: "$vaccine"
        },
        first_date: {
            $min: "$date"
        }
    }
}, {
    $group: {
        _id: null,
        min_date: {
            $min: "$first_date"
        },
        max_date: {
            $max: "$first_date"
        }
    }
}, {
    $project: {
        daysDifference: {
            $dateDiff: {
                startDate: {
                    $dateFromString: {
                        dateString: "$min_date"
                    }
                },
                endDate: {
                    $dateFromString: {
                        dateString: "$max_date"
                    }
                },
                unit: "day"
            }
        }
    }
}])


// q6. What is the country with the most types of administrated vaccine?
// [source table: country_vaccinations_by_manufacturer]
db.country_vaccinations_by_manufacturer.aggregate(
        [
            {$group:
                {
                _id: {"country":"$location"},
                "Vaccines": {$addToSet:"$vaccine"},
                }
            },
            {$project:
                    {"_id":1,"Vaccines":1,count : {$size:"$Vaccines"}}
            },
            {$sort:{count:-1}}
        ]
    );

// q7. What are the countries that have fully vaccinated more than 60% of its people?
// For each country, display the vaccines administrated.
// [source table: country_vaccinations]

db.country_vaccinations.aggregate([
    { $project: { country: 1, vaccines: 1, people_vaccinated_per_hundred: { $convert: { input: "$people_vaccinated_per_hundred", to: "double" } } } },
    { $match: { people_vaccinated_per_hundred: { $gt: 60 } } },
    { $group: {_id: [ { country: "$country" }, { vaccines: "$vaccines" } ], max_people_vaccinated_per_hundred: { $max:"$people_vaccinated_per_hundred" } } },
    { $project: { _id:0, country: { $arrayElemAt: ["$_id.country", 0] }, vaccines: { $arrayElemAt: ["$_id.vaccines",0] }, max_people_vaccinated_per_hundred: 1 } }
])


// q8. Monthly vaccination insight – display the monthly total vaccination amount of each 
// vaccine per month in the United States.
// [source table: country_vaccinations_by_manufacturer

db.country_vaccinations_by_manufacturer.aggregate([
    {$match: {location: "United States"}}, 
    {$group: {_id:[{location :"$location"},{ vaccine :"$vaccine"},{month: {$month: {$convert: {input:"$date", to: "date"}}}}] , 
    monthly_total_vaccination: {$max: {$convert:{input: "$total_vaccinations", to: "double"}}}}},
    {$sort: {"_id.2":1, "_id.1":1}}
])


// q9: Days to 50 percent. Compute the number of days (i.e., using the first available date on records of a country) 
// that each country takes to go above the 50% threshold of vaccination administration (i.e., total_vaccinations_per_hundred > 50)
// [source table: country_vaccinations]

db.country_vaccinations.aggregate([{
    $group : {
        _id: {
            country: "$country"
        },
        first_date: {
            $min: {
                $convert: {
                    input: "$date",
                    to: "date"
                }
            }
        }
    }
}, {
    $project: {
        country: "$_id.country",
        first_date: "$first_date"
    }
}, {
    $lookup: {
       from: "country_vaccinations",
       localField: "country",
       foreignField: "country",
       pipeline: [
            {
               $match: {
                    $expr: {
                        $gt: [{$toDouble: "$total_vaccinations_per_hundred"}, 50]
                    }
                }
            },
            {
                $sort: {
                    date: 1
                }
            },
       ],
       as: "country_vaccinations"
    }
}, {
    $project: {
        country: "_id.country",
        days_to_50th_percent_of_population_vaccinated: {
            $dateDiff: {
                startDate: "$first_date",
                endDate: {
                    $convert: {
                        input: {
                            $arrayElemAt: ["$country_vaccinations.date", 0]
                        },
                        to: "date"
                    }
                },
                unit: "day"
            }
        }
    }
}])

// q10. Compute the global total of vaccinations per vaccine.
// [source table: country_vaccinations_by_manufacturer]
db.country_vaccinations_by_manufacturer.aggregate(
    [
        {$group:
        { _id:{"Vaccine":"$vaccine"},
            count:{$max: {$convert:{input: "$total_vaccinations", to: "double"}}}
        }
        }
        ]
    )

// q11. What is the total population in Asia?

db.covid19data.aggregate([
    { $project: { continent: 1, location: 1, population: { $convert: { input: "$population", to:"double" } } } },
    { $match: { continent: { $eq: "Asia" } } },
    { $group: { _id: "$location", max_pop: { $max: "$population" } } },
    { $group: { _id: "asiaPopulation", asiaPopulation: { $sum: "$max_pop" } } },
    { $project: {_id: 0, asiaPopulation: 1 } }
])


// q12. What is the total population among the ten ASEAN countries?

db.covid19data.aggregate([
    {$match: {location: {$in: ['Brunei', 'Myanmar' , 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Philippines', 'Singapore', 'Thailand' , 'Vietnam']}}},
    {$group: {_id: {groupByLocation: "$location"}, population: {$avg: {$convert: {input: "$population", to: "double"}}}}},
    {$group: {_id: '', total_population: {$sum: "$population"}}},
    {$project: {_id:1, total_population: 1}}
])


// q13: Generate a list of unique data sources (source_name)

db.country_vaccinations.aggregate([
    {
        $group: {
            _id: {
                source_name: "$source_name",
            }
        }
    },
])

// q14: Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021
db.country_vaccinations.aggregate(
    [
        {$project:{"country":1,"total_vaccinations":1,date:{$convert:{input: "$date",to:"date"}}}},
        {$match:
        {$and:[{"country":"Singapore"}, 
                {"date":{$gte:ISODate("2021-03-01T08:00:00.000+08:00")   }}, 
                {"date":{$lte:ISODate("2021-05-31T08:00:00.000+08:00")}}]}
            
        }
        ]
    )

// q15. When is the first batch of vaccinations recorded in Singapore?

db.country_vaccinations.aggregate([
    { $match: { country: "Singapore" } },
    { $group: { _id: "first_date", first_date: { $min: "$date" } } },
    { $project: { _id: 0, first_date: 1 } }
])


// q16. Based on the date identified in (5), specific to Singapore, 
// compute the total number of new cases thereafter. 
// For instance, if the date identified in (5) is Jan-1 2021, the total number of new cases will 
// be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.

db.covid19data.aggregate([
    {$match: {location: "Singapore"}},
    {$project: {location:1, date: {$convert: {input: "$date", to: "date"}}, new_cases: {$convert: {input: "$new_cases", to: "double"}}}},
    {
        $lookup: {
            from: "country_vaccinations",
            let: {
                covid_country: "$location",
                covid_date: "$date"
            },
            pipeline: [
                {$match: {country: "Singapore"}},
                {$group: {_id:null, min_date: {$min: {$convert: {input:"$date", to: "date"}}}}},
                {
                    $project: {
                    country:1, min_date:1
                    }
                },
                {
                    $match:{
                        $expr: {$lte: ["$min_date", "$$covid_date"]}}
                },
        ],
        as: "after_vaccination"
        }
    },
    {$match: {$expr: {$ne:["$after_vaccination",[]]}}},
    {$group: {_id: '', sum_new_cases: {$sum: "$new_cases"}}},
    {$project: {_id:0, sum_new_cases:1}}
])


// q17: Compute the total number of new cases in Singapore before the date identified in (15).
// For instance, if the date identified in (15) is Jan-1 2021 and the first date recorded (in Singapore)
// in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases
// starting from (inclusive) Feb-1 2020 through (inclusive) Dec-31 2020

db.covid19data.aggregate([{
    $match: {
        location: "Singapore"
    }
}, {
    $lookup: {
        from: "country_vaccinations",
        pipeline: [
            { $match: { country: "Singapore" } },
            { $group: { _id: "first_date", first_date: { $min: "$date" } } },
            { $project: { _id: 0, first_date: 1 } }
        ],
        as: "sgFirstVaccineBatch"
    }
}, {
    $unwind: "$sgFirstVaccineBatch"
}, {
    $match: {
        $expr: {
            $lt: [
                {
                    $dateFromString: {
                        dateString: "$date"
                    }
                },
                {
                    $convert: {
                        input: "$sgFirstVaccineBatch.first_date",
                        to: "date"
                    }
                }
            ]
        }
    }
}, {
    $group: {
        _id: "$location",
        totalNewCases: {
            $sum: {
                $convert: {
                    input: "$new_cases",
                    to: "double"
                }
            }
        }
    } 
}])

// q18. Herd immunity estimation. On a daily basis, specific to Germany, calculate the percentage of new cases
//      and total vaccinations on each available vaccine in relation to its population.
db.covid19data.aggregate( 
    [
        {$match:{"location":"Germany"}},
        {$project:{"location":1,
                    date:{$convert:{input: "$date",to:"date"}},
                    "population":{$convert:{input: "$population",to:"double"}},
                    "new_cases":{$ifNull: [ {$convert:{input: "$new_cases",to:"double"}}, 0 ]}
        }},
        {$project:{"location":1,
                    date:1,
                    "population":1,
                    "new_cases":1,
                    "new_cases_percentage":
                    {$concat:[{$convert:{input:{$multiply:[{$divide:["$new_cases","$population"]},100]},to:"string"}},"%"]}
        }},
        {$lookup:
            {
                from:"country_vaccinations_by_manufacturer",
                let: {date:"$date",population:"$population"},
                pipeline:[
                    {$match:{"location":"Germany"}},
                    {$project:{"location":1,
                                date:{$convert:{input: "$date",to:"date"}},
                                "vaccine":1,
                                "total_vaccinations":{$convert:{input:"$total_vaccinations",to:"double"}},
                                "percent_of_population":
                                {$concat:[{$convert:{input:{$multiply:[{$divide:[{$convert:{input:"$total_vaccinations",to:"double"}},"$$population"]},100]},to:"string"}},"%"]}
                    }},
                    {$match:
                        {$expr:
                            {$eq:["$date","$$date"]}
                        }
                    },
                    {$project:{"vaccine":1,"total_vaccinations":1,"percent_of_population":1}}
                ],
                as: "vaccineData"
            }
         }
    ]
);

// q19. Vaccination Drivers. Specific to Germany, based on each daily new case,
// display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.

db.covid19data.aggregate([
    { $match: { location: "Germany" } },
    { $group: { _id: [{ location: "$location" }, { new_cases_date: { $convert: { input: "$date", to: "date" } } }, { new_cases: "$new_cases" }] } },
    {
        $project: {
            _id: 0, location: { $arrayElemAt: ["$_id.location", 0] }, new_cases_date: { $arrayElemAt: ["$_id.new_cases_date", 0] },
            new_cases: { $arrayElemAt: ["$_id.new_cases", 0] }
        }
    },
    {
        $lookup: {
            from: "country_vaccinations_by_manufacturer",
            let: { new_cases_date: "$new_cases_date" },
            pipeline: [
                { $match: { location: "Germany" } },
                {
                    $group: {
                        _id: { date: { $convert: { input: "$date", to: "date" } }, vaccine: "$vaccine" },
                        total_vaccinations: { $max: { $convert: { input: "$total_vaccinations", to: "int" } } }
                    }
                },
                { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 } },
                { $match: { $expr: { $eq: ["$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 20 } }] } } },
                { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
                { $sort: { date: 1 } }], as: "after_20_days_vaccinations"
        }
    },
    {
        $lookup: {
            from: "country_vaccinations_by_manufacturer",
            let: { new_cases_date: "$new_cases_date" },
            pipeline: [
                { $match: { location: "Germany" } },
                {
                    $group: {
                        _id: { date: { $convert: { input: "$date", to: "date" } }, vaccine: "$vaccine" },
                        total_vaccinations: { $max: { $convert: { input: "$total_vaccinations", to: "int" } } }
                    }
                },
                { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 } },
                { $match: { $expr: { $eq: ["$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 30 } }] } } },
                { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
                { $sort: { date: 1 } }], as: "after_30_days_vaccinations"
        }
    },
    {
        $lookup: {
            from: "country_vaccinations_by_manufacturer",
            let: { new_cases_date: "$new_cases_date" },
            pipeline: [
                { $match: { location: "Germany" } },
                {
                    $group: {
                        _id: { date: { $convert: { input: "$date", to: "date" } }, vaccine: "$vaccine" },
                        total_vaccinations: { $max: { $convert: { input: "$total_vaccinations", to: "int" } } }
                    }
                },
                { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 } },
                { $match: { $expr: { $eq: ["$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 40 } }] } } },
                { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
                { $sort: { date: 1 } }], as: "after_40_days_vaccinations"
        }
    },
    { $project: { location: 1, new_cases_date: 1, new_cases: 1, after_20_days_vaccinations: 1, after_30_days_vaccinations: 1, after_40_days_vaccinations: 1 } }
])


// q20. Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of 
// accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), generate 
// the daily new cases after 21 days, 60 days, and 120 days

db.country_vaccinations_by_manufacturer.aggregate([
    {$match: {location: "Germany"}},
    {$group: {_id:{date: {$convert: {input: "$date", to: "date"}}}, 
    total_accumulated_vaccinations: {$sum: {$convert:{input: "$total_vaccinations", to: "double"}}}}},
    {$project: {_id:0,country: "$_id.country" , date:"$_id.date", total_accumulated_vaccinations: 1}},
    {
        $lookup: {
            from: "covid19data",
            let: {
                covid_date: "$date",
            },
            pipeline: [
                {$match: {location: "Germany"}},
                {
                
                    $project: {
                    location:1, date: {$convert: {input:"$date", to: "date"}}, new_cases:{$convert: {input:"$new_cases", to: "double"}}
                    }
                },
                {
                        $match:{$or:[
                            {$expr: {$eq: ["$date",{$dateAdd: {startDate: "$$covid_date", unit: "day", amount: 21}}]}},
                            {$expr: {$eq: ["$date",{$dateAdd: {startDate: "$$covid_date", unit: "day", amount: 60}}]}},
                            {$expr: {$eq: ["$date",{$dateAdd: {startDate: "$$covid_date", unit: "day", amount: 120}}]}}
                            ]
                        }
                },
                {
                    $project: {_id:0, new_cases:1}
                }
            ],
            as: "new_cases_list"
        }
    },
    {$project: {total_accumulated_vaccinations:1,date:1,cases_after_21_days:{$arrayElemAt:["$new_cases_list",0]}, cases_after_60_days:{$arrayElemAt:["$new_cases_list",1]}, cases_after_120_days:{$arrayElemAt:["$new_cases_list",2]}}},
    {$sort: {date:1}}
])
