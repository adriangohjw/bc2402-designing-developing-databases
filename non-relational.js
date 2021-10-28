
// q4. Which is the most administrated vaccine? Display a list of total administration 
// (i.e., sum of total vaccinations) per vaccine.
// [source table: country_vaccinations_by_manufacturer]

db.country_vaccinations_by_manufacturer.aggregate([
    {$group: {_id:{groupByVaccine: "$vaccine"}, total_administrated:{$max: {$convert:{input: "$total_vaccinations", to: "double"}}}}},
    {$project: {_id:1 ,total_administrated:1}},
    {$sort: {total_administrated : -1}}
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


// q12. What is the total population among the ten ASEAN countries?

db.covid19data.aggregate([
    {$match: {location: {$in: ['Brunei', 'Myanmar' , 'Cambodia', 'Indonesia', 'Laos', 'Malaysia', 'Philippines', 'Singapore', 'Thailand' , 'Vietnam']}}},
    {$group: {_id: {groupByLocation: "$location"}, population: {$avg: {$convert: {input: "$population", to: "double"}}}}},
    {$group: {_id: '', total_population: {$sum: "$population"}}},
    {$project: {_id:1, total_population: 1}}
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
    {$project: {_id:0, sum_new_cases:1}
])

// q20. Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of 
// accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), generate 
// the daily new cases after 21 days, 60 days, and 120 days

db.country_vaccinations.aggregate([
    {$match: {country: "Germany"}},
    {$group: {_id:[{country :"$country"},{date: {$convert: {input: "$date", to: "date"}}}] , 
    total_accumulated_vaccinations: {$sum: {$convert:{input: "$total_vaccinations", to: "double"}}}}},
    {$project: {_id:0,country: {$arrayElemAt: ["$_id.country",0]} , date:{$arrayElemAt: ["$_id.date",0]}, total_accumulated_vaccinations: 1}},
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
            as: "new_cases"
        }
    },
    {$project: {total_accumulated_vaccinations:1,date:1,cases_after_21_days:{$arrayElemAt:["$new_cases",0]}, cases_after_60_days:{$arrayElemAt:["$new_cases",1]}, cases_after_120_days:{$arrayElemAt:["$new_cases",2]}}},
    {$sort: {date:1}}
])


