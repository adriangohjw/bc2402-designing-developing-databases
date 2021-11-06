//Table for Q16-17
db.covid19data.aggregate([
    { $match: { location: "Singapore" } },
    { $project: { date: { $convert: { input: "$date", to: "date" } }, location: 1, 
                  new_cases: { $convert: { input: "$new_cases", to: "double", onNull: NumberInt(0) } } } },
    { $lookup: { from: "country_vaccinations",
                 let: { date: "$date" },
                 pipeline: [ { $match: { country: "Singapore" } }, 
                             { $project: { date: { $convert: { input: "$date", to: "date" } },
                                           total_vaccinations: { $convert: { input: "$total_vaccinations", to: "double", onNull: NumberInt(0) } } } },
                             { $match: { $expr: { $eq: [ "$date", "$$date" ] } } },
                             { $project: { _id: 0, total_vaccinations: 1 } }
                           ], as: "vaccine_data"
    } },
    { $out: "sg_data" }
])

db.sg_data.find({})

//Table for Q18 & Q20 
//From country_vaccinations: date, location, total_vaccinations, vaccine
//From covid19data: new_cases, population
db.country_vaccinations_by_manufacturer.aggregate([
    { $match: { location: "Germany" } },
    { $project: { date: { $convert: { input: "$date", to: "date" } },
                  location: 1,
                  total_vaccinations: { $convert: { input: "$total_vaccinations", to: "double", onNull: NumberInt(0) } },
                  vaccine: 1 } },
    { $lookup: { from: "covid19data",
                 let: { date: "$date" },
                 pipeline: [ { $match: { location: "Germany" } },
                             { $project: { date: { $convert: { input: "$date", to: "date" } },
                                           new_cases: { $convert: { input: "$new_cases", to: "double", onNull: NumberInt(0) } },
                                           population: { $convert: { input: "$population", to: "double" } } } },
                             { $match: { $expr: { $eq: [ "$date", "$$date" ] } } },
                             { $project: { _id: 0, "covid_date": "$date", new_cases: 1, population: 1 } }
                           ],
                 as: "covid_data"
               }
    },
    { $unwind: "$covid_data" },
    { $out: "germany_data" }
])

db.germany_data.find({})














