//3
db.country_vaccinations.aggregate([
    { $group: { _id: "$country", "max_daily_vaccinations_per_million": { $max: "$daily_vaccinations_per_million" } } } ] )

//7
db.country_vaccinations.aggregate([
    { $project: { country: 1, vaccines: 1, people_vaccinated_per_hundred: { $convert: { input: "$people_vaccinated_per_hundred", to: "double" } } } },
    { $match: { people_vaccinated_per_hundred: { $gt: 60 } } },
    { $group: {_id: [ { country: "$country" }, { vaccines: "$vaccines" } ], max_people_vaccinated_per_hundred: { $max:"$people_vaccinated_per_hundred" } } },
    { $project: { _id:0, country: { $arrayElemAt: ["$_id.country", 0] }, vaccines: { $arrayElemAt: ["$_id.vaccines",0] }, max_people_vaccinated_per_hundred: 1 } } ] )

//11
db.covid19data.aggregate([
    { $project: { continent: 1, location: 1, population: { $convert: { input: "$population", to:"double" } } } },
    { $match: { continent: { $eq: "Asia" } } },
    { $group: { _id: "$location", max_pop: { $max: "$population" } } },
    { $group: { _id: "asiaPopulation", asiaPopulation: { $sum: "$max_pop" } } },
    { $project: {_id: 0, asiaPopulation: 1 } } ] )

//15
db.country_vaccinations.aggregate([
    { $match: { country: "Singapore" } },
    { $group: { _id: "first_date", first_date: { $min: "$date" } } },
    { $project: { _id: 0, first_date: 1 } } ] )

//19
db.covid19data.aggregate([
    { $match: { location: "Germany" } },
    { $group: { _id: [ { location: "$location" }, { new_cases_date: { $convert: { input: "$date", to: "date"} } }, { new_cases: "$new_cases" } ] } },
    { $project: { _id: 0, location: { $arrayElemAt: ["$_id.location",0] }, new_cases_date: { $arrayElemAt: ["$_id.new_cases_date",0] }, 
        new_cases: { $arrayElemAt: ["$_id.new_cases",0] } } },
    { $lookup: {
        from: "country_vaccinations_by_manufacturer",
        let: { new_cases_date: "$new_cases_date" },
        pipeline: [
            { $match: { location: "Germany" } },
            { $group: { _id: { date: { $convert: { input: "$date" , to: "date" } }, vaccine: "$vaccine" }, 
                total_vaccinations: { $max: { $convert: { input: "$total_vaccinations" to: "int" } } } } },
            { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 },
            { $match: { $expr: { $eq: [ "$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 20 } } ] } } }, 
            { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
            { $sort: { date: 1 } } ], as: "after_20_days_vaccinations" } },
    { $lookup: {
        from: "country_vaccinations_by_manufacturer",
        let: { new_cases_date: "$new_cases_date" },
        pipeline: [
            { $match: { location: "Germany" } },
            { $group: { _id: { date: { $convert: { input: "$date" , to: "date" } }, vaccine: "$vaccine" }, 
                total_vaccinations: { $max: { $convert: { input: "$total_vaccinations" to: "int" } } } } },
            { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 },
            { $match: { $expr: { $eq: [ "$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 30 } } ] } } }, 
            { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
            { $sort: { date: 1 } } ], as: "after_30_days_vaccinations" } },
    { $lookup: {
        from: "country_vaccinations_by_manufacturer",
        let: { new_cases_date: "$new_cases_date" },
        pipeline: [
            { $match: { location: "Germany" } },
            { $group: { _id: { date: { $convert: { input: "$date" , to: "date" } }, vaccine: "$vaccine" }, 
                total_vaccinations: { $max: { $convert: { input: "$total_vaccinations" to: "int" } } } } },
            { $project: { date: "$_id.date", vaccine: "$_id.vaccine", total_vaccinations: 1 },
            { $match: { $expr: { $eq: [ "$date", { $dateAdd: { startDate: "$$new_cases_date", unit: "day", amount: 40 } } ] } } }, 
            { $project: { _id: 0, date: 1, vaccine: 1, total_vaccinations: 1 } },
            { $sort: { date: 1 } } ], as: "after_40_days_vaccinations" } },
    { $project: { location: 1, new_cases_date: 1, new_cases: 1, after_20_days_vaccinations: 1, after_30_days_vaccinations: 1, after_40_days_vaccinations: 1 } } ] )



