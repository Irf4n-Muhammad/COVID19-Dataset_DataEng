WITH weekly_table AS
(
    SELECT
        TIMESTAMP_TRUNC(Date, WEEK) as Week_date,
        Country_region,
        WHO_Region,
        SUM(Confirmed) as Confirmed,
        SUM(Deaths) as Deaths,
        SUM(Recovered) as Recovered,
        SUM(Active) as Active,
        SUM(New_cases) as New_cases,
        SUM(New_deaths) as New_deaths,
        SUM(New_recovered) as New_recovered
    FROM {{ source('staging', 'full_grouped') }}
    GROUP BY
        TIMESTAMP_TRUNC(Date, WEEK), 
        Country_region, 
        WHO_Region
)

SELECT
    -- timestamp
    row_number() over(PARTITION BY Week_date ORDER BY Confirmed DESC) as rn,
    Week_date,

    -- region
    Country_region as Country,
    WHO_Region,

    -- covid situation
    Confirmed,
    Deaths,
    Recovered,
    Active,
    New_cases,
    New_deaths,
    New_recovered
FROM weekly_table
