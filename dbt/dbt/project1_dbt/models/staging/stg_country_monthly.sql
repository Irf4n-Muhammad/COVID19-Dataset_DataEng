{{ config(materialized='view') }}

WITH Monthly_table AS
(
    SELECT
        TIMESTAMP_TRUNC(Date, Month) as Month_date,
        Country_region,
        M.WHO_Region,
        SUM(Confirmed) as Confirmed,
        SUM(Deaths) as Deaths,
        SUM(Recovered) as Recovered,
        SUM(Active) as Active,
        SUM(New_cases) as New_cases,
        SUM(New_deaths) as New_deaths,
        SUM(New_recovered) as New_recovered,

        -- Population data
        S.Population_Category,
        {{ calculate_ratio('Confirmed', 'S.Population') }} AS Population_COVID_ratio,
        S.Population AS New_Population
    FROM {{ source('staging', 'full_grouped') }} AS M JOIN {{ ref('stg_country_geo') }} AS S
    ON M.Country_region = S.Country
    GROUP BY
        TIMESTAMP_TRUNC(Date, Month), 
        Country_region, 
        WHO_Region,
        New_Population,
        Population_COVID_ratio,
        Population_Category
)

SELECT
    -- timestamp
    row_number() over(PARTITION BY Month_date ORDER BY Confirmed DESC) as rn,
    Month_date,

    -- region
    Country_region as Country,
    WHO_Region,

    --Population
    Population_Category,
    New_Population,
    Population_COVID_ratio,

    -- covid situation
    Confirmed,
    Deaths,
    Recovered,
    Active,
    New_cases,
    New_deaths,
    New_recovered
FROM Monthly_table 