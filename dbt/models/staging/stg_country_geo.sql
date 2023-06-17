{{ config(materialized='view') }}

with geo_country as (
    SELECT 
        WHO_Region,
        Country_region as Country,
        Continent,
        SUM(Population) AS Population,
        SUM(TotalCases) AS TotalCases,
        SUM(NewCases) AS NewCases,
        SUM(TotalDeaths) AS TotalDeaths,
        SUM(NewDeaths) AS NewDeaths,
        SUM(TotalRecovered) AS TotalRecovered,
        SUM(NewRecovered) AS NewRecovered,
        SUM(ActiveCases) AS ActiveCases
    FROM {{ source('staging', 'woldometer_data')}}
    GROUP BY WHO_Region, Country, Continent
)

SELECT 
    WHO_Region,
    Country,
    Continent,
    Population,
    CASE 
        WHEN Population > 100000000 THEN 'Large'
        WHEN Population >= 1000000 THEN 'Medium'
        ELSE 'Small'
    END as Population_Category,
    TotalCases,
    NewCases,
    TotalDeaths,
    NewDeaths,
    TotalRecovered,
    NewRecovered,
    ActiveCases,
    {{ calculate_ratio('TotalCases', 'Population') }} AS Population_COVID_ratio
FROM geo_country 
