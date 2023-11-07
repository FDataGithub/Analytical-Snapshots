-- QUERY1 to select non-null continent data from Coviddeaths and order by columns 3 and 4
SELECT*
FROM [PortFolio Project]..Coviddeaths
WHERE continent IS NOT NULL
ORDER BY 3,4


-- QUERY2 to look at total cases vs. total deaths in Italy for the year 2023

select location, date, total_cases, new_cases, total_deaths, 
	case
		when cast(cast(total_cases as float) as decimal) = 0 Then null
		else cast(cast(total_deaths as float) as decimal)/ cast(cast(total_cases as float) as decimal)*100
		end as  death_rate

from [PortFolio Project]..Coviddeaths
	where location like '%italy%'
	and date like '%2023%'
ORDER BY location, date;


-- QUERY3 to look at total cases vs. population in Italy for the year 2023

select location, date, total_cases, new_cases, population, 
	case
		when cast(cast(total_cases as float) as decimal) = 0 Then null
		else cast(cast(total_cases as float) as decimal)/ cast(cast(population as float) as decimal)*100
		end as  PercentPopulationInfected

from [PortFolio Project]..Coviddeaths
	where location like '%italy%'
	and date like '%2023%'
order by 1,2

-- QUERY4 to find countries with the highest infection rate compared to population

select location, population, max(total_cases) as HighestInfectionCount,
	case
		when max(cast(cast(total_cases as float) as decimal)) = 0 Then null
		else max(cast(cast(total_cases as float) as decimal)/ cast(cast(population as float) as decimal)*100)
		end as PercentPopulationInfected

from [PortFolio Project]..Coviddeaths
group by location, population
order by PercentPopulationInfected desc

-- UPDATE THE DATA, ".0" from total_deaths data
UPDATE [Portfolio Project]..Coviddeaths
SET total_deaths = REPLACE(total_deaths, '.0', '')


-- QUERY5 to show countries with the highest death count per population

select location, max (cast(total_deaths as int)) as TotalDeathsCount
from [PortFolio Project]..Coviddeaths
WHERE continent IS NOT null 
	and location not in ('World','High income','Upper middle income','Europe','Asia','Africa','Lower middle income','Oceania','North America','South America')
group by location
order by TotalDeathsCount desc

-- QUERY6 to show continents with the highest death count per population

 Select continent, max(cast(total_deaths as int)) as TotalDeathsCount
 from [PortFolio Project]..Coviddeaths

 where continent is not null and continent <> ' '
  group by continent
 order by TotalDeathsCount desc

-- QUERY7 to get global numbers with date

WITH CTE AS (
    SELECT 
			cast(date as DATE) OriginalDate,
			FORMAT(cast(date as DATE), 'dd/MM/yyyy') as Data,
           sum(cast(cast(new_cases as float)as decimal)) as NewCasesTotal, 
           sum(cast(cast(new_deaths as float)as decimal)) as NewDeathsTotal
    FROM [PortFolio Project]..Coviddeaths
    GROUP BY date
)
SELECT Data, NewCasesTotal, NewDeathsTotal, 
       NewCasesTotal / NULLIF(NewDeathsTotal, 0) as DeathPercentage
FROM CTE
order by OriginalDate;

-- QUERY8 to get global totals without date

WITH CTE AS (
    SELECT 
		   sum(cast(cast(new_cases as float)as decimal)) as NewCasesTotal, 
           sum(cast(cast(new_deaths as float)as decimal)) as NewDeathsTotal
    FROM [PortFolio Project]..Coviddeaths
    --GROUP BY date
)
SELECT NewCasesTotal, NewDeathsTotal, 
       (NewDeathsTotal / NULLIF(NewCasesTotal, 0))*100 as DeathPercentage
FROM CTE

-- Correct the update statement to replace '.0' in population field
Update a
set a.population= REPLACE (b.population, '.0', '')
from [PortFolio Project]..Coviddeaths a
join [PortFolio Project]..Covid_deaths2 b
on a.continent=b.continent
and b.date=a.date

-- QUERY9  to join Coviddeaths with CovidVaccinations and calculate the total vaccinations for location

select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations

from [PortFolio Project]..Coviddeaths dea
join [PortFolio Project]..CovidVacciantions vac
	on dea.location = vac.location
	and dea.date=vac.date
where dea.continent is not null and dea.continent <> ''
order by 2,3,4
update [PortFolio Project]..CovidVacciantions
set new_vaccinations = replace(new_vaccinations,'.0','')

-- QUERY10 to get the rolling total vaccinations for each location

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	sum(coalesce(Convert(bigint, vac.new_vaccinations),0)) over (partition by dea.location order by dea.location, dea.date) 
	as totalVaccinationForLocation
FROM [PortFolio Project]..Coviddeaths dea
JOIN [PortFolio Project]..CovidVacciantions vac
	ON dea.location = vac.location
	AND CONVERT(datetime, dea.date, 103) = CONVERT(datetime, vac.date, 103)
WHERE dea.continent IS NOT NULL AND dea.continent <> ''
ORDER BY dea.location, CONVERT(datetime, dea.date, 103), dea.population;

---total vaccination dava problemi nel raggruppamento, rappresentando anche punti non corretti

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
       sum(COALESCE(TRY_CONVERT(bigint, vac.new_vaccinations), 0)) over (partition by dea.location order by CONVERT(datetime, dea.date, 103) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       as RollingPeoplevaccinated
FROM [PortFolio Project]..Coviddeaths dea
JOIN [PortFolio Project]..CovidVacciantions vac
	ON dea.location = vac.location
	AND CONVERT(datetime, dea.date, 103) = CONVERT(datetime, vac.date, 103)
WHERE dea.continent IS NOT NULL AND dea.continent <> ''
ORDER BY dea.location, CONVERT(datetime, dea.date, 103), dea.population;

-- QUER10 to get the rolling total vaccinations for each location
WITH PopvsVac AS (
    SELECT 
        dea.continent,
        dea.location,
        CONVERT(datetime, dea.date, 103) AS Date,
        TRY_CAST(dea.population AS bigint) AS Population,
        TRY_CAST(vac.new_vaccinations AS bigint) AS New_vaccinations,
        SUM(TRY_CAST(vac.new_vaccinations AS bigint)) OVER (
            PARTITION BY dea.location
            ORDER BY CONVERT(datetime, dea.date, 103)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RollingPeopleVaccinated
    FROM [PortFolio Project]..Coviddeaths dea
    JOIN [PortFolio Project]..CovidVacciantions vac
        ON dea.location = vac.location
        AND CONVERT(datetime, dea.date, 103) = CONVERT(datetime, vac.date, 103)
    WHERE dea.continent IS NOT NULL AND dea.continent <> ''
)
SELECT 
    Continent,
    Location,
    Date,
    Population,
    New_vaccinations,
    RollingPeopleVaccinated,
    CASE 
        WHEN Population = 0 OR Population IS NULL THEN NULL
        ELSE (RollingPeopleVaccinated * 1.0 / Population) * 100
    END AS PercentagePopulationVaccinated
FROM PopvsVac
ORDER BY Location, Date;

-- Creating a temporary table to store the percentage of the population vaccinated
DROP TABLE IF EXISTS #PercentPopulationVaccinated;
CREATE TABLE #PercentPopulationVaccinated (
    Continent NVARCHAR(255),
    Location NVARCHAR(255),
    Date DATETIME,
    Population BIGINT,
    New_vaccinations BIGINT,
    RollingPeopleVaccinated BIGINT,
    PercentagePopulationVaccinated FLOAT
);

-- Inserting the data into the temporary table
INSERT INTO #PercentPopulationVaccinated
SELECT 
    Continent,
    Location,
    Date,
    Population,
    New_vaccinations,
    RollingPeopleVaccinated,
    CASE 
        WHEN Population = 0 OR Population IS NULL THEN NULL
        ELSE (RollingPeopleVaccinated * 1.0 / Population) * 100
    END AS PercentagePopulationVaccinated
FROM PopvsVac;

-- Creating a view to store data for later visualizations
IF OBJECT_ID('[PortFolio Project]..PercentPopulationVaccinated', 'V') IS NOT NULL
    DROP VIEW [PortFolio Project]..PercentPopulationVaccinated;
GO

CREATE VIEW [PortFolio Project]..PercentPopulationVaccinated AS
SELECT 
    Continent,
    Location,
    Date,
    Population,
    New_vaccinations,
    RollingPeopleVaccinated,
    PercentagePopulationVaccinated
FROM #PercentPopulationVaccinated;
GO

-- Now, IS POSIBLE  select from the view with ordering
SELECT *
FROM [PortFolio Project]..PercentPopulationVaccinated
ORDER BY Location, Date;

