-- Data cleaning and EDA project on a company layoff dataset

-- We will clean the raw dataset by performing the common data cleaning steps which include:
--	1. check and remove any duplicates
--	2. standerdize the data and fix any errors found
--	3. explore the data for any NULL or missing values and deal with them
--	4. remove unnecesarry columns and rows

-- First things first, we will import the dataset into a new schema and take a quick glance at the data

SELECT *
FROM layoffs;

-- We will create a backup dataset and work with the new dataset 

CREATE TABLE layoffs_clean
LIKE layoffs;

INSERT layoffs_clean
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_clean;

-- Next, we will explore the dataset for duplicates by creating a new column that gives us
-- a row number

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
				`date`, stage, country, funds_raised_millions) row_num
FROM layoffs_clean)
SELECT *
FROM duplicate_cte
WHERE row_num >1
;

SELECT *
FROM layoffs_clean
WHERE company = 'Wildlife Studios';		#Checking the results for different company names

CREATE TABLE `layoffs_clean2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
# Created a new table to get rid of the duplicates

INSERT INTO layoffs_clean2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
				`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_clean;
# Inserted the values from layoffs_clean and added the row_num column

DELETE
FROM layoffs_clean2
WHERE row_num > 1;
# Deleted the rows with duplicate data instances

-- Now we will standerdise the dataset

SELECT DISTINCT company
FROM layoffs_clean2;
# We can notice some blank spaces in the string columns so we will TRIM them

SELECT company, TRIM(company)
FROM layoffs_clean2;

UPDATE layoffs_clean2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_clean2
ORDER BY 1;
# We can see that the Crypto industry has errors so we are gonna fix that

SELECT *
FROM layoffs_clean2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_clean2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_clean2
ORDER BY 1;
# We can see here that United States has an error

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_clean2
ORDER BY 1;

UPDATE layoffs_clean2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

# The date column is in string data type so we will convert that to Date data type
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_clean2;

UPDATE layoffs_clean2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_clean2
MODIFY COLUMN `date` DATE;

-- Now we will explore the dataset, mainly the numerical variables for NULL cells

# We will first look into the industry column for null and missing values
SELECT *
FROM layoffs_clean2
WHERE industry IS NULL
OR industry = '';

# Explore some of the affected values for the company column
SELECT *
FROM layoffs_clean2
WHERE company = 'Airbnb';

# Joining the new table on itself to match values for industry
SELECT *
FROM layoffs_clean2 t1
JOIN layoffs_clean2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

# Transforming the missing values to NULLs to reduce errors
UPDATE layoffs_clean2
SET industry = NULL
WHERE industry = '';

# Updating the table to match the missing values for industry
UPDATE layoffs_clean2 t1
JOIN layoffs_clean2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

# We will now explore the cells where both total_laid_off and percentage_laid_off have NULLs
SELECT *
FROM layoffs_clean2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# We will delete these cells as there are no way of populating the cells with existing data
DELETE
FROM layoffs_clean2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Finally, we will drop the row_num column we created to explore duplicates
ALTER TABLE layoffs_clean2
DROP COLUMN row_num;

# The final cleaned dataset
SELECT *
FROM layoffs_clean2;

---------
-- Exploratory Data Analysis

SELECT * 
FROM layoffs_clean2;

# Doing some basic descriptive statistics for any obvious trends
SELECT 	MAX(total_laid_off),
		MAX(percentage_laid_off),
		MIN(total_laid_off), 
        AVG(total_laid_off)
FROM layoffs_clean2;
# We can see that there are companies that laid off 100% of their employees

# Looking up the companies that laid off all of their employees
SELECT *
FROM layoffs_clean2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;
# Ther biggest company to lay off all of it's employees is a construction company in the US

# Looking at the companies with the most layoffs
SELECT company, SUM(total_laid_off) AS sum_total, YEAR(`date`) AS year
FROM layoffs_clean2
GROUP BY company, year
ORDER BY sum_total DESC;
# The tech companies have the most layoffs in recent years with Amazon following them

# Looking at the industry with the most layoffs
SELECT industry, SUM(total_laid_off) AS sum_total
FROM layoffs_clean2
GROUP BY industry
ORDER BY sum_total DESC;
# Consumer, retail and the transport industries got hit the hardest potentially due to the pandemic 

# Looking at the country with the most layoffs
SELECT country, SUM(total_laid_off) AS sum_total, YEAR(`date`) AS year
FROM layoffs_clean2
GROUP BY country, year
ORDER BY sum_total DESC;
# USA has by far the largest number of layoffs, more than the top countries combined

# Lookin at total layoffs per year
SELECT SUM(total_laid_off) AS sum_total, YEAR(`date`) AS year
FROM layoffs_clean2
GROUP BY year
ORDER BY year DESC;
# 2023 seems to be off to a bad start considering the data stops at 3 months and already getting close to 2022

# Looking at the different stages of the companies with the most layoffs
SELECT stage, SUM(total_laid_off) AS sum_total, company
FROM layoffs_clean2
GROUP BY stage, company
ORDER BY sum_total DESC;
# Similar to the company results, seems like the big tech companies Post-IPO have the most layoffs

# Creating a total sum for every month
SELECT 	SUBSTRING(`date`, 1,7) AS `month`,
		SUM(total_laid_off)
FROM layoffs_clean2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC
;

# Creating a rolling total sum for every month using the previous code chunk as CTE 
WITH rolling_total AS
(
SELECT 	SUBSTRING(`date`, 1,7) AS `month`,
		SUM(total_laid_off) AS sum_total
FROM layoffs_clean2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC
)
SELECT 	`month`,
		sum_total,
		SUM(sum_total) OVER(ORDER BY `month`) AS rolling_total
FROM Rolling_Total
;
# Using this we can check the increase in total laid off every year, which shows a significant increase in layoffs after 2021

# Creating a ranking based on the top 5 companies with the most layoffs for every year
WITH Company_Year (company, `year`, sum_total) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_clean2
GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `year` ORDER BY sum_total DESC) AS ranking
FROM Company_Year
WHERE `year` IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5
;
# Here we can see the top 5 companies for each year, with the tech companies leading the top positions in 2022 and 2023





