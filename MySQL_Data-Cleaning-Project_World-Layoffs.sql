-- Data Cleaning Project
-- 1. Import Raw Data & Create Staging
-- 2. Remove Duplicates
-- 3. Data Standardization
-- 4. Null Values or Blank Values
-- 5. Remove necessary columns

# Task 1 - Create Staging Table
SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs; 

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

# Task 2 - Remove Duplicates without a unique identifier
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;
-- added a row number column counting rows with unique company, industry, total laid off #s, percentage laid off #s and date.
-- For any in row_num greater than 2, that would be a duplicate row and needs to be removed.

# Create cte filtering for duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# Create new table for easily deleting duplicates
CREATE TABLE `layoffs_staging2` (
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

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

# Task 3 - Data Standardization
SELECT DISTINCT(TRIM(company))
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# Task 4 - Dealing with Nulls and Blanks
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb'; 

SELECT DISTINCT company, industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

# Task 5 - removing columns
ALTER TABLE layoffs_staging2
DROP COLUMN row_num; 


