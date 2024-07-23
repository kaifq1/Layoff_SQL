CREATE DATABASE world_layoff;

use world_layoff;
select * from layoffs;

create table layoff_staging
like layoffs;

insert layoff_staging
select * from layoffs;

select * from layoff_staging;

ALTER TABLE layoff_staging
ADD row_num INT;

INSERT INTO layoff_staging
SELECT *,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoff_staging;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoff_staging2;


INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoff_staging;

create table layoff_staging
like layoffs;

select * from layoff_staging;

insert layoff_staging 
select * from layoffs;

SELECT *,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoff_staging;

with duplicate_cte as 
(SELECT *,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoff_staging
)

select * from
duplicate_cte
where row_num > 1;

select * from
layoff_staging
where company = 'Casper';

select * from layoff_staging2;


CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoff_staging2;


INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoff_staging;

select * from layoff_staging2;
where row_num > 1;


DELETE from layoff_staging2
where row_num > 1;

Select company,(Trim(company))
from layoff_staging;

Update layoff_staging2
SET company = trim(company);

select distinct industry
from layoff_staging2;

select * 
from layoff_staging2
where industry like 'Crypto';

Update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select Distinct country, trim(Trailing '.' from country)
from layoff_staging2
where country like 'United States%'
Order by 1;


update layoff_staging2
set country = Trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
SET `date` = Str_to_date(`date`,'%m/%d/%Y');

select * from layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL
AND industry IS NULL
OR industry = '';  

alter table layoff_staging2 
modify column `date` date; 

select * from layoff_staging2 t1
Join layoff_staging2 t2
	ON t1.company = t2.company
    And t1.location = t2.location
Where (t1.industry is NUll OR t1.industry ='')
And t2.industry is NOT Null;


Update layoff_staging2
Set industry = null
where industry = '';

Select * from layoff_staging2;


UPDATE layoff_staging2 t1
Join layoff_staging2 t2
	ON t1.company = t2.company
Set t1.industry = t2.industry
Where t1.industry is NUll 
And t2.industry is NOT Null;

Delete 
from layoff_staging2
Where total_laid_off is NULL
And percentage_laid_off is NUll;

Alter table layoff_staging2
Drop column row_num;

Select * from layoff_staging2;






































