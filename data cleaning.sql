-- Data Cleaning



-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any columns

SELECT * FROM world_layoffs.layoffs;

create table layoffs_staging
like layoffs;  --- create duplicate table with same columns and data --

select * from layoffs_staging;

insert layoffs_staging
select *
from Layoffs;


select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num>1; 

delete 
from duplicate_cte
where row_num>1; 

-- error occured  so create another table and delete

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num  INT
);

select *
from layoffs_staging2;

insert into layoffs_staging2
select *, 
row_number() over( 
partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, country,
 funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num>1;

select *
from layoffs_staging2;

-- Standardizing data(finding issues in data and fixing it)

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select distinct(industry)
from layoffs_staging2
order by 1;
-- crypto has to be updated

select *
from layoffs_staging2
where industry like 'Crypto%';
-- update all into crypto--

update  layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

select distinct(country)
from layoffs_staging2;
-- united states has error as unitedstates. so update it

update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United states%';

select *
from layoffs_staging2;

-- date  datatype is text so change it to date
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');
-- changed data type in schema using alter--
alter  table layoffs_staging2
modify column `date` date;


-- step 3: non null values

select *
from layoffs_staging2
where industry is null
or industry='';

update layoffs_staging2
set industry=null
where industry='';

select *
from layoffs_staging2
where company='Airbnb';

-- finding out any relevant company loaction without null values using self joins

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;
 
--  update null values with relevant values
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- droping column of row_num

 alter table layoffs_staging2
 drop column row_num;
 









