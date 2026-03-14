select type, count(*) as total_titles
from netflix_titles
group by type;

select year_added, count(*) as titles_added
from netflix_titles
group by year_added
order by year_added;

select country, count(*) as total_titles
from netflix_titles 
where country is not null
group by country
order by total_titles desc
limit 10;

select rating, count(*) as count_titles
from netflix_titles
group by rating 
order by count_titles desc;

select listed_in, count(*) as total_titles
from netflix_titles
group by listed_in
order by total_titles desc
 limit 10;
 
 select  director, count(*) as total_titles
 from netflix_titles
 where director is not null
 group by director
 order by total_titles desc
 limit 10;
 
 select avg(duration_num) as avg_movie_duration
 from netflix_titles
 where duration_type = 'minutes';
 
 
 select title, duration_num
 from netflix_titles
 where duration_type = 'minutes'
 order by duration_num desc
 limit 10;
 SELECT country, COUNT(*) AS total_titles
FROM netflix_titles
WHERE country IS NOT NULL
GROUP BY country
ORDER BY total_titles DESC;