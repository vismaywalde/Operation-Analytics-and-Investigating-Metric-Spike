create database project3;
show databases;
use project3;
CREATE TABLE job_data
(ds DATE,
job_id INT NOT NULL,
actor_id INT NOT NULL,
event VARCHAR(15) NOT NULL,
language VARCHAR(15) NOT NULL,
time_spent INT NOT NULL,
org CHAR(2)
);

INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org)
VALUES ('2020-11-30', 21, 1001,	'skip',	'English',	15,	'A'),
('2020-11-30',  22, 1006,	'transfer',	'Arabic',	25,	'B'),
('2020-11-29',	23,	1003,	'decision',	'Persian',	20,	'C'),
('2020-11-28',	23,	1005,	'transfer',	'Persian',	22,	'D'),
('2020-11-28',	25,	1002,	'decision',	'Hindi',	11,	'B'),
('2020-11-27',	11,	1007,	'decision',	'French',	104,'D'),
('2020-11-26',	23,	1004,	'skip',	    'Persian',	56,	'A'),
('2020-11-25',	20,	1003,	'transfer',	'Italian',	45,	'C');



select * from job_data;



#Task 1: Jobs Reviewed Over Time
select ds, count(job_id) as jobs_per_day, sum(time_spent)/3600 as hours_spent
from job_data
where ds >='2020-11-01' and ds <= '2020-11-30'
group by ds ;



#Task 2: Throughput Analysis
WITH JD AS (
SELECT ds,
COUNT(job_id) AS num_jobs,
SUM(time_spent) AS total_time
FROM job_data
WHERE event IN('transfer', 'decision') AND ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
)
SELECT ds, ROUND(1.0* SUM(num_jobs) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT
ROW) / SUM(total_time) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND
CURRENT ROW),2) AS throughput_7d
FROM JD




WITH JD AS (
    SELECT ds, COUNT(job_id) AS num_jobs, SUM(time_spent) AS total_time
    FROM job_data
    WHERE event IN('transfer', 'decision') AND ds BETWEEN '2020-11-01' AND '2020-11-30'
    GROUP BY ds
)
SELECT ds, ROUND(1.0 * SUM(num_jobs) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 
SUM(total_time) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS throughput_7d
FROM JD;











#Task 3: Language Share Analysis
WITH JD AS (
    SELECT 
        language,
        COUNT(job_id) AS num_jobs
    FROM 
        job_data
    WHERE 
        event IN ('transfer', 'decision')
    AND 
        ds BETWEEN '2020-11-01' AND '2020-11-30'
    GROUP BY 
        language
),
total AS (
    SELECT 
        COUNT(job_id) AS total_jobs
    FROM 
        job_data
    WHERE 
        event IN ('transfer', 'decision')
    AND 
        ds BETWEEN '2020-11-01' AND '2020-11-30'
)
SELECT 
    JD.language,
    ROUND(100.0 * JD.num_jobs / total.total_jobs, 2) AS perc_job
FROM 
    JD
CROSS JOIN 
    total
ORDER BY 
    perc_job DESC;







WITH JD AS (
    SELECT language, COUNT(job_id) AS num_jobs
    FROM job_data
    WHERE event IN ('transfer', 'decision') AND ds BETWEEN '2020-11-01' AND '2020-11-30'
    GROUP BY language
),
total AS (
    SELECT COUNT(job_id) AS total_jobs
    FROM job_data
    WHERE event IN ('transfer', 'decision') AND ds BETWEEN '2020-11-01' AND '2020-11-30'
)
SELECT JD.language, ROUND(100.0 * JD.num_jobs / total.total_jobs, 2) AS perc_job
FROM JD CROSS JOIN total ORDER BY perc_job DESC;


WITH JD AS (
    SELECT language, COUNT(job_id) AS num_jobs
    FROM job_data
    WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
    GROUP BY language
),
total AS (
    SELECT COUNT(job_id) AS total_jobs
    FROM job_data
    WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
)
SELECT JD.language, 
       ROUND(100.0 * JD.num_jobs / total.total_jobs, 2) AS perc_job
FROM JD 
CROSS JOIN total 
ORDER BY perc_job DESC;




#Task 4: Duplicate Rows Detection
SELECT 
    ds, 
    job_id, 
    actor_id, 
    event, 
    language, 
    time_spent, 
    org,
    COUNT(*) AS count_duplicates
FROM 
    job_data
GROUP BY 
    ds, job_id, actor_id, event, language, time_spent, org
HAVING 
    COUNT(*) > 1;
    
    


SELECT ds, job_id, actor_id, event, language, time_spent, org, COUNT(*) AS count_duplicates
FROM job_data
GROUP BY ds, job_id, actor_id, event, language, time_spent, org
HAVING COUNT(*) > 1;



# Table-1 users
create table users (
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));


SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



alter table users add column temp_created_at datetime;
UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');
ALTER TABLE users DROP COLUMN created_at;
ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;



# Table-2 events
create table events (
user_id INT,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type INT
);

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

desc events;
select * from events;

ALTER TABLE events ADD COLUMN temp_occurred_at DATETIME;
UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');
ALTER TABLE events DROP COLUMN occurred_at;
ALTER TABLE events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;







# Table-3 email_events
create table emailEvents(
user_id INT,
occured_at varchar(100),
action varchar(100),
user_type int
);


SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE emailEvents
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from emailEvents;

ALTER TABLE emailEvents ADD COLUMN temp_occured_at DATETIME;
UPDATE emailEvents SET temp_occured_at = STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i');
ALTER TABLE emailEvents DROP COLUMN occured_at;
ALTER TABLE emailEvents CHANGE COLUMN temp_occured_at occured_at DATETIME;



#(a) Weekly User Engagement
select extract(week from occurred_at) as week_number,
count(distinct user_id) as active_user
from events
where event_type='engagement'
group by week_number
order by week_number;





#User Growth Analysis
SELECT 
    YEAR(created_at) AS year, -- Extract the year
    WEEK(created_at, 1) AS week_num, -- Extract the week number of the year  (1 = week starts from Monday)
	COUNT(CASE WHEN activated_at IS NOT NULL THEN u.user_id ELSE NULL END) AS activated_users, -- Count of activated users
    SUM(COUNT(*)) OVER (ORDER BY YEAR(created_at), WEEK(created_at, 1)) AS cumulative_users -- Cumulative sum of all users
FROM users u
WHERE created_at >= '2013-04-01'
AND created_at < '2014-09-30'
GROUP BY year, week_num
ORDER BY year, week_num;



#Weekly Retention Analysis:
-- Step 1: Extract sign-up week for each user
WITH signups AS (
    SELECT 
        user_id,
        EXTRACT(YEAR FROM occurred_at) AS signup_year,
        EXTRACT(WEEK FROM occurred_at) AS signup_week
    FROM events
    WHERE event_type = 'signup_flow'
    AND event_name = 'complete_signup'
),
-- Step 2: Extract engagement week for each user
engagements AS (
    SELECT 
        user_id,
        EXTRACT(YEAR FROM occurred_at) AS engagement_year,
        EXTRACT(WEEK FROM occurred_at) AS engagement_week
    FROM events
    WHERE event_type = 'engagement'
),
-- Step 3: Join signups and engagements to track retention
retention AS (
    SELECT 
        s.user_id,
        s.signup_year,
        s.signup_week,
        e.engagement_year,
        e.engagement_week,
        (e.engagement_year - s.signup_year) * 52 + (e.engagement_week - s.signup_week) AS retention_week -- Calculate the retention week (0 is the sign-up week, 1 is the first week after sign-up, etc.)
    FROM signups s
    LEFT JOIN engagements e ON s.user_id = e.user_id
)
-- Step 4: Calculate the number of retained users per week
SELECT 
    signup_year,
    signup_week,
    COUNT(DISTINCT user_id) AS total_signups,
    SUM(CASE WHEN retention_week = 0 THEN 1 ELSE 0 END) AS week_0_retained,  -- Users active in the sign-up week
    SUM(CASE WHEN retention_week = 1 THEN 1 ELSE 0 END) AS week_1_retained,  -- Users retained in the first week
    SUM(CASE WHEN retention_week = 2 THEN 1 ELSE 0 END) AS week_2_retained,  -- Users retained in the second week
    SUM(CASE WHEN retention_week = 3 THEN 1 ELSE 0 END) AS week_3_retained   -- Users retained in the third week
    -- You can extend this pattern for additional weeks
FROM retention
GROUP BY signup_year, signup_week
ORDER BY signup_year, signup_week;



#Weekly Engagement Per Device:
-- Step 1: Calculate weekly engagement per device
WITH weekly_engagement AS (
    SELECT 
        CONCAT(EXTRACT(YEAR FROM occurred_at), '-', EXTRACT(WEEK FROM occurred_at)) AS weeknum, -- Extract year and week as 'year-week' format
        device,
        COUNT(DISTINCT user_id) AS user_count -- Count distinct users for each device in each week
    FROM events
    WHERE event_type = 'engagement' -- Only consider engagement events
    GROUP BY weeknum, device -- Group by week and device
    ORDER BY weeknum
)
-- Step 2: Select the results
SELECT 
    weeknum,
    device,
    user_count
FROM weekly_engagement;

    
    

-- Email Engagement Analysis
SELECT
    100 * SUM(CASE WHEN email_cat = 'email_open' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN email_cat = 'email_sent' THEN 1 ELSE 0 END), 0) AS email_open_rate,
	100 * SUM(CASE WHEN email_cat = 'email_clicked' THEN 1 ELSE 0 END) /
    NULLIF(SUM(CASE WHEN email_cat = 'email_sent' THEN 1 ELSE 0 END), 0) AS email_click_rate
FROM (
    SELECT 
        user_id,
        occured_at,
        action,
        user_type,
        CASE
            WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
            WHEN action = 'email_open' THEN 'email_open'
            WHEN action = 'email_clickthrough' THEN 'email_clicked'
            ELSE NULL
        END AS email_cat
    FROM emailEvents
) sub
WHERE email_cat IS NOT NULL; -- Exclude NULL categories



SELECT
    100 * SUM(CASE WHEN email_cat = 'email_open' THEN 1 ELSE 0 END) / 
        SUM(CASE WHEN email_cat = 'email_sent' THEN 1 ELSE 0 END) AS email_open_rate,
    100 * SUM(CASE WHEN email_cat = 'email_clicked' THEN 1 ELSE 0 END) / 
        SUM(CASE WHEN email_cat = 'email_sent' THEN 1 ELSE 0 END) AS email_click_rate
FROM (
    SELECT
        CASE
            WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
            WHEN action = 'email_open' THEN 'email_open'
            WHEN action = 'email_clickthrough' THEN 'email_clicked'
        END AS email_cat
    FROM emailEvents
) AS sub;

