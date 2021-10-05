-- first create the database we need
create or replace database stackoverflow2010;

-- then set our context
use role sysadmin;
use warehouse compute_wh;
use database stackoverflow2010;
use schema public;

-- now create a new stage to load the stackoverflow data
create or replace stage stackoverflow_data url = 's3://genbus760/stackoverflow2010';

-- let's look at what is in this stage
-- you should see 8 csv.gz files
-- these are compressed CSV files
list @stackoverflow_data;

-- now we need to create 8 tables to store this data
create or replace table posts  
(Id integer,
AcceptedAnswerId integer,
AnswerCount integer,
ClosedDate timestamp,
CommentCount integer,
CommunityOwnedDate timestamp,
CreationDate timestamp,
FavoriteCount integer,
LastActivityDate timestamp,
LastEditDate timestamp,
LastEditorUserId integer,
OwnerUserId integer,
ParentId integer,
PostTypeId integer,
Score integer,
Tags string,
ViewCount integer);

create or replace table users  
(Id string,
Age string,
CreationDate timestamp,
DownVotes integer,
LastAccessDate timestamp,
Reputation integer,
UpVotes integer,
Views integer,
AccountId integer);

create or replace table badges  
(Id integer,
Name string,
UserId integer,
Date timestamp);

create or replace table votes
(Id	integer,
PostId	integer,
UserId integer,
BountyAmount integer,
VoteTypeId	integer,
CreationDate timestamp);

create or replace table postlinks  
(Id integer,
CreationDate timestamp,
PostId integer,
RelatedPostId integer,
LinkTypeId integer);

create or replace table votetypes  
(Id integer,
Name string);

create or replace table posttypes  
(Id integer,
Type string);

create or replace table linktypes  
(Id integer,
Type string);

-- we have completed creating all the tables we need
-- now we will load the data into our tables
copy into posts
from @stackoverflow_data/posts.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into users
from @stackoverflow_data/users.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into votes
from @stackoverflow_data/votes.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into badges
from @stackoverflow_data/badges.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into postlinks
from @stackoverflow_data/postlinks.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into votetypes
from @stackoverflow_data/votetypes.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into posttypes
from @stackoverflow_data/posttypes.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

copy into linktypes
from @stackoverflow_data/linktypes.csv.gz
file_format = (
type=csv
skip_header=1
field_delimiter="\t"
record_delimiter="\n"
empty_field_as_null=true)
on_error = "continue";

-- DESCRIBE
describe table posts;
describe stage stackoverflow_data;

-- General SnowSQL Query Structure
-- Syntax: https://docs.snowflake.com/en/sql-reference/constructs.html
-- Operators: https://docs.snowflake.com/en/sql-reference/operators.html

-- SELECT, Column Aliases
-- https://docs.snowflake.com/en/sql-reference/sql/select.html
--
-- Note: Aliases and identifiers are case-insensitive by default.
-- To preserve case, enclose them within double quotes (").
--
-- Note: Without an ORDER BY clause, the results returned by SELECT are an unordered set.
-- Running the same query repeatedly against the same tables might result in a different
-- output order every time. If order matters, use the ORDER BY clause.
select 1 + 2;
select getdate();
select pi() * 2.0 * 2.0 as area_of_circle;
select 'Emaad' as FirstName, getdate() as CurrentDate;

-- Try: Write a query to print your fist name, last name,
-- and @wisc.edu email as 3 separate columns

-- SELECT FROM
-- https://docs.snowflake.com/en/sql-reference/constructs/from.html
select Id, Reputation from users;
select Id, CreationDate from posts;
select Id, CreationDate from posts p;

-- SELECT DISTINCT FROM
-- https://docs.snowflake.com/en/sql-reference/sql/select.html
select OwnerUserId from posts;
select distinct OwnerUserId from posts;

-- SELECT FROM LIMIT
select * from posts limit 5;
select * from users limit 5;
select * from votes limit 5;
select * from badges limit 5;
select * from postlinks limit 5;
select * from posttypes limit 5;
select * from votetypes limit 5;
select * from linktypes limit 5;

-- SELECT FROM ORDER BY
-- https://docs.snowflake.com/en/sql-reference/constructs/order-by.html
select * from posts order by creationdate desc limit 10;
select * from posts order by creationdate, answercount limit 10;
select tags from posts order by len(tags) desc;

select Id, tags, len(tags) from posts
    order by len(tags) desc
    nulls last
    limit 5;
-- https://stackoverflow.com/questions/131165
    
-- https://docs.snowflake.com/en/sql-reference/functions/regexp_count.html
select Id, tags, regexp_count(tags, '<') as num_tags from posts
    order by num_tags desc
    nulls last
    limit 5;

-- https://docs.snowflake.com/en/sql-reference/functions/nvl.html    
-- Expression and column should have the same data type
select nvl(to_varchar(LastEditDate), 'Never Edited') from posts;

-- SELECT FROM WHERE
-- https://docs.snowflake.com/en/sql-reference/constructs/where.html
-- https://docs.snowflake.com/en/sql-reference/operators-logical.html
--
-- Use care when creating expressions that might evaluate NULLs.
--
-- In most contexts, the boolean expression NULL = NULL returns NULL, not TRUE.
-- In a WHERE clause, if an expression evaluates to NULL, the row for that expression
-- is removed from the result set (i.e. it is filtered out).
select * from users where reputation > 10000;
select * from posts where commentcount > answercount;
select * from posts where creationdate > '2010-01-01';
select * from users where reputation between 10000 and 15000;
select * from posts where commentcount > answercount and creationdate < '2010-01-01';
select * from posts where commentcount > answercount
    or (creationdate > '2005-01-01' and creationdate < '2010-01-01');
    
-- https://docs.snowflake.com/en/sql-reference/functions/like.html
-- SQL wildcards are supported in pattern:
--    An underscore (_) matches any single character.
--    A percent sign (%) matches any sequence of zero or more characters.
select * from posts where tags = '<sql>';
select * from posts where tags like '%sql%';
    
-- Try: Write a query to print the posts that were
-- created in November, 2009 (the entire month)

-- SQL FUNCTION date_part
-- https://docs.snowflake.com/en/sql-reference/functions/date_part.html
select date_part(month, creationdate) from posts;

-- Try: Redo the previous query using the date_part function

-- Try: Write a query to print the posts that were created
-- in November, 2009 and tagged with <sql> (may include other tags)

-- Try: Which StackOverflow user had the highest reputation in this
-- dataset? Hint: First find this user's ID using SQL, then go to their
-- StackOverflow profile at the link:
--     https://stackoverflow.com/users/ID YOU FOUND USING SQL (replace)

select count(*) from posts where LastEditDate is not null;

-- SELECT FROM GROUPBY AND AGGREGATION
-- https://docs.snowflake.com/en/sql-reference/constructs/group-by.html
select OwnerUserId as Id, max(CreationDate) as FirstPostDate
    from posts
    group by OwnerUserId
    order by FirstPostDate;

-- Try: Who created the first ever post on StackOverflow?
-- Hint: First find this user's ID using SQL, then go to their
-- StackOverflow profile at the link:
--     https://stackoverflow.com/users/ID YOU FOUND USING SQL (replace)

select count(*) as number from posts where tags like '%sql%';

select count(*) as number, tags from posts
    group by tags
    order by number desc
    limit 5;

-- https://docs.snowflake.com/en/sql-reference/functions/monthname.html
select monthname(CreationDate) as month,
    count(*) as number_of_users
    from users
    group by month;
    
select tags, count(*) as number_of_questions
    from posts
    where tags like '%sql%' and PostTypeId = 1
    group by tags
    order by number_of_questions desc
    limit 10;

select tags, sum(AnswerCount) as number_of_answers
    from posts
    where tags like '%sql%' and PostTypeId = 1
    group by tags
    order by number_of_answers desc
    limit 10;
    
select tags, sum(AnswerCount)/count(*) as number_of_ans_per_qn
    from posts
    where tags like '%sql%'
    group by tags
    order by number_of_ans_per_qn desc
    limit 10;
    
-- https://docs.snowflake.com/en/sql-reference/functions/div0.html
select tags, div0(sum(AnswerCount), count(*)) as number_of_ans_per_qn
    from posts
    where tags like '%sql%'
    group by tags
    order by number_of_ans_per_qn desc
    limit 10;
    
-- https://docs.snowflake.com/en/sql-reference/functions/round.html
-- https://docs.snowflake.com/en/sql-reference/functions/ceil.html
-- https://docs.snowflake.com/en/sql-reference/functions/floor.html
select tags, round(sum(AnswerCount)/count(*), 0) as number_of_ans_per_qn
    from posts
    where tags like '%sql%'
    group by tags
    order by number_of_ans_per_qn desc
    limit 10;

select OwnerUserId, monthname(CreationDate) as mon, count(*)
    from posts
    where PostTypeId = 1
    group by OwnerUserId, mon
    order by OwnerUserId;

-- https://docs.snowflake.com/en/sql-reference/functions/nvl2.html
-- All three expressions should have the same (or compatible) data type.
select nvl2(to_varchar(LastEditDate), 'Edited', 'Never Edited') as EditStatus, count(*)
    from posts
    group by EditStatus;

-- SELECT FROM GROUPBY HAVING

select tags, sum(AnswerCount)/count(*) as number_of_ans_per_qn
    from posts
    where tags like '%sql%'
    group by tags
    having count(*) > 10
    order by number_of_ans_per_qn desc
    limit 10;

select OwnerUserId, round(nvl(avg(regexp_count(tags, '<')), 0), 2) as avg_num_tags
    from posts
    where PostTypeId = 1
    group by OwnerUserId
    having count(*) > 50
    order by avg_num_tags desc
    limit 10;
    
-- CHALLENGE LAB 4 [5 points]:
--
-- Submit your working queries on Canvas
-- by copy-pasting them into the Canvas textbox
--
-- Also submit query results by clicking "Copy" in
-- the results panel below, and then pasting the
-- results into the Canvas textbox
--
-- Write a single query for each question
--
-- Example submission (notice the results after each,
-- query are followed by an empty line):
--
-- select id, answercount from posts limit 1;
-- ID	ANSWERCOUNT
-- 498939	0
--
-- select id, reputation from users limit 1;
-- ID	REPUTATION
-- -1	1
--
-- Q1. [1 point] How many questions in the
-- dataset have accepted answers?
--
-- Hint 1: When a question has no accepted answer,
-- AcceptedAnswerId = 0 in the posts table
--
-- Hint 2: The posts table contains both questions
-- and non-questions (i.e. answers, wikis, etc.).
-- For questions, PostTypeId = 1 (see the PostTypes
-- table for the PostTypeIDs of other post types)
--
-- Expected result (column name might vary): 
--
-- NUMBER
-- 791102
--
-- Q2. [2 points] How many questions with the
-- <sql> tag (among other tags) are posted in each year?
--
-- Hint 1: Recall the date_part SQL function
-- https://docs.snowflake.com/en/sql-reference/functions/date_part.html
--
-- Hint 2: Use group by, count(*), like
--
-- Hint 3: Don't forget that questions have PostTypeId=1
--
-- Expected result (order of rows and column names might vary):
--
-- YEAR	COUNT
-- 2010	56951
-- 2009	31642
-- 2008	5446 
--
-- Q3. [2 points] On what day of the week are
-- most questions posted?
--
-- Hint 1: Use the dayname SnowSQL function
-- https://docs.snowflake.com/en/sql-reference/functions/dayname.html
--
-- Hint 2: Use group by, count(*), order by, and limit
--
-- Hint 3: Don't forget that questions have PostTypeId=1
--
-- Expected result (column names may vary):
--
-- DAY	NUMBER
-- Wed	195901
