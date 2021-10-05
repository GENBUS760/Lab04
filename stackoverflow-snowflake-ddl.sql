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

-- to test, we will print the first few rows
-- of each of the 8 tables in our database
select * from posts limit 5;
select * from users limit 5;
select * from votes limit 5;
select * from badges limit 5;
select * from postlinks limit 5;
select * from posttypes limit 5;
select * from votetypes limit 5;
select * from linktypes limit 5;
