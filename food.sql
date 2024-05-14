drop table load_food;

CREATE TABLE load_food (
	id BIGINT Primary Key,
	business_name VARCHAR(255),
	dbanname VARCHAR(255),
	legalowner VARCHAR(255),
	namelast VARCHAR(255),
	namefirst VARCHAR(255),
	licenseNO INT,
	issdttm Date,
	expdttm Date,
	licstatus VARCHAR(30),
	licsesecat VARCHAR(10),
	descript VARCHAR(60),
	result VARCHAR(30),
	resultdttm timestamp,
	violation VARCHAR(60),
	violevels Varchar(10),
	viodesc VARCHAR(255),
	viodttm DATE,
	viostatus VARCHAR(20),
	status_date DATE,
	comments VARCHAR(5000),
	address VARCHAR(255),
	city VARCHAR(60),
	state VARCHAR(10),
	zip varchar(100),
	property_id BIGINT,
	lat float,
	lon float,
	lat_prev float,
	lon_prev float,
	start_date date,
	end_date date
);

-- \copy load_food FROM '/Users/weixuanhuang/Desktop/CS 779/final/Bos_food.csv' DELIMITER ',' CSV HEADER;

-- check
select * from load_food;



--- dimensional tables
drop table License;

create table License(
	license_id serial primary key,
	license_no int,
	issue_date date,
	expire_date date,
	license_status varchar(50),
	cattype varchar(10),
	description varchar(60),
	start_date date,
	end_date date,
	is_current bool
);


drop table Time;
CREATE TABLE Time (
    time_id serial primary key,
	full_time timestamp,
    time_year int,
    time_month int,
    time_day int,
	time_clocktime time
);


drop table Ban;
create table Ban(
	ban_id serial primary key,
	ban_name varchar(66)
);

drop table Location;
create table Location(
	location_id serial primary key,
	address varchar(255),
	city varchar(60),
	state varchar(6),
	zip varchar(100),
	property_id BIGINT,
	lat float,
	lat_prev float,
	lon float,
	lon_prev float
);


drop table Violation;
create table Violation(
	violation_id serial primary key,
	code varchar(60),
	level varchar(10),
	description varchar(255),
	vio_date date,
	status varchar(20),
	status_date date,
	comments varchar(5000)
);

drop table Business;
create table Business(
	business_id serial primary key,
	business_name varchar(255)
);

drop table Owner;
create table Owner(
	owner_id serial primary key,
	legal_owner varchar(255),
	namelast varchar(255),
	namefirst varchar(255)
);




drop table Result;
create table Result(
	result_id serial primary key,
	license_id BIGINT,
    ban_id BIGINT,
    location_id BIGINT,
    violation_id BIGINT,
    business_id BIGINT,
    owner_id BIGINT,
	time_id BIGINT,
    status varchar(60),
    result_date date,
    result_count bigint,
	foreign key (license_id) references License(license_id),
	foreign key (ban_id) references ban(ban_id),
	foreign key (location_id) references Location(location_id),
	foreign key (violation_id) references Violation(violation_id),
	foreign key (business_id) references Business(business_id),
	foreign key (owner_id) references Owner(owner_id),
	foreign key (time_id) references Time(time_id)
);


----- handle data here

-- I handled by python


------------------------insert

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(1, 313440, '2017-08-14', '2020-01-01', 'Inactive', 'FS', 'Eating & Drinking', '2020-01-01', '2011-12-31', False);

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(2, 34789, '2011-11-07', '2012-01-01', 'Active', 'FS', 'Eating & Drinking', '2012-01-01', '2024-12-31', True);

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(3, 34789, '2023-08-24', '2025-01-01', 'Active', 'FT', 'Eating & Drinking w/ Take Out', '2025-01-01', '2026-12-31', True);

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(4, 34789, '2023-08-24', '2025-01-01', 'Active', 'FS', 'Eating & Drinking', '2025-01-01', '2026-12-31', True);

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(5, 34789, '2024-08-24', '2027-01-01', 'Active', 'FS', 'Eating & Drinking', '2027-01-01', '2027-03-22', True);

-- insert into license (license_id, license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- values(6, 34789, '2026-03-22', '2027-03-23', 'Active', 'FS', 'Eating & Drinking', '2027-03-23', '9999-03-22', True);


drop table cp_License;

create table cp_License(
	license_id serial primary key,
	license_no int,
	issue_date date,
	expire_date date,
	license_status varchar(50),
	cattype varchar(10),
	description varchar(60)
);
insert into cp_License (license_no, issue_date, expire_date, license_status, cattype, description)
select distinct
    licenseno,  
    issdttm,
    expdttm,
    licstatus,
    licsesecat,  
    descript 
from load_food;

select * from cp_License;


MERGE INTO License l
USING (
    SELECT
        license_no,
        issue_date,
        expire_date,
        license_status,
        cattype,
        description,
        expire_date AS start_date,
        COALESCE(LEAD(expire_date) OVER (ORDER BY license_id) - INTERVAL '1 day', '9999-12-31') AS end_date,
        CASE
            WHEN COALESCE(LEAD(expire_date) OVER (ORDER BY license_id) - INTERVAL '1 day', '9999-12-31') > CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM cp_License
) AS t
ON t.license_no = l.license_no
WHEN MATCHED THEN
    UPDATE SET
        issue_date = t.issue_date,
        expire_date = t.expire_date,
        license_status = t.license_status,
        cattype = t.cattype,
        description = t.description,
        start_date = t.start_date,
        end_date = t.end_date,
        is_current = t.is_current
WHEN NOT MATCHED THEN
    INSERT (license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
    VALUES (t.license_no, t.issue_date, t.expire_date, t.license_status, t.cattype, t.description, t.start_date, t.end_date, t.is_current);


-- INSERT INTO License (license_no, issue_date, expire_date, license_status, cattype, description, start_date, end_date, is_current)
-- SELECT 
--     license_no,
--     issue_date,
--     expire_date,
--     license_status,
--     cattype,
--     description,
--     expire_date AS start_date,
--     COALESCE(LEAD(expire_date) OVER (ORDER BY license_id) - INTERVAL '1 day', '9999-12-31') AS end_date,
--     CASE 
--         WHEN COALESCE(LEAD(expire_date) OVER (ORDER BY license_id) - INTERVAL '1 day', '9999-12-31') > CURRENT_DATE THEN TRUE 
--         ELSE FALSE 
--     END AS is_current
-- from cp_License;



select * from cp_License;
select * from License ;
select * from load_food;


insert into Time (full_time, time_year, time_month, time_day, time_clocktime)
select distinct
	resultdttm,
    extract(year from resultdttm) as time_year,
    extract(month from resultdttm) as time_month,
    extract(day from resultdttm) as time_day,
	CAST(resultdttm AS TIME) AS time_clocktime
from
    load_food;

select * from Time;
	


-- insert into Ban(ban_name)
-- values(NULL);

-- insert into Ban(ban_name)
-- values('Baggage Claim');

-- insert into Ban(ban_name)
-- values('Upper Level/Main Food Court');

-- insert into Ban(ban_name)
-- values('#477');

-- insert into Ban(ban_name)
-- values('1844 Inc');


insert into Ban(ban_name)
select distinct
	dbanname
from load_food;

select * from Ban;



--locatiion
drop table cp_Location;

create table cp_Location(
	location_id serial primary key,
	address varchar(255),
	city varchar(60),
	state varchar(6),
	zip varchar(100),
	property_id BIGINT,
	lat float,
	lon float
);

insert into cp_Location(address, city, state, zip, property_id, lat, lon)
select distinct
address, 
city, 
state,
zip,
property_id, 
lat,
lon
from load_food;

select * from cp_Location;
select * from cp_Location where address = '55  COURT ST';


MERGE INTO Location l
USING (
    SELECT
        address,
        city,
        state,
        zip,
        property_id,
        lat,
        COALESCE(LAG(lat) OVER (ORDER BY location_id), lat) AS lat_prev,
        lon,
        COALESCE(LAG(lon) OVER (ORDER BY location_id), lon) AS lon_prev
    FROM cp_Location
) AS t
ON l.property_id = t.property_id
WHEN MATCHED THEN
    UPDATE SET
        address = t.address,
        city = t.city,
        state = t.state,
        zip = t.zip,
        lat = t.lat,
        lat_prev = CASE
            WHEN l.lat <> t.lat THEN l.lat
            ELSE l.lat_prev
        END,
        lon = t.lon,
        lon_prev = CASE
            WHEN l.lon <> t.lon THEN l.lon
            ELSE l.lon_prev
        END
WHEN NOT MATCHED THEN
    INSERT (address, city, state, zip, property_id, lat, lat_prev, lon, lon_prev)
    VALUES (t.address, t.city, t.state, t.zip, t.property_id, t.lat, t.lat_prev, t.lon, t.lon_prev);


-- insert into Location(address, city, state, zip, property_id, lat, lat_prev, lon, lon_prev)
-- select 
-- address,
-- city,
-- state,
-- zip,
-- property_id,
-- lat,
-- coalesce(lag(lat) over (order by location_id), lat) as lat_prev, 
-- lon,
-- coalesce(lag(lon) over (order by location_id), lon) as lon_prev
-- from cp_Location;



select * from Location;

select * from Location where address = '55  COURT ST';


-- violation
insert into Violation(code, level, description, vio_date, status, status_date, comments)
select
	violation,
	violevels,
	viodesc,
	viodttm,
	viostatus,
	status_date,
	comments
from load_food;

select * from Violation;
select * from Violation where code = '15-4-202.16';


insert into Business(business_name)
select
Distinct business_name
from load_food;

-- checking
select * from Business where business_name = '1000 Degrees Pizza';
select * from Business;



insert into Owner(legal_owner, namelast, namefirst)
select
Distinct legalowner,
namelast,
namefirst
from load_food;
select * from Owner;

-- checking
select * from Owner where legal_owner = 'TERADYNE INC';

select * from load_food where legalowner = 'TERADYNE INC';



-- insert into Result(license_id, ban_id, location_id, violation_id, business_id, owner_id, status, result_date, result_count)
-- select 
-- 	license_id, 
-- 	ban_id, 
-- 	location_id, 
-- 	violation_id, 
-- 	business_id, 
-- 	owner_id, 
-- 	status, 
-- 	result_date, 
-- 	result_count
-- from result
-- join 

-- explain
INSERT INTO Result (license_id, ban_id, location_id, violation_id, business_id, owner_id, time_id, status, result_date, result_count)
select
l.license_id,
b.ban_id,
loc.location_id,
v.violation_id,
bus.business_id,
o.owner_id,
t.time_id,
lf.result,
lf.resultdttm, 
COUNT(*) AS result_count -- each combination has several inspection_result
from 
load_food lf
inner join License l on l.license_no = lf.licenseno
inner join Ban b on b.ban_name = lf.dbanname
inner join Location loc on loc.address = lf.address
inner join Violation v on v.code = lf.violation
inner join Business bus on bus.business_name = lf.business_name
inner join Owner o on o.namelast = lf.namelast and o.namefirst = lf.namefirst
inner join Time t on t.full_time = lf.resultdttm
group by l.license_id, b.ban_id, loc.location_id, v.violation_id, bus.business_id, o.owner_id, t.time_id, lf.result, lf.resultdttm
limit 10000;



select * from Result;
select * from Time;

