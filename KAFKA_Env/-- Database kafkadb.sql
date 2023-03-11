-- Database: kafkadb

-- DROP DATABASE IF EXISTS kafkadb;

CREATE TABLE global (
	id SERIAL PRIMARY KEY,
	new_confirmed INT,
	total_confirmed INT,
	new_deaths INT,
	total_deaths INT,
	new_recovered INT,
	total_recovered INT
);

CREATE TABLE countries (
	id SERIAL PRIMARY KEY,
	country CHAR(255),
	country_code CHAR(2),
	slug CHAR(255),
	new_confirmed INT,
	total_confirmed INT,
	new_deaths INT,
	total_deaths INT,
	new_recovered INT,
	total_recovered INT,
	date_maj CHAR(255)
);

CREATE TABLE summary (
	id SERIAL PRIMARY KEY,
	global_id INT REFERENCES global(id),
	country_id INT REFERENCES countries(id),
	date CHAR(255)
);
