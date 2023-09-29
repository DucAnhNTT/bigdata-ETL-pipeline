DROP TABLE if exists d_date;

CREATE TABLE d_date
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE public.d_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX d_date_date_actual_idx
  ON d_date(date_actual);

COMMIT;

INSERT INTO d_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;




-- Create the staff_dimension table
CREATE TABLE IF NOT EXISTS staff_dimension (
    staff_id smallint PRIMARY KEY,
    first_name varchar(45),
    last_name varchar(45),
    email varchar(50),
    username varchar(16)
);

-- Insert data into the staff_dimension table
INSERT INTO staff_dimension (staff_id, first_name, last_name, email, username)
SELECT DISTINCT
    s.staff_id,
    s.first_name,
    s.last_name,
    s.email,
    s.username
FROM
    staff s;

-- Create the customer_dimension table if it doesn't exist
CREATE TABLE IF NOT EXISTS customer_dimension (
    customer_id smallint PRIMARY KEY,
    first_name varchar(45),
    last_name varchar(45),
    email varchar(50),
    address varchar(50),
    city varchar(50),
    country varchar(50),
    create_date date,
    last_update timestamp
);
CREATE TABLE IF NOT EXISTS film_dimension (
    film_id smallint PRIMARY KEY,
    title varchar(255),
    description text,
    release_year smallint,
    language varchar(20),
    rental_duration smallint,
    rental_rate numeric(4,2),
    length smallint,
    replacement_cost numeric(5,2),
    rating varchar(10),
    special_features varchar(255),
    last_update timestamp
);
CREATE TABLE IF NOT EXISTS store_dimension (
    store_id INT PRIMARY KEY,
    manager_staff_id INT,
    address VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50)
);

INSERT INTO store_dimension (store_id, manager_staff_id, address, city, country)
SELECT distinct
    s.store_id,
    s.manager_staff_id,
    a.address,
    city.city,
    country.country
FROM
    public.store AS s
    JOIN public.address AS a ON s.address_id = a.address_id
    JOIN public.city AS city ON a.city_id = city.city_id
    JOIN public.country AS country ON city.country_id = country.country_id;

INSERT INTO film_dimension (film_id, title, description, release_year, language, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update)
SELECT distinct
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    l.name AS language,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    f.special_features,
    f.last_update
FROM
    film f
JOIN
    language l ON f.language_id = l.language_id;

-- Insert data into the customer_dimension table
INSERT INTO customer_dimension (customer_id, first_name, last_name, email, address, city, country, create_date, last_update)
SELECT DISTINCT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    a.address,
    ci.city,
    co.country,
    c.create_date::date, -- Ensure create_date is of type date
    c.last_update
FROM
    customer c
JOIN
    address a ON c.address_id = a.address_id
JOIN
    city ci ON a.city_id = ci.city_id
JOIN
    country co ON ci.country_id = co.country_id;
	


CREATE TABLE IF NOT EXISTS sales_fact (
    sales_id serial PRIMARY KEY,
    date_dim_id int NOT NULL,
    customer_id smallint NOT NULL,
    store_id smallint NOT NULL,
    staff_id smallint NOT NULL,
    film_id smallint NOT NULL,
    quantity smallint NOT NULL,
    amount numeric(5, 2),
    CONSTRAINT sales_fact_date_dim_id_fkey FOREIGN KEY (date_dim_id) REFERENCES d_date (date_dim_id),
    CONSTRAINT sales_fact_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customer_dimension (customer_id),
    CONSTRAINT sales_fact_store_id_fkey FOREIGN KEY (store_id) REFERENCES store_dimension (store_id),
    CONSTRAINT sales_fact_film_id_fkey FOREIGN KEY (film_id) REFERENCES film_dimension (film_id),
	CONSTRAINT sales_fact_staff_id_fkey FOREIGN KEY (staff_id) REFERENCES staff_dimension (staff_id)
);


INSERT INTO public.sales_fact(
    date_dim_id, customer_id, store_id, staff_id, film_id, quantity, amount)
SELECT
    TO_CHAR(r.rental_date, 'YYYYMMDD')::int as date_dim_id,
    p.customer_id,
    st.store_id,
    s.staff_id,
    i.film_id,
    COUNT(r.rental_id) AS quantity,
    SUM(f.rental_rate) AS amount
FROM
    payment p
    JOIN rental r ON p.rental_id = r.rental_id
    JOIN staff s ON r.staff_id = s.staff_id
    JOIN store st ON s.store_id = st.store_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film_dimension f ON i.film_id = f.film_id
GROUP BY
    date_dim_id,
    p.customer_id,
    st.store_id,
    s.staff_id,
    i.film_id;



    



-- If it show this line or same, congrats! let's move on
-- NOTICE:  table "d_date" does not exist, skipping
-- WARNING:  there is no transaction in progress
-- WARNING:  there is no transaction in progress
-- INSERT 0 14594

-- Query returned successfully in 4 secs 126 msec.