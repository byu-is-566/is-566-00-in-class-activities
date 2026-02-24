/* =========================================================
   Semi-Structured Query Practice
   Table: car_sales
   VARIANT column: src
   ========================================================= */

use database IS566;
use schema semi_structured;


/* =========================================================
   #1 Sale Summary
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - salesperson_name
   - customer_name (first customer)
   - vehicle_make, vehicle_model, vehicle_year (first vehicle)
   - vehicle_price (as NUMBER)

   Order by sale_date, dealership.
   ========================================================= */




/* =========================================================
   #2 Multi-Customer / Multi-Vehicle Detection
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - num_customers
   - num_vehicles
   - multi_customer_flag  (Y if more than 1 customer, else N)
   - multi_vehicle_flag   (Y if more than 1 vehicle, else N)

   Order by sale_date.
   ========================================================= */




/* =========================================================
   #3 High-Value Sales
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - vehicle_make
   - vehicle_model
   - vehicle_price
   - high_value_flag  (Y if price >= 40000, else N)

   Only include sales where vehicle_year >= 2019.

   Order by vehicle_price descending.
   ========================================================= */




/* =========================================================
   #4 One Row Per Vehicle (LATERAL FLATTEN)
   ---------------------------------------------------------
   Some sales contain multiple vehicles in the vehicle array.

   Return ONE row per vehicle with:

   - sale_date
   - dealership
   - salesperson_name
   - vehicle_make
   - vehicle_model
   - vehicle_year
   - vehicle_price

   Order by sale_date, dealership, vehicle_make.
   ========================================================= */






























































/* =========================================================
   #1 Sale Summary
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - salesperson_name
   - customer_name (first customer)
   - vehicle_make, vehicle_model, vehicle_year (first vehicle)
   - vehicle_price (as NUMBER)

   Order by sale_date, dealership.
   ========================================================= */

SELECT
  src:date::DATE                     AS sale_date,
  src:dealership::STRING             AS dealership,
  src:salesperson.name::STRING       AS salesperson_name,
  src:customer[0].name::STRING       AS customer_name,
  src:vehicle[0].make::STRING        AS vehicle_make,
  src:vehicle[0].model::STRING       AS vehicle_model,
  src:vehicle[0].year::INT           AS vehicle_year,
  src:vehicle[0].price::NUMBER(12,0) AS vehicle_price
FROM car_sales
ORDER BY sale_date, dealership;



/* =========================================================
   #2 Multi-Customer / Multi-Vehicle Detection
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - num_customers
   - num_vehicles
   - multi_customer_flag  (Y if more than 1 customer, else N)
   - multi_vehicle_flag   (Y if more than 1 vehicle, else N)

   Order by sale_date.
   ========================================================= */

SELECT
  src:date::DATE AS sale_date,
  src:dealership::STRING AS dealership,
  ARRAY_SIZE(src:customer) AS num_customers,
  ARRAY_SIZE(src:vehicle)  AS num_vehicles,
  IFF(ARRAY_SIZE(src:customer) > 1, 'Y', 'N') AS multi_customer_flag,
  IFF(ARRAY_SIZE(src:vehicle)  > 1, 'Y', 'N') AS multi_vehicle_flag
FROM car_sales
ORDER BY sale_date;



/* =========================================================
   #3 High-Value Sales
   ---------------------------------------------------------
   Return ONE row per sale with:

   - sale_date
   - dealership
   - vehicle_make
   - vehicle_model
   - vehicle_price
   - high_value_flag  (Y if price >= 40000, else N)

   Only include sales where vehicle_year >= 2019.

   Order by vehicle_price descending.
   ========================================================= */


SELECT
  src:date::DATE                     AS sale_date,
  src:dealership::STRING             AS dealership,
  src:vehicle[0].make::STRING        AS vehicle_make,
  src:vehicle[0].model::STRING       AS vehicle_model,
  src:vehicle[0].price::NUMBER(12,0) AS vehicle_price,
  IFF(src:vehicle[0].price::NUMBER(12,0) >= 40000, 'Y', 'N') AS high_value_flag
FROM car_sales
WHERE src:vehicle[0].year::INT >= 2019
ORDER BY vehicle_price DESC;



/* =========================================================
   #4 One Row Per Vehicle (LATERAL FLATTEN)
   ---------------------------------------------------------
   Some sales contain multiple vehicles in the vehicle array.

   Return ONE row per vehicle with:

   - sale_date
   - dealership
   - salesperson_name
   - vehicle_make
   - vehicle_model
   - vehicle_year
   - vehicle_price

   Order by sale_date, dealership, vehicle_make.
   ========================================================= */

SELECT
  cs.src:date::DATE               AS sale_date,
  cs.src:dealership::STRING       AS dealership,
  cs.src:salesperson.name::STRING AS salesperson_name,
  v.value:make::STRING            AS vehicle_make,
  v.value:model::STRING           AS vehicle_model,
  v.value:year::INT               AS vehicle_year,
  v.value:price::NUMBER(12,0)     AS vehicle_price
FROM car_sales cs,
LATERAL FLATTEN(input => cs.src:vehicle) v
ORDER BY sale_date, dealership, vehicle_make;