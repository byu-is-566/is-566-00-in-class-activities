-- If needed, you can set your context:
-- use role [your role];
-- use warehouse [your warehouse];
use database IS566;
use schema sql_practice;
use warehouse ANIMAL_TASK_WH;


-----------||     In-Class Exercises: Introducing CTEs     ||--------------------------

-- Exercise 1: Baseline (No CTEs)
--
-- Task:
-- For each order, compute the total quantity of items ordered.
--
-- Requirements:
--   • Use the LINEITEM table
--   • Group by L_ORDERKEY
--
-- Display the following columns:
--   L_ORDERKEY,
--   Total_Quantity   -- sum of L_QUANTITY for that order
--
-- Order the results by L_ORDERKEY.


SELECT
  L_ORDERKEY,
  SUM(L_QUANTITY) AS Total_Quantity
FROM LineItem
GROUP BY L_ORDERKEY
ORDER BY L_ORDERKEY;


-- Exercise 2: The same query, but this time as a CTE
--
-- Task:
-- Rewrite Exercise 1 using a CTE called Order_Quantities.
--
-- The CTE should compute:
--   L_ORDERKEY,
--   Total_Quantity
--
-- Then select from the CTE to display the same result as Exercise 1.
--
-- Order the final output by L_ORDERKEY.


WITH Order_Quantities AS (
  SELECT
    L_ORDERKEY,
    SUM(L_QUANTITY) AS Total_Quantity
  FROM LineItem
  GROUP BY L_ORDERKEY
)
SELECT
  L_ORDERKEY,
  Total_Quantity
FROM Order_Quantities
ORDER BY L_ORDERKEY;


-- Exercise 3: Using a CTE to Simplify a Join
--
-- Task:
-- Starting from the Order_Quantities CTE from Exercise 2, join it to the ORDERS table
-- to display order-level details.
--
-- Display the following columns:
--   O_ORDERKEY,
--   O_ORDERDATE,
--   O_TOTALPRICE,
--   Total_Quantity
--
-- Order the results by O_TOTALPRICE descending.

WITH Order_Quantities AS (
  SELECT
    L_ORDERKEY,
    SUM(L_QUANTITY) AS Total_Quantity
  FROM LineItem
  GROUP BY L_ORDERKEY
)
SELECT
  o.O_ORDERKEY,
  o.O_ORDERDATE,
  o.O_TOTALPRICE,
  oq.Total_Quantity
FROM Orders o
JOIN Order_Quantities oq
  ON oq.L_ORDERKEY = o.O_ORDERKEY
ORDER BY o.O_TOTALPRICE DESC;


-- Exercise 4: Medium Complexity (CTEs as Building Blocks)
--
-- Goal: Show how CTEs support layered reasoning.
--
-- Task:
-- Identify customers who tend to place large orders.
--
-- Steps:
--   1) Use a CTE (Order_Quantities) to compute Total_Quantity per order.
--   2) Join this CTE to ORDERS and CUSTOMER.
--   3) For each customer, compute:
--        • Avg_Order_Quantity = average Total_Quantity across their orders
--        • Order_Count        = number of orders placed
--
-- Display the following columns:
--   C_CUSTKEY,
--   C_FIRSTNAME,
--   C_LASTNAME,
--   Avg_Order_Quantity,
--   Order_Count
--
-- Filter to customers with more than 5 orders.
-- Order the results by Avg_Order_Quantity descending.

WITH Order_Quantities AS (
  SELECT
    L_ORDERKEY,
    SUM(L_QUANTITY) AS Total_Quantity
  FROM LineItem
  GROUP BY L_ORDERKEY
),
Customer_Order_Summary AS (
  SELECT
    c.C_CUSTKEY,
    c.C_FIRSTNAME,
    c.C_LASTNAME,
    AVG(oq.Total_Quantity) AS Avg_Order_Quantity,
    COUNT(*) AS Order_Count
  FROM Customer c
  JOIN Orders o
    ON o.O_CUSTKEY = c.C_CUSTKEY
  JOIN Order_Quantities oq
    ON oq.L_ORDERKEY = o.O_ORDERKEY
  GROUP BY
    c.C_CUSTKEY,
    c.C_FIRSTNAME,
    c.C_LASTNAME
)
SELECT
  C_CUSTKEY,
  C_FIRSTNAME,
  C_LASTNAME,
  Avg_Order_Quantity,
  Order_Count
FROM Customer_Order_Summary
WHERE Order_Count > 5;


-- Exercise 5: Multiple CTEs instead of a complex subquery
--
-- Goal: Use several small CTEs as “building blocks” to answer a question that would
-- otherwise require a nested subquery (or a window function).
--
-- Task:
-- Identify “top customers” within each region based on total spending.
--
-- Definitions:
--   • Customer_Total_Spent = sum of O_TOTALPRICE across all orders for that customer
--   • A customer is considered a “top customer” in their region if their total spending
--     is greater than the average Customer_Total_Spent among customers in that same region.
--
-- CTE guidance:
--   1) Create a CTE (Customer_Regions) that maps each customer to their region:
--        CUSTOMER -> NATION -> REGION
--   2) Create a CTE (Customer_Spend) that computes Customer_Total_Spent per customer:
--        ORDERS grouped by O_CUSTKEY
--   3) Create a CTE (Customer_Spend_By_Region) that combines (1) and (2) so each row has:
--        customer info + region name + Customer_Total_Spent
--   4) Create a CTE (Region_Avg_Spend) that computes the average Customer_Total_Spent per region.
--   5) Join (3) to (4) and filter to customers whose Customer_Total_Spent is greater than their
--      region’s average.
--
-- Display the following columns:
--   R_NAME,
--   C_CUSTKEY,
--   C_FIRSTNAME,
--   C_LASTNAME,
--   Customer_Total_Spent,
--   Region_Avg_Spend
--
-- Order the results by R_NAME, then Customer_Total_Spent descending.


WITH Customer_Regions AS (
  SELECT
    c.C_CUSTKEY,
    c.C_FIRSTNAME,
    c.C_LASTNAME,
    r.R_NAME
  FROM Customer c
  JOIN Nation n
    ON n.N_NATIONKEY = c.C_NATIONKEY
  JOIN Region r
    ON r.R_REGIONKEY = n.N_REGIONKEY
),
Customer_Spend AS (
  SELECT
    o.O_CUSTKEY AS C_CUSTKEY,
    SUM(o.O_TOTALPRICE) AS Customer_Total_Spent
  FROM Orders o
  GROUP BY o.O_CUSTKEY
),
Customer_Spend_By_Region AS (
  SELECT
    cr.R_NAME,
    cr.C_CUSTKEY,
    cr.C_FIRSTNAME,
    cr.C_LASTNAME,
    cs.Customer_Total_Spent
  FROM Customer_Regions cr
  JOIN Customer_Spend cs
    ON cs.C_CUSTKEY = cr.C_CUSTKEY
),
Region_Avg_Spend AS (
  SELECT
    R_NAME,
    AVG(Customer_Total_Spent) AS Region_Avg_Spend
  FROM Customer_Spend_By_Region
  GROUP BY R_NAME
)
SELECT
  csbr.R_NAME,
  csbr.C_CUSTKEY,
  csbr.C_FIRSTNAME,
  csbr.C_LASTNAME,
  csbr.Customer_Total_Spent,
  ras.Region_Avg_Spend
FROM Customer_Spend_By_Region csbr
JOIN Region_Avg_Spend ras
  ON ras.R_NAME = csbr.R_NAME
WHERE csbr.Customer_Total_Spent > ras.Region_Avg_Spend
ORDER BY
  csbr.R_NAME,
  csbr.Customer_Total_Spent DESC;


/* ---------------------------------------------------------
   Same logic as above, but WITHOUT CTEs
   (implemented using nested derived tables)
--------------------------------------------------------- */
SELECT
  csbr.R_NAME,
  csbr.C_CUSTKEY,
  csbr.C_FIRSTNAME,
  csbr.C_LASTNAME,
  csbr.Customer_Total_Spent,
  ras.Region_Avg_Spend
FROM
  (
    /* Customer_Spend_By_Region */
    SELECT
      r.R_NAME,
      c.C_CUSTKEY,
      c.C_FIRSTNAME,
      c.C_LASTNAME,
      cs.Customer_Total_Spent
    FROM Customer c
    JOIN Nation n
      ON n.N_NATIONKEY = c.C_NATIONKEY
    JOIN Region r
      ON r.R_REGIONKEY = n.N_REGIONKEY
    JOIN (
      /* Customer_Spend */
      SELECT
        o.O_CUSTKEY AS C_CUSTKEY,
        SUM(o.O_TOTALPRICE) AS Customer_Total_Spent
      FROM Orders o
      GROUP BY o.O_CUSTKEY
    ) cs
      ON cs.C_CUSTKEY = c.C_CUSTKEY
  ) csbr
JOIN
  (
    /* Region_Avg_Spend computed from Customer_Spend_By_Region (repeated) */
    SELECT
      x.R_NAME,
      AVG(x.Customer_Total_Spent) AS Region_Avg_Spend
    FROM (
      /* Customer_Spend_By_Region (repeated again) */
      SELECT
        r.R_NAME,
        c.C_CUSTKEY,
        cs.Customer_Total_Spent
      FROM Customer c
      JOIN Nation n
        ON n.N_NATIONKEY = c.C_NATIONKEY
      JOIN Region r
        ON r.R_REGIONKEY = n.N_REGIONKEY
      JOIN (
        /* Customer_Spend (repeated again) */
        SELECT
          o.O_CUSTKEY AS C_CUSTKEY,
          SUM(o.O_TOTALPRICE) AS Customer_Total_Spent
        FROM Orders o
        GROUP BY o.O_CUSTKEY
      ) cs
        ON cs.C_CUSTKEY = c.C_CUSTKEY
    ) x
    GROUP BY x.R_NAME
  ) ras
  ON ras.R_NAME = csbr.R_NAME
WHERE csbr.Customer_Total_Spent > ras.Region_Avg_Spend
ORDER BY
  csbr.R_NAME,
  csbr.Customer_Total_Spent DESC;



