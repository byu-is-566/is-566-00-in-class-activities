# dbt Testing Workshop: Student Guide

## Setup

You should already have the Jaffle Shop DuckDB project open and working. (Create the dbt `.venv` with `uv` if needed.) Confirm by running:

```bash
dbt build
```

All models should build and all existing tests should pass.

---

## Activity 1: Orient + Run (5 min)

**Goal:** See what tests look like in YAML and what the output looks like when they pass.

1. Open these two files in your editor:
   - `models/staging/schema.yml`
   - `models/schema.yml`
2. Look through both files. For each test declaration you find, note:
   - Which model is it under?
   - Which column is it on?
   - What type of test is it?
3. Run all tests:
   ```bash
   dbt test
   ```
4. Run tests for just one model:
   ```bash
   dbt test --select stg_orders
   ```

---

## Activity 2: Break Something (8 min)

**Goal:** Intentionally introduce a data quality problem, watch the test catch it, and read the failure output.

Pick **one** of the following options:

### Option A: Duplicate Primary Key

1. Edit `seeds/raw_customers.csv` and add this line at the bottom: `1,Duplicate,Customer`
2. Run:
   ```bash
   dbt seed && dbt test --select stg_customers
   ```
3. Read the failure output. Which test failed and why?
4. Open `target/compiled/` and find the compiled SQL for the failing test. Read it.
5. **Revert your CSV change before moving on.**

### Option B: Invalid Status Value

1. Edit `seeds/raw_orders.csv` line 2, change `returned` to `cancelled`
2. Run:
   ```bash
   dbt seed && dbt test --select stg_orders
   ```
3. Read the failure output. Which test failed and why?
4. Think about it: should you fix the data, or add `cancelled` to the allowed list?
5. **Revert your CSV change before moving on.**

---

## Activity 3: Write Your Own Built-in Tests (10 min)

**Goal:** Add test declarations for columns that currently have no tests.

1. Open `models/schema.yml`. Under the `customers` model, add a `not_null` test to:
   - `first_name`
   - `last_name`
   - `number_of_orders`
2. Open `models/staging/schema.yml`. Under the `stg_orders` model, add a `not_null` test to:
   - `customer_id`
3. Run all tests:
   ```bash
   dbt test
   ```

**Something will fail.** When it does, your job is to figure out *why* it fails. Look at the model SQL that produces the failing column. Think about the JOIN logic.

Ask yourself: is this bad data, or is the test wrong?

---

## Activity 4: Read + Run a Custom Test (5 min)

**Goal:** Read an existing custom test, understand what it checks, and run it.

1. Open `tests/assert_order_amount_is_non_negative.sql`
2. Read the SQL carefully. Before running it, answer for yourself:
   - What does this test check?
   - What kind of data would make it fail?
3. Run it:
   ```bash
   dbt test --select assert_order_amount_is_non_negative
   ```

Remember: custom tests follow one rule. If the query returns rows, those rows are violations and the test fails.

---

## Activity 5: Write a Custom Test (10 min)

**Goal:** Write your own custom test from a business rule.

**The business rule:** "Every payment should reference an order that actually exists."

**Instructions:**

1. Create a new file: `tests/assert_no_orphan_payments.sql`
2. Write a SQL query that finds payments whose `order_id` does not match any order in `stg_orders`
3. Use `{{ ref('stg_payments') }}` and `{{ ref('stg_orders') }}` to reference the models
4. Run your test:
   ```bash
   dbt test --select assert_no_orphan_payments
   ```

**If you get stuck, try these hints one at a time:**

- Hint 1: You need to JOIN `stg_payments` to `stg_orders` on `order_id`
- Hint 2: Use a LEFT JOIN. Rows where the right side is NULL are orphans.
- Hint 3: Your WHERE clause should filter for cases where the order side is NULL.

---

## Activity 6: Data Detective (20 min)

**Goal:** Upstream data has arrived with quality issues. Your tests are the safety net. Your job is to find out what went wrong.

**Setup** (your instructor will walk you through this):

```bash
mv seeds/raw_payments.csv seeds/raw_payments_good.csv
cp raw_payments_bad.csv seeds/raw_payments.csv
dbt seed && dbt build
```

**Your mission:**

1. Run the build and observe the test failures
2. For each failure:
   - Read the failure message carefully
   - Query the data to investigate
   - Trace back to the seed file to find the root cause
   - Document: which line in the CSV is the problem, what changed, and which test caught it
3. There are **three** distinct issues hidden in the bad data file. Find all three.

**Investigation tips:**

- Use `dbt test --select model_name` to run specific tests
- Look at `target/compiled/` for the SQL behind failing tests
- Write ad-hoc queries against your models to explore the data
- Some issues are harder to spot than others. Pay attention to details that aren't visible at first glance.

**When you're done, restore the clean data:**

```bash
mv seeds/raw_payments_good.csv seeds/raw_payments.csv
dbt seed && dbt build
```

---

## Quick Reference

### Running tests

| Command | What it does |
|---------|-------------|
| `dbt test` | Run all tests |
| `dbt test --select model_name` | Run tests for one model |
| `dbt test --select test_name` | Run one specific test |
| `dbt build` | Run models + tests together |

### Built-in test syntax (in schema.yml)

```yaml
models:
  - name: my_model
    columns:
      - name: my_column
        tests:
          - unique
          - not_null
          - accepted_values:
              values: ['a', 'b', 'c']
          - relationships:
              to: ref('other_model')
              field: other_column
```

### Custom test convention

- File location: `tests/your_test_name.sql`
- Write a SELECT that returns **violations**
- If the query returns rows, the test **fails**
- If the query returns zero rows, the test **passes**
- Use `{{ ref('model_name') }}` to reference models
