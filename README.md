# Retail Rules Engine (Scala Functional Programming)

## Project Overview

This project is a high-performance, functionally pure rule engine built in Scala. It processes retail transactions from a CSV file with 10M rows, evaluates them against a series of qualifying discount rules, calculates the final price, logs the engine's events, and persists the processed data into a relational database.

The application is built to support both **SQLite** (for lightweight, local testing) and **PostgreSQL** (for production-grade database integration) within the same repository.

## Functional Programming Principles Applied

This project strictly adheres to the functional programming constraints outlined in the project requirements. Below is a breakdown of how these principles were implemented:

* **Immutability (`val` vs `var`):** No mutable variables (`var`) or mutable data structures were used anywhere in the codebase. All collections (like `List` and `Seq`) are immutable, and states are carried forward safely using functional paradigms.
* **No Iterative Loops:** Traditional `for` or `while` loops were completely avoided. Iteration and state accumulation are handled using `map`, `filter`, `flatMap`, and `foldLeft`.
* **Pure Functions:** The core business logic is entirely decoupled from side effects. Every discount rule is defined as a pure function (specifically, a Tuple containing a `Qualifier` and a `Calculator`). Given the same `Transaction`, these functions will always return the exact same discount without modifying the input or affecting external states.
* **Functional Error Handling:** * I/O operations (like parsing CSV rows) are wrapped in `Try` to safely catch formatting issues without throwing runtime exceptions.
  * `Option` is heavily utilized to represent the presence or absence of data safely.
* **Resource Management:** Database connections and file writers are wrapped in Scala's `Using` construct, ensuring resources are safely closed after execution, preventing memory leaks without relying on imperative `try-catch-finally` blocks.

## Business Rules Implemented

1. **Expiry Rule:** Discounts based on days remaining until expiration (e.g., 29 days = 1%, 15 days = 15%). Max 30%.
2. **Product Type Rule:** Cheese products receive 10%, Wine products receive 5%.
3. **Special Date Rule:** Transactions occurring exactly on March 23rd receive a 50% discount.
4. **Quantity Rule:** 6-9 units = 5%, 10-14 units = 7%, >15 units = 10%.
5. **App Channel Rule:** Purchases via the App get a discount based on quantity ceilings (5% for 1-5, 10% for 6-10, etc.).
6. **Visa Rule:** Payments made via Visa receive a flat 5% discount.

*Calculation Engine:* If a transaction qualifies for multiple discounts, only the **top two** highest discounts are selected and averaged. If no rules apply, a 0% discount is applied.

## Architecture & Performance

To handle potentially massive datasets (`TRX10M.csv`), the application utilizes a lazy `Iterator` to read the CSV line-by-line. Instead of loading everything into memory, the data is chunked into batches of 10,000 records.

Each chunk is processed concurrently using `scala-parallel-collections` (`chunk.par`), distributing the heavy calculation load across multiple CPU cores. Finally, the processed records are inserted into the database using JDBC batch execution (`executeBatch`), significantly reducing network round-trips and I/O overhead.

## Tech Stack

* **Language:** Scala 2.13.12
* **Build Tool:** sbt 1.12.x
* **Dependencies:**
  * `scala-parallel-collections` (Parallel processing)
  * `sqlite-jdbc` (SQLite integration)
  * `postgresql` (PostgreSQL integration)
* **Environment:** VS Code (Metals extension)

---

## Setup & Execution Guide

### Prerequisites

1. Scala and `sbt` installed.
2. VS Code with the Scala (Metals) extension.
3. For PostgreSQL execution: A local instance of PostgreSQL installed and running.

### 1. Database Configuration (PostgreSQL)

Ensure your local PostgreSQL server is running. You will need to create an empty database for the engine to write to:

```sql
create database rules_engine;
```

Open `src/main/scala/pg/Main_PG.scala` and verify the connection credentials match your local setup:

```scala
val dbUrl = "jdbc:postgresql://localhost:5432/rules_engine"
val dbUser = "postgres" // Change to your local pg username
val dbPass = "admin"    // Change to your local pg password
```

### 2. Project Directory Structure

Ensure your data file is in the root directory, alongside `build.sbt`:

```text
/retail-rules-engine
 ├── build.sbt
 ├── TRX10M.csv
 └── /src/main/scala
      ├── sqlite/Main_SQLlite.scala
      └── pg/Main_PG.scala
```

### 3. Running the Engine

Open the VS Code terminal and navigate to the project root. Since the project contains two entry points, use the `runMain` command to specify which database target you want to execute.

**To run the PostgreSQL version:**

```bash
sbt "runMain pg.Main"
```

**To run the SQLite version:**

```bash
sbt "runMain sqlite.Main"
```

### Expected Output

Upon successful execution, the application will:

1. Print batch processing updates and total row counts to the terminal.
2. Generate/append to a `rules_engine.log` file in the root directory, recording all system events and errors with timestamps.
3. Automatically create a `processed_transactions` table in your chosen database and populate it with the final calculated prices and applied discounts.
