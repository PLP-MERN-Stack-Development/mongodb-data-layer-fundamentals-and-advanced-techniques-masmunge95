// queries.js - Script to perform various queries on the plp_bookstore database

const { MongoClient } = require('mongodb');

// Connection URI

/**
 * Helper function to pretty print query results.
 * @param {string} title - The title of the query.
 * @param {Array|Object} result - The result from the database operation.
 */
function printResult(title, result) {
  console.log(`\n--- ${title} ---`);
  if (Array.isArray(result)) {
    if (result.length === 0) {
      console.log("No documents found.");
    } else {
      console.log(JSON.stringify(result, null, 2));
    }
  } else {
    console.log(result);
  }
}

/**
 * Task 2: Basic CRUD Operations
 */
async function runTask2(collection) {
    console.log('\n--- Running Task 2: Basic CRUD Operations ---');

    // Find all books in a specific genre ('Fiction')
    const fictionBooks = await collection.find({ genre: 'Fiction' }).toArray();
    printResult("Task 2.1: Find all 'Fiction' books", fictionBooks);

    // Find books published after a certain year (1950)
    const recentBooks = await collection.find({ published_year: { $gt: 1950 } }).toArray();
    printResult("Task 2.2: Find books published after 1950", recentBooks);

    // Find books by a specific author ('George Orwell')
    const orwellBooks = await collection.find({ author: 'George Orwell' }).toArray();
    printResult("Task 2.3: Find books by 'George Orwell'", orwellBooks);

    // Update the price of a specific book ('The Hobbit')
    const updateResult = await collection.updateOne(
      { title: 'The Hobbit' },
      { $set: { price: 15.99 } }
    );
    printResult("Task 2.4: Update price of 'The Hobbit'", updateResult);
    const updatedHobbit = await collection.findOne({ title: 'The Hobbit' });
    printResult("Task 2.4: Verifying update for 'The Hobbit'", updatedHobbit);

    // Delete a book by its title ('Moby Dick')
    const deleteResult = await collection.deleteOne({ title: 'Moby Dick' });
    printResult("Task 2.5: Delete 'Moby Dick'", deleteResult);

}

/**
 * Task 3: Advanced Queries
 */
async function runTask3(collection) {
    console.log('\n--- Running Task 3: Advanced Queries ---');
    const inStockAndRecent = await collection.find({
      in_stock: true,
      published_year: { $gt: 1950 }
    }).toArray();
    printResult("Task 3.1: Find in-stock books published after 1950", inStockAndRecent);

    // Use projection to return only the title, author, and price
    const projectedBooks = await collection.find({})
      .project({ title: 1, author: 1, price: 1, _id: 0 })
      .toArray();
    printResult("Task 3.2: Project title, author, and price", projectedBooks);

    // Implement sorting to display books by price (ascending)
    const sortedAsc = await collection.find({})
      .sort({ price: 1 })
      .project({ title: 1, price: 1, _id: 0 })
      .toArray();
    printResult("Task 3.3: Sort by price ascending", sortedAsc);

    // Implement sorting to display books by price (descending)
    const sortedDesc = await collection.find({})
      .sort({ price: -1 })
      .project({ title: 1, price: 1, _id: 0 })
      .toArray();
    printResult("Task 3.3: Sort by price descending", sortedDesc);

    // Use limit and skip for pagination (Page 1, 5 books)
    const page1 = await collection.find({})
      .sort({ published_year: 1 })
      .skip(0)
      .limit(5)
      .project({ title: 1, published_year: 1, _id: 0 })
      .toArray();
    printResult("Task 3.4: Pagination - Page 1 (5 books)", page1);

    // Use limit and skip for pagination (Page 2, 5 books)
    const page2 = await collection.find({})
      .sort({ published_year: 1 })
      .skip(5)
      .limit(5)
      .project({ title: 1, published_year: 1, _id: 0 })
      .toArray();
    printResult("Task 3.4: Pagination - Page 2 (5 books)", page2);
}

/**
 * Task 4: Aggregation Pipeline
 */
async function runTask4(collection) {
    console.log('\n--- Running Task 4: Aggregation Pipeline ---');

    const avgPriceByGenre = await collection.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" },
          bookCount: { $sum: 1 }
        }
      },
      { $sort: { averagePrice: -1 } }
    ]).toArray();
    printResult("Task 4.1: Average price by genre", avgPriceByGenre);

    // Find the author with the most books in the collection
    const authorWithMostBooks = await collection.aggregate([
      {
        $group: {
          _id: "$author",
          numberOfBooks: { $sum: 1 }
        }
      },
      { $sort: { numberOfBooks: -1 } },
      { $limit: 1 }
    ]).toArray();
    printResult("Task 4.2: Author with the most books", authorWithMostBooks);

    // Group books by publication decade and count them
    const booksByDecade = await collection.aggregate([
      {
        $group: {
          _id: {
            $subtract: [
              "$published_year",
              { $mod: ["$published_year", 10] }
            ]
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } },
      { $project: { decade: "$_id", count: 1, _id: 0 } }
    ]).toArray();
    printResult("Task 4.3: Group books by publication decade", booksByDecade);
}

/**
 * Task 5: Indexing
 */
async function runTask5(collection) {
    console.log('\n--- Running Task 5: Indexing ---');
    console.log("This task creates indexes and analyzes query performance.");

    // Create an index on the 'title' field
    try {
      const titleIndexResult = await collection.createIndex({ title: 1 });
      console.log("Task 5.1: Index on 'title' created successfully:", titleIndexResult);
    } catch (e) {
      console.error("Error creating title index:", e.message);
    }

    // Create a compound index on 'author' and 'published_year'
    try {
      const compoundIndexResult = await collection.createIndex({ author: 1, published_year: -1 });
      console.log("Task 5.2: Compound index on 'author' and 'published_year' created successfully:", compoundIndexResult);
    } catch (e) {
      console.error("Error creating compound index:", e.message);
    }

    // Use explain() to demonstrate performance improvement
    console.log("\nTask 5.3: Using explain() to analyze query performance");

    // Helper to analyze and print explain plan
    const analyzePlan = (title, explainPlan) => {
      const stats = explainPlan.executionStats;
      const winningStage = stats.executionStages;
      // The actual scan stage is usually nested inside a FETCH stage.
      const scanStage = winningStage.inputStage || winningStage;

      console.log(`\n--- ${title} ---`);
      console.log(`Execution Time: ${stats.executionTimeMillis}ms`);
      console.log(`Documents Examined: ${stats.totalDocsExamined}`);
      console.log(`Winning Plan Stage: ${winningStage.stage} -> ${scanStage.stage}`);
      if (scanStage.stage === 'IXSCAN') {
        console.log("Analysis: Great! The query used an efficient index scan (IXSCAN).");
      } else if (scanStage.stage === 'COLLSCAN') {
        console.log("Analysis: The query used a collection scan (COLLSCAN). An index was not used.");
      }
    };

    // Explain a query that uses the 'title' index
    const explainTitleSearch = await collection.find({ title: '1984' }).explain("executionStats");
    analyzePlan("Explain plan for query on indexed 'title' field", explainTitleSearch);

    // Explain a query that uses the compound index
    const explainCompoundSearch = await collection.find({ author: 'George Orwell', published_year: { $lt: 1950 } }).explain("executionStats");
    analyzePlan("Explain plan for query on compound indexed fields ('author', 'published_year')", explainCompoundSearch);
}

/**
 * Main function to connect to MongoDB and run all tasks.
 */
async function main() {
  // Connection URI and DB/Collection names
  const uri = 'mongodb://localhost:27017';
  const dbName = 'plp_bookstore';
  const collectionName = 'books';

  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected successfully to MongoDB server');
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Run all tasks sequentially
    await runTask2(collection);
    await runTask3(collection);
    await runTask4(collection);
    await runTask5(collection);

  } catch (err) {
    console.error('An error occurred:', err);
  } finally {
    await client.close();
    console.log('\nConnection to MongoDB closed.');
  }
}

// Run the main function
main().catch(console.error);

/*
 * --- Explanation of Concepts ---
 *
 * Query Operators:
 * - `$gt`: "Greater Than". Used for finding numbers or dates after a certain value.
 * - `$lt`: "Less Than".
 * - `$set`: Used in update operations to set the value of a field.
 *
 * Projection:
 * - `project({ field: 1, _id: 0 })`: Selects which fields to return. `1` means include, `0` means exclude. `_id` is included by default, so we explicitly exclude it.
 *
 * Sorting:
 * - `sort({ field: 1 })`: Sorts in ascending order (A-Z, 1-10).
 * - `sort({ field: -1 })`: Sorts in descending order (Z-A, 10-1).
 *
 * Pagination:
 * - `skip(N)`: Skips the first N documents.
 * - `limit(N)`: Returns a maximum of N documents.
 * - For page `P` with `S` items per page, you `skip((P - 1) * S)`.
 *
 * Aggregation Pipeline:
 * - A series of stages that process documents.
 * - `$group`: Groups documents by a specified identifier (`_id`) and applies accumulator expressions (e.g., `$avg`, `$sum`).
 * - `$sort`: Sorts the documents.
 * - `$limit`: Limits the number of documents passed to the next stage.
 * - `$project`: Reshapes documents, similar to projection in find queries.
 *
 * Indexing:
 * - Indexes support the efficient execution of queries. Without them, MongoDB must perform a collection scan (COLLSCAN), reading every document.
 * - With an index, MongoDB can perform an index scan (IXSCAN), which is much faster.
 * - `createIndex({ field: 1 })`: Creates an ascending index on a field.
 * - `explain("executionStats")`: Provides detailed information about how a query was executed, allowing you to verify if an index was used.
 */
