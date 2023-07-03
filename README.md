# memstore

This repository houses a versatile, object-oriented database model, designed for Node.js. It handles all your database needs with a simple interface, providing an easy-to-use API with a variety of powerful features.

## Features 🚀

 - Map/Set, BigInt, Symbol, and Recursive Object Handling: The model is equipped to handle more than just the simple data types, you can also work with Map/Set, BigInt, Symbol, and even recursive objects.
 - In-Memory Data Store: this database stores data in memory, providing extremely fast access times.
 - File System Based Persistence: We have a subclass of the Database that is designed to persistently store your data on the filesystem, allowing you to save and load data as needed.
 - Efficient Indexing and Lookups: There is built-in indexing to provide fast lookup of records.
 - ACID Transaction: The model supports transactions. This means you can perform a series of operations as a single atomic unit, and it will ensure consistency and isolation of each transaction.
 - Auto-Generated Unique Identifiers: The model also includes an auto-incrementing numeric unique ID for each record added to a table.

## Getting Started 🏁

### Import the Database class

```javascript
import { Database, FSDatabase } from './database';
```

### Create a new Database or FSDatabase instance

```javascript
const db = new Database();
const fsDb = new FSDatabase('./myData.json');
```

### Add a table

```javascript
db.createTable('users', ['name', 'email']);
```

### Add some records to the table

```javascript
db.addRecord('users', { name: 'John', email: 'john@gmail.com' });
```

### Retrieve a record

```javascript
    const userRecords = db.getRecordsByColumn('users', 'name', 'John');
```

### Working with indices

```javascript
const db = new Database();

// Create a table with indices
db.createTable('users', ['name', 'email']);

// Check if an index exists
const indexExists = db.indexExists('users', 'email');  // true

// Add an index to an existing table
db.addIndex('users', 'age');

// Remove an index from a table
db.removeIndex('users', 'age');
```


### Working with transactions

```javascript
const db = new Database();

db.createTable('users');

db.beginTransaction();

try {
    // Add some records
    db.addRecord('users', { name: 'John' });
    db.addRecord('users', { name: 'Jane' });

    // Commit the transaction
    db.commitTransaction();
} catch (error) {
    // Rollback the transaction in case of any errors
    db.rollbackTransaction();
}
```

### Removing a record

```javascript
const record = { name: 'John' };

// Add a record
db.addRecord('users', record);

// Remove a record
db.removeRecord('users', record);
```
