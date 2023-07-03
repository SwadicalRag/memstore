import test, { describe, it } from "node:test";
import assert from "assert";
import { Database } from "./Database";

describe("Database Tests", () => {
    const db = new Database();
    const tableName = "testTable";
    const indexKey = "testIndex";
    const testRecord = {
        firstName: "John",
        lastName: "Doe",
        email: "john.doe@example.com",
    };

    it("Create a new table", () => {
        db.createTable(tableName, [indexKey]);
        assert(db.tableExists(tableName));
    });

    it("Add a new record", () => {
        const newRecord = db.addRecord(tableName, testRecord);
        assert(newRecord);
    });

    it("Fetch a record by column", () => {
        const records = db.getRecordsByColumn(tableName, "email", testRecord.email);
        assert(records.length > 0);
    });

    it("Add and remove an index", () => {
        db.addIndex(tableName, "lastName");
        assert(db.indexExists(tableName, "lastName"));
        db.removeIndex(tableName, "lastName");
        assert(!db.indexExists(tableName, "lastName"));
    });

    it("Remove a record", () => {
        const recordToRemove = db.getRecordsByColumn(tableName, "email", testRecord.email)[0];
        db.removeRecord(tableName, recordToRemove);
        const records = db.getRecordsByColumn(tableName, "email", testRecord.email);
        assert(records.length === 0);
    });

    it("Delete a table", () => {
        db.deleteTable(tableName);
        assert(!db.tableExists(tableName));
    });
});

describe("Database Wrap Functions Tests", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        contact: {
            email: "john.doe@example.com",
            phone: "1234567890",
            special: {pager: "1234"},
        },
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when changing a property", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.name = "Doe"; // Changing the name property should trigger onUpdateRecord

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "set");
        assert.strictEqual(event.key, "name");
        assert.strictEqual(event.oldValue, "John");
        assert.strictEqual(event.newValue, "Doe");
        done();
    });

    test("Trigger onUpdateRecord when changing a property of a nested object", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.contact.special.pager = "54321";

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.tableName, tableName);
        assert.strictEqual(event.colName, "contact");
        assert.strictEqual(event.record, newRecord);
        assert.strictEqual(event.object, newRecord.contact.special);
        assert.strictEqual(event.type, "set");
        assert.strictEqual(event.key, "pager");
        assert.strictEqual(event.oldValue, "1234");
        assert.strictEqual(event.newValue, "54321");
        done();
    });
});

describe("Database Wrap Functions with Map and Set", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        contactDetails: new Map().set("email", "john.doe@example.com").set("phone", "1234567890"),
        previousAddresses: new Set(["address1", "address2"])
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when adding an item in a Map", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.contactDetails.set("fax", "1234567891");

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.set");
        assert.strictEqual(event.key, "set");
        assert.deepStrictEqual(event.args, ["fax", "1234567891"]);
        done();
    });

    test("Trigger onUpdateRecord when removing an item in a Map", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.contactDetails.delete("fax");

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.delete");
        assert.strictEqual(event.key, "delete");
        assert.deepStrictEqual(event.args, ["fax"]);
        done();
    });

    test("Trigger onUpdateRecord when adding an item in a Set", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.previousAddresses.add("address3");

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.add");
        assert.strictEqual(event.key, "add");
        assert.deepStrictEqual(event.args, ["address3"]);
        done();
    });

    test("Trigger onUpdateRecord when removing an item in a Set", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.previousAddresses.delete("address1");

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.delete");
        assert.strictEqual(event.key, "delete");
        assert.deepStrictEqual(event.args, ["address1"]);
        done();
    });

    test("Trigger onUpdateRecord when clearing a Set", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.previousAddresses.clear();

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.clear");
        assert.strictEqual(event.key, "clear");
        assert.deepStrictEqual(event.args, []);
        done();
    });
});

describe("Database Wrap Functions with Arrays", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        hobbies: ["reading", "coding", "travelling"]
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when an array item is changed", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.hobbies[1] = "biking";

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.tableName, tableName);
        assert.strictEqual(event.colName, "hobbies");
        assert.strictEqual(event.record, newRecord);
        assert.strictEqual(event.object, newRecord.hobbies);
        assert.strictEqual(event.type, "set");
        assert.strictEqual(event.key, "1");
        assert.strictEqual(event.oldValue, "coding");
        assert.strictEqual(event.newValue, "biking");
        done();
    });

    test("Trigger onUpdateRecord when a new array item is added", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.hobbies[3] = "music";

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.tableName, tableName);
        assert.strictEqual(event.colName, "hobbies");
        assert.strictEqual(event.record, newRecord);
        assert.strictEqual(event.object, newRecord.hobbies);
        assert.strictEqual(event.type, "set");
        assert.strictEqual(event.key, "3");
        assert.strictEqual(event.oldValue, undefined);
        assert.strictEqual(event.newValue, "music");
        done();
    });

    test("Trigger onUpdateRecord when an array item is deleted", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        delete newRecord.hobbies[2];

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.tableName, tableName);
        assert.strictEqual(event.colName, "hobbies");
        assert.strictEqual(event.record, newRecord);
        assert.strictEqual(event.object, newRecord.hobbies);
        assert.strictEqual(event.type, "delete");
        assert.strictEqual(event.key, "2");
        assert.strictEqual(event.oldValue, "travelling");
        assert.strictEqual(event.newValue, undefined);
        done();
    });
});

describe("Database Wrap Functions with Array Methods", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        hobbies: ["reading", "coding", "travelling"]
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when an item is added using push", (_, done) => {
        let events: any[] = [];
        db.onUpdateRecord = (e) => {
            events.push(e);
        };
    
        newRecord.hobbies.push("music");
    
        assert.strictEqual(events.length, 1);
    
        const event1 = events[0];
        assert.strictEqual(event1.type, "method.push");
        assert.strictEqual(event1.args[0], "music");
    
        done();
    });

    test("Trigger onUpdateRecord when an item is added using unshift", (_, done) => {
        let events: any[] = [];
        db.onUpdateRecord = (e) => {
            events.push(e);
        };
    
        newRecord.hobbies.unshift("music");
    
        assert.strictEqual(events.length, 1);
    
        const event1 = events[0];
        assert.strictEqual(event1.type, "method.unshift");
        assert.strictEqual(event1.args[0], "music");
    
        done();
    });

    test("Trigger onUpdateRecord when an item is removed using pop", (_, done) => {
        let events: any[] = [], poppedValue;
        db.onUpdateRecord = (e) => {
            events.push(e);
        };
    
        poppedValue = newRecord.hobbies.pop();
    
        assert.strictEqual(events.length, 1);
    
        const event1 = events[0];
        assert.strictEqual(event1.type, "method.pop");
        assert.deepEqual(event1.args, []);
        assert.strictEqual(event1.deletedValue, poppedValue);
    
        done();
    });
});

describe("Database Transaction", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        hobbies: ["reading", "coding", "travelling"]
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Records should be altered inside of a transaction", (_, done) => {
        db.beginTransaction();

        newRecord.hobbies.push("dancing");
    
        assert.strictEqual(newRecord.hobbies.length, 4);
        assert.strictEqual(newRecord.hobbies[3], "dancing");
    
        db.commitTransaction();

        done();
    });

    test("Records should be rolled back after a transaction", (_, done) => {
        db.beginTransaction();

        newRecord.hobbies.push("painting");
    
        assert.strictEqual(newRecord.hobbies.length, 5);
        assert.strictEqual(newRecord.hobbies[4], "painting");

        db.rollbackTransaction();

        assert.strictEqual(newRecord.hobbies.length, 4);
        assert.strictEqual(newRecord.hobbies.includes("painting"), false);
    
        done();
    });

    test("Records should be rolled back after a complex transaction", (_, done) => {
        db.beginTransaction();

        newRecord.hobbies.push("painting");
        newRecord.hobbies.splice(2, 1, "banana","apple");

        db.rollbackTransaction();

        assert.strictEqual(newRecord.hobbies.length, 4);
        assert.strictEqual(newRecord.hobbies.includes("painting"), false);
        
        assert.strictEqual(newRecord.hobbies.length, 4);
        assert.strictEqual(newRecord.hobbies.includes("banana"), false);
    
        done();
    });
});

describe("Database Wrap Functions with Iterators", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        contactDetails: new Map().set("email", "john.doe@example.com").set("phone", "1234567890"),
        previousAddresses: new Set(["address1", "address2"]),
        hobbies: ["reading", "coding", "travelling"]
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when modifying a Map inside iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let key of newRecord.contactDetails.keys()) {
            if (key === "email") {
                newRecord.contactDetails.set(key, "new.email@example.com");
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.set");
        assert.strictEqual(event.key, "set");
        assert.deepStrictEqual(event.args, ["email", "new.email@example.com"]);
        done();
    });

    test("Trigger onUpdateRecord when modifying a Set inside iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let address of newRecord.previousAddresses) {
            if (address === "address1") {
                newRecord.previousAddresses.delete(address);
                newRecord.previousAddresses.add("address3");
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.add");
        assert.strictEqual(event.key, "add");
        assert.deepStrictEqual(event.args, ["address3"]);
        done();
    });

    test("Trigger onUpdateRecord when modifying an Array inside iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let i = 0; i < newRecord.hobbies.length; i++) {
            if (newRecord.hobbies[i] === "coding") {
                newRecord.hobbies[i] = "biking";
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.tableName, tableName);
        assert.strictEqual(event.colName, "hobbies");
        assert.strictEqual(event.record, newRecord);
        assert.strictEqual(event.object, newRecord.hobbies);
        assert.strictEqual(event.type, "set");
        assert.strictEqual(event.key, "1");
        assert.strictEqual(event.oldValue, "coding");
        assert.strictEqual(event.newValue, "biking");
        done();
    });
});

describe("Database Wrap Functions with Nested Iterators", () => {
    const db = new Database();
    const tableName = "testTable";
    const testRecord = {
        name: "John",
        contactDetails: new Map().set("email", "john.doe@example.com").set("addresses", new Set(["address1", "address2"])),
    };
    db.createTable(tableName);
    const newRecord = db.addRecord(tableName, testRecord);

    test("Trigger onUpdateRecord when modifying nested Set inside Map using keys() iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let key of newRecord.contactDetails.keys()) {
            if (key === "addresses") {
                newRecord.contactDetails.get(key).add("address3");
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.add");
        assert.strictEqual(event.key, "add");
        assert.deepStrictEqual(event.args, ["address3"]);
        done();
    });

    test("Trigger onUpdateRecord when modifying nested Set inside Map using values() iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let value of newRecord.contactDetails.values()) {
            if (value instanceof Set) {
                value.add("address4");
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.add");
        assert.strictEqual(event.key, "add");
        assert.deepStrictEqual(event.args, ["address4"]);
        done();
    });

    test("Trigger onUpdateRecord when modifying nested Set inside Map using entries() iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let [key, value] of newRecord.contactDetails.entries()) {
            if (key === "addresses") {
                value.delete("address1");
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.delete");
        assert.strictEqual(event.key, "delete");
        assert.deepStrictEqual(event.args, ["address1"]);
        done();
    });

    test("Trigger onUpdateRecord when modifying nested Set inside Map using Symbol.iterator", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        for (let [key, value] of newRecord.contactDetails) {
            if (key === "addresses") {
                value.clear();
            }
        }

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.clear");
        assert.strictEqual(event.key, "clear");
        assert.deepStrictEqual(event.args, []);
        done();
    });

    test("Trigger onUpdateRecord when modifying nested Set inside Map using forEach", (_, done) => {
        let event: any;
        db.onUpdateRecord = (e) => {
            event = e;
        };

        newRecord.contactDetails.forEach((value, key) => {
            if (key === "addresses") {
                value.add("address5");
            }
        });

        assert.strict(typeof event !== "undefined");
        assert.strictEqual(event.type, "method.add");
        assert.strictEqual(event.key, "add");
        assert.deepStrictEqual(event.args, ["address5"]);
        done();
    });
});
