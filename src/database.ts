import { promises as fs } from "fs";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { Obj2Obj } from "./obj2obj";

import * as v8 from "v8"

const _DEBUG = true;
let _V8 = false;

function logTime(cat: string) {
    if(_DEBUG) console.time(cat);
}

function logTimeEnd(cat: string) {
    if(_DEBUG) console.timeEnd(cat);
}

function logWarn(e: string) {
    if(_DEBUG) console.warn(e);
}

function logError(e: string) {
    if(_DEBUG) console.error(e);
}

export type DataRecordUniqueID = bigint;
export const UniqueID = Symbol("DataRecord.__id");
export type DataRecordExternal = {[colName: string]: any} & {[UniqueID]?: DataRecordUniqueID};
export type DataRecord = {[colName: string]: any} & {[UniqueID]: DataRecordUniqueID};
interface DataStore {
    [tableName: string]: {
        /** Master record */
        records: Map<DataRecordUniqueID, DataRecord>;

        /**
         * Map for close to O(1) lookup of DataRecords by string.
         * The records stored in the maps here have the same reference/object as in the `records` UID map above.
         */
        indices: {[colName: string]: Map<any, Set<DataRecord>>};

        /** auto incrementing numeric unique ID, used by `addRecord` */
        lastUID?: DataRecordUniqueID;
    };
};

type DataRecordUpdateEvent = (
    {type: "added"} |
    {type: "removed"} |
    {type: "method.clear", colName: string, object: Map<any, any> | Set<any>, key: string, args: []} |
    {type: "method.delete", colName: string, object: Map<any, any> | Set<any>, key: string, oldValue: any, args: [any]} |
    {type: "method.add", colName: string, object: Set<any>, key: string, args: [any]} |
    {type: "method.set", colName: string, object: Map<any, any>, key: string, oldValue: any, args: [any, any]} |
    {type: "method.push", colName: string, object: any[], key: string, args: [any]} |
    {type: "method.pop", colName: string, object: any[], key: string, deletedValue: any, args: []} |
    {type: "method.shift", colName: string, object: any[], key: string, deletedValue: any, args: []} |
    {type: "method.unshift", colName: string, object: any[], key: string, args: [any]} |
    {type: "method.splice", colName: string, object: any[], key: string, deletedValues: [], args: [number] | [number, number] | [number, number, any[]]} |
    {type: "delete", colName: string, object: any, key: string, oldValue: any} |
    {type: "set", colName: string, object: any, key: string, newValue: any, oldValue: any}
) & {record: DataRecord; tableName: string;};

interface Transaction {
    // records: {
    //     [tableName: string]: Map<DataRecordUniqueID, DataRecord>;
    // };
    // indices: {
    //     [tableName: string]: {[colName: string]: Map<any, Set<DataRecord>>};
    // };
    changes: DataRecordUpdateEvent[];
};

export class Database {
    private _data: DataStore = {};

    /** 
     * This method converts the DataStore object into a JSON string.
     * It uses the Encode method from Obj2Obj to handle special objects like recursive objects, Map/Set, BigInt and Symbol.
     */
    serialize(): string | Buffer {
        logTime("encode")
        const encoded = Obj2Obj.Encode(this._data, {__id: UniqueID});
        logTimeEnd("encode")
        logTime("serialize")
        const out = !_V8 ? JSON.stringify(encoded) : v8.serialize(encoded);
        logTimeEnd("serialize")
        return out;
    }

    /** 
     * This method is used to convert the JSON string or buffer into a DataStore object.
     * It uses the Decode method from Obj2Obj to handle special objects like recursive objects, Map/Set, BigInt and Symbol.
     * @param data - The JSON string or Buffer to be decoded.
     */
    private deserializeInternal(data: string | Buffer) {
        logTime("deserialize")
        const parsed = !_V8 ? JSON.parse(data.toString()) : v8.deserialize(data instanceof Buffer ? data : Buffer.from(data));
        logTimeEnd("deserialize")
        logTime("decode")
        const decoded = Obj2Obj.Decode(parsed, {__id: UniqueID});
        logTimeEnd("decode")
        return decoded;
    }

    load(data: string | Buffer) {
        this._data = this.deserializeInternal(data);
    }

    async saveFile(path: string) {
        await fs.writeFile(path, this.serialize());
    }

    async loadFile(path: string) {
        const data = await fs.readFile(path);
        this.load(data);
    }

    saveFileSync(path: string) {
        try {
            writeFileSync(path, this.serialize());
        } catch (e: any) {
            logError(`Failed to save file: ${e.message}`);
        }
    }

    loadFileSync(path: string) {
        try {
            if (existsSync(path)) {
                const data = readFileSync(path);
                this.load(data);
            } else {
                logError(`File does not exist at path: ${path}`);
            }
        } catch (e: any) {
            logError(`Failed to load file: ${e.message}`);
        }
    }

    tableExists(tableName: string) {
        return typeof this._data[tableName] !== "undefined";
    }

    createTable(tableName: string, indexKeys: string[] = []) {
        if (!this._data[tableName]) {
            this._data[tableName] = {
                records: new Map(),
                indices: {}
            };

            this.onUpdateTable(tableName, "added");
        }

        for (const key of indexKeys) {
            this.addIndex(tableName, key);
        }
    }

    deleteTable(tableName: string) {
        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return;
        }

        delete this._data[tableName];
        this.onUpdateTable(tableName, "removed");
    }

    private _wrapped = new Map<any, any>();
    private _wrap<T>(tableName: string, record: DataRecord, colName: string | undefined, obj: T): T {
        if (typeof obj !== "object" || obj === null) {
            return obj;
        }
        if(this._wrapped.has(obj)) return this._wrapped.get(obj);
   
        const wrapped =  new Proxy(obj, {
            get: (target: any, property) => {
                let curColName = colName ?? (property as any);

                if(target instanceof Promise) {
                    return (target as any)[property];
                }

                if (obj instanceof Map || obj instanceof Set) {
                    if(["add", "set", "delete", "clear"].includes(property as any)) {
                        return (...args: any[]) => {
                            this.onUpdateRecord({
                                tableName,
                                record: this._wrapRecord(tableName, record),
                                colName: curColName,
                                object: this._wrap(tableName, target, curColName, target),
                                type: `method.${property as string}` as any,
                                oldValue: ["set","delete"].includes(property as any) && obj instanceof Map? this._wrap(tableName, record, curColName, target.get(args[0])) : undefined,
                                key: property as any,
                                args: args as any,
                            });
                            return target[property].apply(target, args);
                        };
                    }
                    
                    const getterMethods = ["get", "entries", "keys", "values", "forEach", Symbol.iterator];
                    if(getterMethods.includes(property as any)) {
                        let result;
            
                        switch (property) {
                            case "get":
                                return (key: any) => this._wrap(tableName, target, curColName, target.get(key));
                            case "keys":
                            case "values":
                            case Symbol.iterator:
                                return () => Array.from(target[property]()).map(entry =>
                                    this._wrap(tableName, target, curColName, entry)
                                ).values();
                            case "entries":
                                if(obj instanceof Map) {
                                    return () => Array.from(target[property]()).map((entry: any) =>
                                        [
                                            this._wrap(tableName, target, curColName, entry[0]),
                                            this._wrap(tableName, target, curColName, entry[1]),
                                        ]
                                    ).values();
                                }
                                else {
                                    return () => Array.from(target[property]()).map(entry =>
                                        this._wrap(tableName, target, curColName, entry)
                                    ).values();
                                }
                            case "forEach":
                                return (callback: (value: any, key: any, mapOrSet: any) => any, thisArg?: any) => {
                                    target[property]((value: any, key: any, mapOrSet: any) => {
                                        callback.apply(
                                            thisArg,
                                            [
                                                this._wrap(tableName, target, curColName, value),
                                                this._wrap(tableName, target, curColName, key),
                                                this._wrap(tableName, target, curColName, mapOrSet)
                                            ]
                                        );
                                    }, thisArg); // thisArg
                                    return; // forEach doesn"t return anything
                                };
                        }
            
                        return result;
                    }
                }

                if(obj instanceof Map || obj instanceof Set || obj instanceof Date) {
                    const value = target[property] as any;
                    return typeof value == "function" ? value.bind(target) : value;
                }

                if (Array.isArray(obj)) {
                    // Array method tracking
                    switch (property) {
                        case "pop":
                        case "shift":
                            return (...args: any[]) => {
                                const deletedItems = target[property].apply(target, args);
                                this.onUpdateRecord({
                                    tableName,
                                    record: this._wrapRecord(tableName, record),
                                    colName: curColName,
                                    object: this._wrap(tableName, target, curColName, target),
                                    type: `method.${property as string}` as any,
                                    deletedValue: this._wrap(tableName, record, curColName, deletedItems),
                                    key: property as any,
                                    args: args as any,
                                });
                                return deletedItems;
                            };
                        case "push":
                        case "unshift":
                            return (...args: any[]) => {
                                const deletedItems = target[property].apply(target, args);
                                this.onUpdateRecord({
                                    tableName,
                                    record: this._wrapRecord(tableName, record),
                                    colName: curColName,
                                    object: this._wrap(tableName, target, curColName, target),
                                    type: `method.${property as string}` as any,
                                    key: property as any,
                                    args: args as any,
                                });
                                return deletedItems;
                            };
                        case "splice":
                            return (...args: any[]) => {
                                const deletedItems = target[property].apply(target, args);
                                this.onUpdateRecord({
                                    tableName,
                                    record: this._wrapRecord(tableName, record),
                                    colName: curColName,
                                    object: this._wrap(tableName, target, curColName, target),
                                    type: "method.splice",
                                    deletedValues: this._wrap(tableName, record, curColName, deletedItems),
                                    key: property as any,
                                    args: args as any,
                                });
                                return deletedItems;
                            };
                    }
                }

                return this._wrap(tableName, record, curColName, target[property]);
            },
            set: (target: any, property, value) => {
                let curColName = colName ?? (property as any);
                const oldValue = target[property];
                target[property] = this._wrap(tableName, record, curColName, value);
                this.onUpdateRecord({
                    tableName,
                    record: this._wrapRecord(tableName, record),
                    colName: curColName,
                    object: this._wrap(tableName, target, curColName, target),
                    type: "set",
                    key: property as any,
                    oldValue: this._wrap(tableName, record, curColName, oldValue),
                    newValue: this._wrap(tableName, record, curColName, value),
                });
                return true;
            },
            deleteProperty: (target, property) => {
                let curColName = colName ?? (property as any);
                const oldValue = target[property];
                delete target[property];
                this.onUpdateRecord({
                    tableName,
                    colName: curColName,
                    record: this._wrapRecord(tableName, record),
                    object: this._wrap(tableName, target, curColName, target),
                    type: "delete",
                    key: property as any,
                    oldValue: this._wrap(tableName, record, curColName, oldValue),
                });
                return true;
            },
        });

        this._wrapped.set(obj, wrapped);
        return wrapped;
    }
    
    private _wrapRecord<T extends DataRecord>(tableName: string, record: T): T {
        return this._wrap(tableName, record, undefined, record);
    }

    getNextUID(tableName: string) {
        const uid = this._data[tableName].lastUID || BigInt(0);
        return uid + BigInt(1);
    }

    reserveNextUID(tableName: string) {
        const uid = this.getNextUID(tableName);
        this._data[tableName].lastUID = uid;
        return uid;
    }

    addRecord<T extends DataRecordExternal>(tableName: string, record: T, wrapped = true) {
        type TI = T & DataRecord;

        if (!this._data[tableName]) {
            throw new Error(`Table "${tableName}" does not exist.`);
        }

        record[UniqueID] = this.reserveNextUID(tableName);

        this._data[tableName].records.set(record[UniqueID], record as TI);
        const wrappedRecord = wrapped ? this._wrapRecord(tableName, record as TI) : record as TI;

        for (const [colName, indexMap] of Object.entries(this._data[tableName].indices)) {
            if (wrappedRecord[colName] !== undefined) {
                if (!indexMap.has(wrappedRecord[colName])) {
                    indexMap.set(wrappedRecord[colName], new Set());
                }

                indexMap.get(wrappedRecord[colName])!.add(wrappedRecord);
            }
        }

        this.onUpdateRecord({
            type: "added",
            tableName,
            record: wrappedRecord,
        });

        return wrappedRecord;
    }

    setRecord<T extends DataRecordExternal>(tableName: string, record: T, uid: DataRecordUniqueID, wrapped = true) {
        type TI = T & DataRecord;

        if (!this._data[tableName]) {
            throw new Error(`Table "${tableName}" does not exist.`);
        }

        if(!this._data[tableName].lastUID || (this._data[tableName].lastUID! < uid)) {
            this._data[tableName].lastUID = uid;
        }

        record[UniqueID] = uid;
        this._data[tableName].records.set(record[UniqueID], record as TI);
        const wrappedRecord = wrapped ? this._wrapRecord(tableName, record as TI) : record as TI;

        for (const [colName, indexMap] of Object.entries(this._data[tableName].indices)) {
            if (wrappedRecord[colName] !== undefined) {
                if (!indexMap.has(wrappedRecord[colName])) {
                    indexMap.set(wrappedRecord[colName], new Set());
                }

                indexMap.get(wrappedRecord[colName])!.add(wrappedRecord);
            }
        }

        this.onUpdateRecord({
            type: "added",
            tableName,
            record: wrappedRecord,
        });

        return wrappedRecord;
    }

    removeRecord<T extends DataRecordExternal>(tableName: string, record: T) {
        type TI = T & DataRecord;

        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return;
        }

        if (!record[UniqueID]) {
            logError(`Could not find UID in record object for removal.`);
            return;
        }

        if (!this._data[tableName].records.has(record[UniqueID])) {
            logError(`Record with id "${record[UniqueID]}" does not exist in "${tableName}".`);
            return;
        }

        this._data[tableName].records.delete(record[UniqueID]);
        for (const [colName, indexMap] of Object.entries(this._data[tableName].indices)) {
            if (record[colName] !== undefined && indexMap.has(record[colName])) {
                indexMap.get(record[colName])!.delete(record as TI);
            }
        }

        this.onUpdateRecord({
            type: "removed",
            tableName,
            record: record as TI,
        });
    }

    removeRecordsByColumn<T extends DataRecordExternal, K extends keyof T>(tableName: string, colName: K, value: T[K], wrapped = true) {
        const entries = this.getRecordsByColumn(tableName, colName, value, wrapped);
        for(let entry of entries) {
            this.removeRecord(tableName, entry);
        }
        return entries;
    }

    getRecord<T extends DataRecord>(tableName: string, id: DataRecordUniqueID, wrapped = true): T | undefined {
        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return;
        }

        const record = this._data[tableName].records.get(id) as T;

        if (!record) {
            return;
        }

        return wrapped ? this._wrapRecord(tableName, record) : record;
    }

    getAllRecords<T extends DataRecord>(tableName: string): T[] {
        const out: T[] = [];

        for(let [uid,entry] of this._data[tableName].records) {
            out.push(entry as T);
        }

        return out;
    }

    getRecordsByColumn<T extends DataRecordExternal, K extends keyof T>(tableName: string, colName: K, value: T[K], wrapped = true) {
        type TI = T & DataRecord;

        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return [];
        }

        const indexMap = this._data[tableName].indices[colName as string];

        let records: T[];
    
        if (indexMap) {
            records = Array.from(indexMap.get(value) || []) as T[];
        } else {
            // logWarn(`Index for column "${String(colName)}" does not exist in "${tableName}". Performing O(n) search.`);
            records = (Array.from(this._data[tableName].records.values()) as T[])
                .filter(record => record[colName] === value);
        }

        return wrapped ? records.map(record => this._wrapRecord(tableName, record as TI)) : records as T[];
    }

    indexExists(tableName: string, indexKey: string) {
        if(!this.tableExists(tableName)) return false;
        return typeof this._data[tableName].indices[indexKey] !== "undefined";
    }

    addIndex(tableName: string, indexKey: string) {
        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return;
        }

        if (this._data[tableName].indices[indexKey]) {
            logError(`Index "${indexKey}" already exists in "${tableName}".`);
            return;
        }

        const indexMap: Map<string, Set<DataRecord>> = new Map();

        for (const record of this._data[tableName].records.values()) {
            const value = record[indexKey];
            if (value !== undefined) {
                if (!indexMap.has(value)) {
                    indexMap.set(value, new Set());
                }
                indexMap.get(value)!.add(record);
            }
        }

        this._data[tableName].indices[indexKey] = indexMap;

        this.onUpdateTable(tableName, "added-index", indexKey);
    }

    removeIndex(tableName: string, indexKey: string) {
        if (!this._data[tableName]) {
            logError(`Table "${tableName}" does not exist.`);
            return;
        }

        if (!this._data[tableName].indices[indexKey]) {
            logError(`Index "${indexKey}" does not exist in "${tableName}".`);
            return;
        }

        delete this._data[tableName].indices[indexKey];

        this.onUpdateTable(tableName, "removed-index", indexKey);
    }

    onUpdateRecord(event: DataRecordUpdateEvent) {
        if (this._transactions.length > 0) {
            this._transactions[this._transactions.length - 1].changes.push(event);
        }

        if(event.type === "set") {
            if(event.record === event.object) {
                const schema = this._data[event.tableName];
                const index = schema && schema.indices[event.key];
                if(index) {
                    let oldSet = index.get(event.oldValue);
                    if(oldSet) {
                        oldSet.delete(event.record);
                    }

                    let newSet = index.get(event.newValue);
                    if(!newSet) {
                        newSet = new Set();
                        index.set(event.newValue, newSet);
                    }
                    newSet.add(event.record);
                }
            }
        }
        else if(event.type === "delete") {
            if(event.record === event.object) {
                const schema = this._data[event.tableName];
                const index = schema && schema.indices[event.key];
                if(index) {
                    let oldSet = index.get(event.oldValue);
                    if(oldSet) {
                        oldSet.delete(event.record);
                    }

                    let newSet = index.get(undefined);
                    if(!newSet) {
                        newSet = new Set();
                        index.set(undefined, newSet);
                    }
                    newSet.add(event.record);
                }
            }
        }

        // Implement your logic here, e.g., trigger an event, update related data, etc.
    }

    onUpdateTable(tableName: string, event: "added" | "removed" | "updated" | "added-index" | "removed-index", colName?: string) {
        // Implement your logic here, e.g., trigger an event, update related data, etc.
    }// ...

    private _transactions: Transaction[] = [];

    get isInTransaction() {
        return this._transactions.length > 0;
    }

    beginTransaction() {
        this._transactions.push({
            // records: {},
            // indices: {},
            changes: [],
        });
    }

    commitTransaction() {
        if (this._transactions.length === 0) {
            throw new Error("No transaction to commit. Did you forget to call beginTransaction()?");
        }

        this._transactions.pop();
    }

    rollbackTransaction() {
        if (this._transactions.length === 0) {
            throw new Error("No transaction to rollback. Did you forget to call beginTransaction()?");
        }

        const transaction = this._transactions.pop()!;

        for (const change of transaction.changes.reverse()) {
            switch (change.type) {
                case "added":
                    this.removeRecord(change.tableName, change.record);
                    break;
                case "removed":
                    this.addRecord(change.tableName, change.record, false);
                    break;
                case "set":
                    change.object[change.key] = change.oldValue;
                    break;
                case "delete":
                    change.object[change.key] = change.oldValue;
                    break;
                case "method.add":
                    change.object.delete(change.args[0]);
                    break;
                case "method.delete":
                    if(change.object instanceof Map) {
                        change.object.set(change.args[0], change.oldValue);
                    }
                    else {
                        change.object.add(change.args[0]);
                    }
                    break;
                case "method.set":
                    if (change.object.has(change.args[0])) {
                        change.object.set(change.args[0], change.args[1]);
                    } else {
                        change.object.delete(change.args[0]);
                    }
                    break;
                case "method.clear":
                    // Restore the previous values.
                    break;
                case "method.pop":
                    if (Array.isArray(change.object)) {
                        change.object.push(change.deletedValue);
                    } else {
                        throw new Error("Invalid object type for pop method.");
                    }
                    break;
                case "method.shift":
                    if (Array.isArray(change.object)) {
                        change.object.unshift(change.deletedValue);
                    } else {
                        throw new Error("Invalid object type for shift method.");
                    }
                    break;
                case "method.push":
                    if (Array.isArray(change.object)) {
                        if (change.object.length > 0) {
                            change.object.pop();
                        }
                    } else {
                        throw new Error("Invalid object type for pop method.");
                    }
                    break;
                case "method.unshift":
                    if (Array.isArray(change.object)) {
                        if (change.object.length > 0) {
                            change.object.shift();
                        }
                    } else {
                        throw new Error("Invalid object type for shift method.");
                    }
                    break;
                case "method.splice":
                    if (Array.isArray(change.object)) {
                        const [start, deleteCount] = change.args;
                        change.object.splice(start, Math.max(0, change.args.length - 2));
                        change.object.splice(start, 0, ...change.deletedValues);
                    } else {
                        throw new Error("Invalid object type for splice method.");
                    }
                    break;
            }
        }
    }

    transaction(cb: () => void) {
        return () => {
            this.beginTransaction();
            try {
                cb();
                this.commitTransaction();
            }
            catch(err) {
                this.rollbackTransaction();
                throw err;
            }
        }
    }
}

export class FSDatabase extends Database {
    constructor(public path: string) {
        super();

        // should gracefully do nothing if the file does not exist
        try {
            this.loadFileSync();
        }
        catch {}
    }

    private _hasUpdates: number = 0;
    private _saveTimer?: NodeJS.Timer;
    createPeriodicSaveTimer(interval: number = 500) {
        this.removePeriodicSaveTimer();
        this._saveTimer = setInterval(() => {
            if(this._hasUpdates && ((Date.now() - this._hasUpdates) >= interval)) {
                this.saveFileSync();
                this._hasUpdates = 0;
            }
        }, interval)
    }

    removePeriodicSaveTimer() {
        if(this._saveTimer) {
            clearInterval(this._saveTimer);
            this._saveTimer = undefined;
        }
    }

    loadFile(path = this.path) {
        return super.loadFile(path);
    }

    saveFile(path = this.path) {
        return super.saveFile(path);
    }

    loadFileSync(path = this.path) {
        return super.loadFileSync(path);
    }

    saveFileSync(path = this.path) {
        return super.saveFileSync(path);
    }

    onUpdateRecord(event: DataRecordUpdateEvent): void {
        super.onUpdateRecord(event);

        this._hasUpdates = Date.now();
    }

    onUpdateTable(tableName: string, event: "added" | "removed" | "updated" | "added-index" | "removed-index", colName?: string | undefined): void {
        super.onUpdateTable(tableName, event, colName);

        this._hasUpdates = Date.now();
    }

    rollbackTransaction(): void {
        super.rollbackTransaction();

        this._hasUpdates = Date.now();
    }

    commitTransaction(): void {
        super.commitTransaction();

        this._hasUpdates = Date.now();
    }
}
