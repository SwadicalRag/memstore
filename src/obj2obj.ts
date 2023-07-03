interface EncodedValue {
    type: "string" | "number" | "boolean" | "undefined";
    val: string | number | boolean | undefined;
}

interface EncodedDate {
    type: "date";
    val: string;
    ref: number;
}

interface EncodedArray {
    type: "array";
    val: EncodedObj[];
    ref: number;
}

interface EncodedMap {
    type: "map";
    val: {key: EncodedObj, val: EncodedObj}[];
    ref: number;
}

interface EncodedSet {
    type: "set";
    val: EncodedObj[];
    ref: number;
}

interface EncodedObject {
    type: "object";
    val: {key: EncodedObj, val: EncodedObj}[];
    ref: number;
}

interface EncodedReference {
    type: "ref";
    val: number;
}

interface EncodedSymbol {
    type: "symbol";
    val?: string;
}

interface EncodedBigInt {
    type: "bigint";
    val: string;
}

interface EncodedNull {
    type: "null";
}

export type EncodedObj = EncodedValue | EncodedDate | EncodedArray | EncodedMap | EncodedSet | EncodedObject | EncodedReference | EncodedSymbol | EncodedBigInt | EncodedNull;

export class Obj2Obj {
    /**
     * Encodes any object into an EncodedObj.
     * @param {*} obj - The object to encode.
     * @param {{[key: string]: Symbol}=} symbols - Symbols reference map.
     * @param {Map<*, number>=} refs - The reference map.
     * @param {Set<*>=} blocklist - Entities to omit during encoding.
     * @return {EncodedObj} - The encoded object.
     */
    static Encode(obj: any, symbols: {[key: string]: Symbol} = {}, refs?: Map<any, number>, blocklist?: Set<any>): EncodedObj {
        const typeID = typeof obj;
        refs = refs ?? new Map();
        
        if(typeID === "object") {
            if(blocklist && blocklist.has(obj)) {
                return {type: "undefined",val: undefined};
            }
            else if(refs.has(obj)) {
                return {
                    type: "ref",
                    val: refs.get(obj)!,
                }
            }
            else if(Array.isArray(obj)) {
                let out: EncodedArray["val"] = [];

                let ref = refs.size;
                refs.set(obj,ref);

                for(let val of obj) {
                    out.push(this.Encode(val,symbols,refs,blocklist));
                }

                return {
                    type: "array",
                    val: out,
                    ref,
                }
            }
            else if(obj instanceof Date) {
                let ref = refs.size;
                refs.set(obj,ref);
                return {
                    type: "date",
                    val: obj.toISOString(),
                    ref,
                }
            }
            else if(obj instanceof Map) {
                let out: EncodedMap["val"] = [];

                let ref = refs.size;
                refs.set(obj,ref);

                for(let [key,val] of obj) {
                    out.push({
                        key: this.Encode(key,symbols,refs,blocklist),
                        val: this.Encode(val,symbols,refs,blocklist),
                    });
                }

                return {
                    type: "map",
                    val: out,
                    ref,
                }
            }
            else if(obj instanceof Set) {
                let out = [];

                let ref = refs.size;
                refs.set(obj,ref);

                for(let val of obj) {
                    out.push(this.Encode(val,symbols,refs,blocklist));
                }
                
                return {
                    type: "set",
                    val: out,
                    ref,
                }
            }
            else if(obj === null) {
                return {type: "null"};
            }
            else {
                let out: EncodedObject["val"] = [];

                let ref = refs.size;
                refs.set(obj,ref);

                for(let key in obj) {
                    out.push({
                        key: this.Encode(key,symbols,refs,blocklist),
                        val: this.Encode(obj[key],symbols,refs,blocklist),
                    });
                }

                for(let key of Object.getOwnPropertySymbols(obj)) {
                    out.push({
                        key: this.Encode(key,symbols,refs,blocklist),
                        val: this.Encode(obj[key],symbols,refs,blocklist),
                    });
                }
                
                return {
                    type: "object",
                    val: out,
                    ref,
                }
            }
        }
        else if(typeID === "symbol") {
            let seenSymbols = 0;
            for(let key in symbols) {
                seenSymbols++;
                if(symbols[key] === obj) {
                    return {type: "symbol", val: key};
                }
            }

            symbols[`__unk${seenSymbols}`] = obj;
            return {type: "symbol", val: `__unk${seenSymbols}`};
        }
        else if(typeID === "function") {
            return {type: "undefined", val: undefined};
        }
        else if(typeID === "bigint") {
            return {type: "bigint", val: obj.toString()};
        }
        else {
            // encoding for string, number, boolean and undefined types
            return {
                type: typeID,
                val: obj,
            };
        }
    }

    /**
     * Decodes an EncodedObj into its original form.
     * @param {EncodedObj} obj - The EncodedObj to decode.
     * @param {{[key: string]: Symbol}=} symbols - Symbols reference map.
     * @param {Map<number, *>=} refs - The reference map.
     * @return {*} - The decoded object.
     */
    static Decode(obj: EncodedObj, symbols: {[key: string]: Symbol} = {}, refs?: Map<number, any>): any {
        refs = refs ?? new Map();
        switch(obj.type) {
            case "array": {
                let out: any[] = [];
                refs.set(obj.ref,out);
                for(let entry of obj.val) {
                    out.push(this.Decode(entry,symbols,refs));
                }
                return out;
            }
            case "date": {
                let out = new Date(obj.val);
                refs.set(obj.ref,out);
                return out;
            }
            case "map": {
                let out = new Map();
                refs.set(obj.ref,out);
                for(let entry of obj.val) {
                    out.set(this.Decode(entry.key,symbols,refs),this.Decode(entry.val,symbols,refs));
                }
                return out;
            }
            case "object": {
                let out: any = {};
                refs.set(obj.ref,out);
                for(let entry of obj.val) {
                    out[this.Decode(entry.key,symbols,refs)] = this.Decode(entry.val,symbols,refs);
                }
                return out;
            }
            case "ref": {
                return refs.get(obj.val)!;
            }
            case "set": {
                let out = new Set();
                refs.set(obj.ref,out);
                for(let entry of obj.val) {
                    out.add(this.Decode(entry,symbols,refs));
                }
                return out;
            }
            case "symbol": {
                if(typeof obj.val === "string") {
                    if(!symbols[obj.val]) {
                        symbols[obj.val] = Symbol();
                    }
                    return symbols[obj.val];
                }

                return Symbol();
            }
            case "bigint": {
                return BigInt(obj.val);
            }
            case "null": {
                return null;
            }
            default: {
                return obj.val;
            }
        }
    }
}
