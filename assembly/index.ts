import {GetDataByRID, JSON, Log, ExecSQL, QuerySQL} from "@w3bstream/wasm-sdk";
import {Int64, String, Time} from "@w3bstream/wasm-sdk/assembly/sql";


export {alloc} from "@w3bstream/wasm-sdk";

function extractHeader(rid: i32): JSON.Obj | null {
    const message_string: string = GetDataByRID(rid);
    const json_object: JSON.Obj = JSON.parse(message_string) as JSON.Obj;
    return json_object.getObj("header") as JSON.Obj;
}

function extractPayload(rid: i32): JSON.Obj | null {
    const message_string: string = GetDataByRID(rid);
    const json_object: JSON.Obj = JSON.parse(message_string) as JSON.Obj;
    return json_object.getObj("payload") as JSON.Obj;
}

function findDeviceByIndividual(individualName: JSON.Str): JSON.Integer | null {

    const queryResult: string = QuerySQL(`SELECT c_id FROM "t_device" WHERE c_vr_individual_name = ?;`
        , [new String(individualName.valueOf())]);

    let result: JSON.Integer | null = null;
    if (queryResult !== "") {
        let parsedObject: JSON.Obj = JSON.parse(queryResult) as JSON.Obj;
        result = parsedObject.getInteger("c_id");
        if (result) {
            Log("Find device result: found id is: " + result.valueOf().toString());
        } else {
            Log("Find device result: Unsuccessful!!!");
        }
        return result;
    }
    Log("Find device result: Unsuccessful!!!");
    return result;

}

function updateDeviceStatus(deviceId: JSON.Integer, deviceStatus: string): boolean {
    const executeResult: number = ExecSQL(`UPDATE "t_device" set c_status = ? where c_id = ?;`,
        [
            new String(deviceStatus),
            new Int64(deviceId.valueOf())
        ]);
    if (executeResult != 0) {
        Log("Update device status result: Unsuccessful!!!");
        return false;
    } else {
        Log("Update device status result: Successful!!!");
        return true;
    }
}

function registerDevice(deviceIdInteger: JSON.Integer, individualName: JSON.Str): JSON.Integer {
    const executeResult: number = ExecSQL(`INSERT INTO "t_device" (c_id, c_vr_individual_name, c_status) VALUES (?,?,?);`,
        [
            new Int64(deviceIdInteger.valueOf()),
            new String(individualName.valueOf()),
            new String("REGISTERED")
        ]);
    if (executeResult != 0) {
        Log("Insert device status result: Unsuccessful!!!");
    } else {
        Log("Insert device status result: Successful!!!");
    }
    return deviceIdInteger;
}

function extractDeviceIdInteger(object: JSON.Obj): JSON.Integer | null {
    let deviceIdInteger: JSON.Integer | null = object.getInteger("deviceId");
    if (!deviceIdInteger) {
        Log("Error: Device id did not receive!!!");
    }
    return deviceIdInteger;
}

export function establishDevice(rid: i32): i32 {

    const message_string: string = GetDataByRID(rid);
    const json_object: JSON.Obj = JSON.parse(message_string) as JSON.Obj;
    const headerObject: JSON.Obj | null = json_object.getObj("header") as JSON.Obj;
    const payloadObject: JSON.Obj | null = json_object.getObj("payload") as JSON.Obj;

    if (headerObject && payloadObject) {
        const individualNameString: JSON.Str | null = payloadObject.getString("individualName");
        if (!individualNameString) {
            Log("Error: Individual name did not receive!!!")
            return 0;
        }
        const deviceIdInteger: JSON.Integer | null = extractDeviceIdInteger(payloadObject);
        if (deviceIdInteger) {
            let foundDeviceId: JSON.Integer | null = findDeviceByIndividual(individualNameString);
            if (foundDeviceId) {
                if (deviceIdInteger && deviceIdInteger.valueOf() !== foundDeviceId.valueOf()) {
                    Log("Error: Device id did not match with the registered name!!!")
                    return 0;
                }
                updateDeviceStatus(foundDeviceId, "ESTABLISHED");
            } else {
                let registeredDeviceId: JSON.Integer = registerDevice(deviceIdInteger, individualNameString);
                if (registeredDeviceId) {
                    updateDeviceStatus(registeredDeviceId, "ESTABLISHED");
                }
            }
        }
        return 0;
    }

    Log("Error: Payload object did not receive!!!")
    return 0;
}


function isDeviceValid(deviceId: JSON.Integer | null): boolean {

    let result: boolean = false;
    if (deviceId) {
        const queryResult: string = QuerySQL(`SELECT c_id FROM "t_device" WHERE c_id = ? and c_status = 'ESTABLISHED' and c_is_active = 'true';`
            , [new Int64(deviceId.valueOf())]);
        if (queryResult !== "") {
            let parsedObject: JSON.Obj = JSON.parse(queryResult) as JSON.Obj;
            let foundId: JSON.Integer | null = parsedObject.getInteger("c_id");
            if (foundId && foundId.valueOf() === deviceId.valueOf()) {
                Log("Device is valid with id: " + foundId.valueOf().toString());
                result = true;
                return result;
            }
        }
    }
    Log("Device is not valid!!!");
    return result;

}

function getTableIndex(tableName: string): JSON.Integer | null {
    const queryResult: string = QuerySQL(`SELECT c_index FROM "t_incremental_index" WHERE c_table_name = ?;`
        , [new String(tableName)]);

    let result: JSON.Integer | null = null;
    Log(queryResult);
    if (queryResult !== "") {
        let parsedObject: JSON.Obj = JSON.parse(queryResult) as JSON.Obj;
        result = parsedObject.getInteger("c_index");
        if (result) {
            const updateIndexResult: number = ExecSQL(`UPDATE "t_incremental_index" set c_index = ? WHERE c_table_name = ?;`
                , [
                    new Int64(result.valueOf() + 1),
                    new String(tableName)]);

            if (updateIndexResult == 0) {
                return result;
            }
        }
        result = null;
    }
    return result;
}

function fetchObservation(): i32 {
    const queryResult: string = QuerySQL(`SELECT * FROM "t_observation" where c_status = 'NEW'`);
    Log(queryResult);
    if (queryResult !== "") {
        const json_object: JSON.Obj = JSON.parse(queryResult) as JSON.Obj;
        // Log(json_object);
    }
    return 0;
}

function registerObservation(deviceIdInteger: JSON.Integer, temperatureCentigradeString: JSON.Str, actualTime: JSON.Str): JSON.Integer | null {
    let tableIndex: JSON.Integer | null = getTableIndex("t_observation");
    if (tableIndex) {
        const executeResult: number = ExecSQL(`INSERT INTO "t_observation" (c_id, f_device_id, c_temperature_centigrade, c_actual_time) VALUES (?,?,?,?);`,
            [
                new Int64(tableIndex.valueOf()),
                new Int64(deviceIdInteger.valueOf()),
                new String(temperatureCentigradeString.valueOf()),
                new Time(actualTime.valueOf())
            ]);
        if (executeResult != 0) {
            Log("Insert observation status result: Unsuccessful!!!");
        } else {
            Log("Insert observation status result: Successful!!!");
        }
        return deviceIdInteger;
    }
    return null;
}

export function observeTemperature(rid: i32): i32 {

    const message_string: string = GetDataByRID(rid);
    const json_object: JSON.Obj = JSON.parse(message_string) as JSON.Obj;
    const header: JSON.Obj | null = json_object.getObj("header") as JSON.Obj;
    const payload: JSON.Obj | null = json_object.getObj("payload") as JSON.Obj;

    if (header && payload) {
        let deviceIdInteger: JSON.Integer | null = extractDeviceIdInteger(payload);
        if (!isDeviceValid(deviceIdInteger)) {
            Log("Error: Device is not valid to observe!!!");
            return 0;
        }
        const temperatureCentigradeString: JSON.Str | null = payload.getString("temperatureCentigrade");
        const actualTime: JSON.Str | null = payload.getString("actualTime");
        if (!actualTime) {
            Log("Error: Actual time did not receive!!!");
            return 0;
        }
        if (!temperatureCentigradeString) {
            Log("Error: Temperature did not receive correctly!!!");
            return 0;
        }
        if (deviceIdInteger) {
            registerObservation(deviceIdInteger, temperatureCentigradeString, actualTime);
            // fetchObservation();
            return 0;
        }
    }

    Log("Error: Header or Payload object did not receive!!!");
    return 0;
}