/*
Copyright (c) 2014 Lee Barney
Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


*/

import Foundation
//the C version of SQLITE_TRANSIENT is not available from Swift. Create it here.
let SQLITE_TRANSIENT = sqlite3_destructor_type(COpaquePointer(bitPattern: -1))

struct SwiftlyDB {
    var db:COpaquePointer
    var queue:dispatch_queue_t
}
struct DBAccessError {
    var errorDescription:String
}

func setupSwiftly(theDBName:String)->(DBAccessError?,SwiftlyDB?){
    var anError:DBAccessError?
    let documentsDirectory = NSSearchPathForDirectoriesInDomains(NSSearchPathDirectory.DocumentDirectory, NSSearchPathDomainMask.UserDomainMask, true)[0] as String
    let fileLocation = documentsDirectory.stringByAppendingPathComponent(theDBName)
    if !NSFileManager.defaultManager().fileExistsAtPath(fileLocation){
        let sourceLocation = NSBundle.mainBundle().resourcePath?.stringByAppendingPathComponent(theDBName)
        if sourceLocation != nil
            && NSFileManager.defaultManager().fileExistsAtPath(sourceLocation!){
                //copy existing db to documents directory
                var error:NSErrorPointer = nil
                NSFileManager.defaultManager().copyItemAtPath(sourceLocation!, toPath: fileLocation, error: error)
        }
        
    }
    //either the db file exists in the documents directory or opening the db will create it
    var theSwiftly:SwiftlyDB? = nil
    var theDB:COpaquePointer = nil
    if sqlite3_open((fileLocation as NSString).cStringUsingEncoding(NSUTF8StringEncoding), &theDB) != SQLITE_OK{
        anError = DBAccessError(errorDescription: "Unable to open database")
    }
    else{
        theSwiftly = SwiftlyDB(db:theDB, queue: dispatch_queue_create("com.barney.lee.swiftlydb", DISPATCH_QUEUE_SERIAL))
    }
    return (anError,theSwiftly)
}

func discardSwiftly(theSwiftlyDB:SwiftlyDB){
    sqlite3_close(theSwiftlyDB.db)
}

func swiftlyTransact(aSwiftlyDB:SwiftlyDB, sql:String, parameters:[Storable]?, resultHandler:((DBAccessError?, Any?) ->())?) -> (){
    //check if select. if not dispatch_barrier to start and complete a transaction
    
    dispatch_async(aSwiftlyDB.queue){
        if sql.isSelect{
            let(tupleError,tupleResult:Any?) = execRaw(aSwiftlyDB, sql, parameters, true)
            if resultHandler != nil{
                dispatch_async(dispatch_get_main_queue()){
                    resultHandler!(tupleError, tupleResult)
                }
            }
        }
        else{
            var tupleError:DBAccessError?
            var tupleResult:Any?
            
            if sqlite3_exec(aSwiftlyDB.db, "BEGIN EXCLUSIVE TRANSACTION", nil, nil, nil) != SQLITE_OK{
                let error = sqlite3_errmsg(aSwiftlyDB.db)
                let errorString = String.fromCString(UnsafePointer<CChar>(error))
                tupleError = DBAccessError(errorDescription: "Unable to begin transaction. (\(errorString!))")
            }
            else{
                let (error, result:Any?) = execRaw(aSwiftlyDB, sql, parameters, sql.isSelect)
                //end transaction
                if error != nil || sqlite3_exec(aSwiftlyDB.db, "END TRANSACTION", nil, nil, nil) != SQLITE_OK{
                    //!!!pull the database error out here and use it in the error description!!!
                    sqlite3_exec(aSwiftlyDB.db, "ROLLBACK", nil, nil, nil)
                    tupleError = DBAccessError(errorDescription: "Transaction failed (\(error?.errorDescription)). Rolling back")
                }
                else{
                    tupleResult = result
                }
            }
            if resultHandler != nil{
                dispatch_async(dispatch_get_main_queue()){
                    resultHandler!(tupleError, tupleResult)
                }
            }
        }
    }
}



//change this so it only executes the result handler after all tasks have been completed.

func swiftlyTransactAll(aSwiftlyDB:SwiftlyDB, tasks:[[String:[Storable]?]],resultHandler:((DBAccessError?, [Any]?) ->())?) -> (){
    //rather than check if any of the statements are !select statements assume at least one is not. Therefore
    //dispatch_barrier to start and complete a transaction
    dispatch_async(aSwiftlyDB.queue){
        var tupleError:DBAccessError?
        var tupleResult = [Any]()
        //begin transaction
        if sqlite3_exec(aSwiftlyDB.db, "BEGIN EXCLUSIVE TRANSACTION", nil, nil, nil) != SQLITE_OK{
            let error = sqlite3_errmsg(aSwiftlyDB.db)
            let errorString = String.fromCString(UnsafePointer<CChar>(error))
            tupleError = DBAccessError(errorDescription: "Unable to begin transaction. (\(errorString!))")
        }
        else{
            for task in tasks{
                
                let keyList = task.keys
                //there is only one key in the dictionary, the sql.
                let sql = keyList.first!
                let parameters = task[sql]
                let (error:DBAccessError?, result: Any?) = execRaw(aSwiftlyDB, sql, parameters?, sql.isSelect)
                if error != nil{
                    tupleError = error
                    break
                }
                else{
                    if result != nil{
                        tupleResult.append(result!)
                    }
                }
            }
            //end transaction
            if tupleError != nil || sqlite3_exec(aSwiftlyDB.db, "END TRANSACTION", nil, nil, nil) != SQLITE_OK{
                //!!!pull the database error out here and use it in the error description!!!
                sqlite3_exec(aSwiftlyDB.db, "ROLLBACK", nil, nil, nil)
                tupleError = DBAccessError(errorDescription: "Transaction failed (\(tupleError!.errorDescription)). Rolling back")
            }
        }
        dispatch_async(dispatch_get_main_queue()){
            if resultHandler != nil{
                resultHandler!(tupleError, tupleResult)
            }
        }
    }
}

internal func prepareAndBind(aSqliteDB:COpaquePointer, sql:String, parameters:[Storable]?) ->(COpaquePointer, String?){
    var preparedStatement:COpaquePointer = nil
    var errorString:String?
    if sqlite3_prepare_v2(aSqliteDB, sql, -1, &preparedStatement, nil) != SQLITE_OK{
        let error = sqlite3_errmsg(aSqliteDB)
        errorString = String.fromCString(UnsafePointer<CChar>(error))
    }
    let requiredParameterCount = Int(sqlite3_bind_parameter_count(preparedStatement))
    if requiredParameterCount > 0 && parameters == nil{
        errorString = "\(sql) has missing parameters array"
    }
    else if parameters != nil && requiredParameterCount != parameters!.count{
        errorString = "\(sql) requires \(requiredParameterCount) parameters. Only \(parameters!.count) provided"
    }
    else if parameters != nil{
        for index in 0..<parameters!.count{
            let parameter:Storable = parameters![index]
            if parameter is String{
                let result = sqlite3_bind_text(preparedStatement, Int32(index + 1), (parameter.asString()! as NSString).UTF8String, -1, SQLITE_TRANSIENT)
            }
            else if parameter is Double{
                let doubleOptional = parameter.asDouble()
                if doubleOptional != nil{
                    let result = sqlite3_bind_double(preparedStatement, Int32(index + 1), doubleOptional!)
                }
            }
            else if parameter is Int{
                let intOptional = parameter.asInt()
                if intOptional != nil{
                    let result = sqlite3_bind_int(preparedStatement, Int32(index + 1), (intOptional! as NSNumber).intValue)
                }
            }
            else{
                errorString = "parameter \(parameter) is an unsupported type"
                preparedStatement = nil
            }
        }
    }
    return (preparedStatement, errorString)
}

internal func execRaw(swiftlyDB:SwiftlyDB, sql:String, parameters:[Storable]?, isSelect:Bool) ->(DBAccessError?,Any?){
    var dbError:DBAccessError?
    var result:Any?
    let (statement, errorString) = prepareAndBind(swiftlyDB.db, sql, parameters)
    if errorString != nil{
        dbError = DBAccessError(errorDescription: errorString!)
    }
    else{
        if isSelect{
            if statement != nil{
                var queryResult = [[String:Any?]]()
                let numColumns = sqlite3_column_count(statement)
                while sqlite3_step(statement) == SQLITE_ROW{
                    var row = [String:Any?]()
                    for index in 0..<numColumns {
                        let fieldName = sqlite3_column_name(statement, index)
                        let columnType = sqlite3_column_type(statement, index)
                        let fieldNameString = String.fromCString(UnsafePointer<CChar>(fieldName))
                        let fieldValue = sqlite3_column_text(statement, index)
                        var fieldValueOptional:Any? = nil
                        if fieldValue == nil{
                            fieldValueOptional = nil
                        }
                        else{
                            if columnType == SQLITE_INTEGER{
                                let intString = String.fromCString(UnsafePointer<CChar>(fieldValue))
                                fieldValueOptional = intString?.toInt()
                                
                            }
                            else if columnType == SQLITE_FLOAT{
                                let doubleStringOptional = String.fromCString(UnsafePointer<CChar>(fieldValue))
                                if doubleStringOptional != nil{
                                    let doubleString = (doubleStringOptional! as NSString)
                                    let aDoubleValueString = (doubleString as NSString)
                                    fieldValueOptional = aDoubleValueString.doubleValue
                                }
                            }
                            else if columnType == SQLITE_NULL{
                                fieldValueOptional = nil
                            }
                            else{
                                fieldValueOptional = String.fromCString(UnsafePointer<CChar>(fieldValue))
                            }
                        }
                        
                        row[fieldNameString!] = fieldValueOptional
                    }
                    queryResult.append(row)
                }
                result  = queryResult
            }
        }
        else if statement != nil{
            if sqlite3_step(statement) == SQLITE_DONE {
                result = Int(sqlite3_changes(swiftlyDB.db))
            }
            else{
                let error = sqlite3_errmsg(statement)
                let errorString = String.fromCString(UnsafePointer<CChar>(error))
                dbError = DBAccessError(errorDescription:errorString!)
            }
        }
    }
    return (dbError, result)
}




