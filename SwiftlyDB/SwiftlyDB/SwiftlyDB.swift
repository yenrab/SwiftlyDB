//
//  SwiftlyDB.swift
//  SwiftlyDB
//
//  Created by Lee Barney on 10/8/14.
//  Copyright (c) 2014 Lee Barney. All rights reserved.
//

import Foundation

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
        theSwiftly = SwiftlyDB(db:theDB, queue: dispatch_queue_create("com.barney.lee.swiftlydb", DISPATCH_QUEUE_CONCURRENT))
    }
    return (anError,theSwiftly)
}

func discardSwiftly(theSwiftlyDB:SwiftlyDB){
    sqlite3_close(theSwiftlyDB.db)
}

func swiftlyTransact(aSwiftlyDB:SwiftlyDB, sql:String, parameters:Array<Storable>?, resultHandler:((DBAccessError?, AnyObject?) ->())?) -> (){
    //check if select. if not dispatch_barrier to start and complete a transaction
    
    dispatch_retain(aSwiftlyDB.queue)
    dispatch_barrier_async(aSwiftlyDB.queue){
        if sql.isSelect{
            let(tupleError,tupleResult:AnyObject?) = execRaw(aSwiftlyDB, sql, parameters, true)
            if resultHandler != nil{
                dispatch_async(dispatch_get_main_queue()){
                    resultHandler!(tupleError, tupleResult)
                }
            }
        }
        else{
            var tupleError:DBAccessError?
            var tupleResult:AnyObject?
            
            if sqlite3_exec(aSwiftlyDB.db, "BEGIN EXCLUSIVE TRANSACTION", nil, nil, nil) != SQLITE_OK{
                let error = sqlite3_errmsg(aSwiftlyDB.db)
                let errorString = String.fromCString(UnsafePointer<CChar>(error))
                tupleError = DBAccessError(errorDescription: "Unable to begin transaction. (\(errorString!))")
            }
            else{
                let (error, result:AnyObject?) = execRaw(aSwiftlyDB, sql, parameters, sql.isSelect)
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
    dispatch_release(aSwiftlyDB.queue)
}



//change this so it only executes the result handler after all tasks have been completed.

func swiftlyTransactAll(aSwiftlyDB:SwiftlyDB, tasks:Array<Dictionary<String,Array<Storable>?>>,resultHandler:((DBAccessError?, [AnyObject]?) ->())?) -> (){
    //rather than check if any of the statements are !select statements assume at least one is not. Therefore
    //dispatch_barrier to start and complete a transaction
    dispatch_retain(aSwiftlyDB.queue)
    dispatch_barrier_async(aSwiftlyDB.queue){
        var tupleError:DBAccessError?
        var tupleResult = [AnyObject]()
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
                let (error:DBAccessError?, result: AnyObject?) = execRaw(aSwiftlyDB, sql, parameters?, sql.isSelect)
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
    dispatch_release(aSwiftlyDB.queue)
}

internal func prepareAndBind(aSqliteDB:COpaquePointer, sql:String, parameters:Array<Storable>?) ->(COpaquePointer, String?){
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
                let result = sqlite3_bind_text(preparedStatement, Int32(index + 1), (parameter.asString() as NSString).UTF8String, -1, SQLITE_TRANSIENT)
            }
            else if parameter is Double{
                let result = sqlite3_bind_double(preparedStatement, Int32(index + 1), parameter.asDouble())
            }
            else if parameter is Int{
                let result = sqlite3_bind_int(preparedStatement, Int32(index + 1), (parameter.asInt() as NSNumber).intValue)
            }
            else{
                errorString = "parameter \(parameter) is an unsupported type"
                preparedStatement = nil
            }
        }
    }
    return (preparedStatement, errorString)
}

internal func execRaw(swiftlyDB:SwiftlyDB, sql:String, parameters:Array<Storable>?, isSelect:Bool) ->(DBAccessError?,AnyObject?){
    var dbError:DBAccessError?
    var result:AnyObject?
    let (statement, errorString) = prepareAndBind(swiftlyDB.db, sql, parameters)
    if errorString != nil{
        dbError = DBAccessError(errorDescription: errorString!)
    }
    else{
        if isSelect{
            if statement != nil{
                var queryResult = Array<Dictionary<String,String>>()
                let numColumns = sqlite3_column_count(statement)
                while sqlite3_step(statement) == SQLITE_ROW{
                    var row = [String:String]()
                    for index in 0..<numColumns {
                        let fieldName = sqlite3_column_name(statement, index)
                        let fieldNameString = String.fromCString(UnsafePointer<CChar>(fieldName))
                        let fieldValue = sqlite3_column_text(statement, index)
                        var fieldValueString:String? = nil
                        if fieldValue == nil{
                            fieldValueString = ""
                        }
                        else{
                            fieldValueString = String.fromCString(UnsafePointer<CChar>(fieldValue))
                        }
                        
                        row[fieldNameString!] = fieldValueString!
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




