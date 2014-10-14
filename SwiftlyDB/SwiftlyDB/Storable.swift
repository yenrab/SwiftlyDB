//
//  Storable.swift
//  SwiftlyDB
//
//  Created by Lee Barney on 10/13/14.
//  Copyright (c) 2014 Lee Barney. All rights reserved.
//

import Foundation

protocol Storable {
    func asDouble() -> Double?
    func asInt() -> Int?
    func asString() -> String?
}
extension Int:Storable {
    func asDouble() -> Double?{
        return Double(self)
    }
    func asInt() -> Int?{
        return self
    }
    func asString() -> String?{
        return "\(self)"
    }
}
extension Double:Storable {
    func asDouble() -> Double?{
        return self
    }
    func asInt() -> Int?{
        return Int(self)
    }
    func asString() -> String?{
        return "\(self)"
    }
}
extension String:Storable{
    
    func asDouble() -> Double?{
        let notDigits = NSCharacterSet.decimalDigitCharacterSet().invertedSet
        if (self.rangeOfCharacterFromSet(notDigits)?.isEmpty == false){
            return (self as NSString).doubleValue
        }
        return nil
    }
    func asInt() -> Int?{
        let notDigits = NSCharacterSet.decimalDigitCharacterSet().invertedSet
        if (self.rangeOfCharacterFromSet(notDigits)?.isEmpty == false){
            return (self as NSString).integerValue
        }
        return nil
    }
    func asString() -> String?{
        return self
    }
}
