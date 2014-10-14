//
//  String+SQL.swift
//  SwiftlyDB
//
//  Created by Lee Barney on 10/10/14.
//  Copyright (c) 2014 Lee Barney. All rights reserved.
//

import Foundation

extension String{
    var isSelect:Bool {
        return self.lowercaseString.hasPrefix("select")
    }
}