//
//  SQLite.swift
//  Alamofire
//
//  Created by hublot on 2017/12/21.
//

import Foundation
import SQLite3

open class SQLite {
	
	open static var queue = DispatchQueue.init(label: "com.hublot.sqlite.queue")
	
	open var sqlite: OpaquePointer?
	
	public init(path: String, create: String? = nil) {
		let path = path.cString(using: .utf8)
		sqlite3_open(path, &sqlite)
		if let create = create {
			execute(create)
		}
	}
	
	@discardableResult
	open func select(_ string: String) -> [[String:String]] {
		guard let pointer = sqlite else {
			return [[:]]
		}
		var result = [[String:String]]()
		var stmt: OpaquePointer?
		let sql = string.cString(using: .utf8)
		if sqlite3_prepare_v2(pointer, sql, -1, &stmt, nil) == SQLITE_OK {
			while sqlite3_step(stmt) == SQLITE_ROW {
				let column = sqlite3_column_count(stmt)
				var dictionary = [String:String]()
				for i in 0..<column {
					let name = sqlite3_column_name(stmt, i)
					let key: String = String(validatingUTF8: name!)!
					let text = sqlite3_column_text(stmt, i)
					var value: String?
					if let text = text {
						value = String(cString: text)
						dictionary[key] = value
					}
				}
				result.append(dictionary)
			}
		}
		return result
	}
	
	@discardableResult
	open func execute(_ string: String) -> Bool {
		guard let pointer = sqlite else {
			return false
		}
		let sql = string.cString(using: .utf8)
		let error: UnsafeMutablePointer<UnsafeMutablePointer<Int8>?>? = nil
		if sqlite3_exec(pointer, sql, nil, nil, error) == SQLITE_OK {
			return true
		} else {
			return false
		}
	}
	
	open func close() {
		guard let pointer = sqlite else {
			return
		}
		sqlite3_close(pointer)
	}
	
}

extension SQLite {
	
	open class TableColumn {
		let name: String
		let type: String
		let notnull: Int
		var dflt_value: String?
		let pk: Int
		init(name: String, type: String, notnull: String, dflt_value: String?, pk: String) {
			self.name = name
			self.type = type
			self.notnull = Int(notnull) ?? 1
			self.dflt_value = dflt_value
			self.pk = Int(pk) ?? 0
		}
	}
	
	open func tableInfo(tableName: String) -> [TableColumn] {
		var columnArray = [TableColumn]()
		let sql = "pragma table_info('\(tableName)')"
		let result = select(sql)
		for row in result {
			let name = row["name"] ?? ""
			let type = row["type"] ?? ""
			var notnull = row["notnull"] ?? ""
			var dflt_value: String?
			if let defaultValue = row["dflt_value"] {
				dflt_value = defaultValue
			}
			let pk = row["pk"] ?? ""
			
			if type.caseInsensitiveCompare("INTEGER").rawValue == 0 {
				notnull = "0"
			}
			let model = TableColumn(name: name, type: type, notnull: notnull, dflt_value: dflt_value, pk: pk)
			columnArray.append(model)
		}
		return columnArray
	}
	
	open func validate(columnArray: [TableColumn], dictionary: [String:String]) -> Bool {
		for column in columnArray {
			var value: String?
			value = dictionary[column.name]
			if let _ = value {
			} else {
				if (column.notnull != 0 && nil == column.dflt_value) {
					return false
				}
			}
		}
		return true
	}
	
	open func valueFromKey(key: String, dictionary: [String:String]) -> String {
		var value: String
		if let text = dictionary[key] {
			value = "'\(text)'"
		} else {
			value = "null"
		}
		return value
	}
	
	@discardableResult
	open func update(tableName: String, columnArray: [TableColumn], dictionary: [String:String]) -> Bool {
		guard validate(columnArray: columnArray, dictionary: dictionary) else {
			return false
		}
		var whereDictionary = [String:String]()
		var keyValeuDictionary = [String:String]()
		for column in columnArray {
			let key = column.name
			let value = valueFromKey(key: key, dictionary: dictionary)
			keyValeuDictionary[key] = value
			if column.pk == 1 {
				whereDictionary[key] = value
			}
		}
		let whereString: String = {
			var whereString = ""
			var whereArray = [String]()
			for (whereKey, whereValue) in whereDictionary {
				whereArray.append("\(whereKey) = \(whereValue)")
			}
			if whereArray.count > 0 {
				whereString = " where " + whereArray.joined(separator: " and ")
			}
			return whereString
		}()
		
		let existRow: Bool = {
			var exist = false
			if whereString.count > 0 {
				let existSql = "select * from '\(tableName)'" + whereString
				if (select(existSql)).count > 0 {
					exist = true
				}
			}
			return exist
		}()
		
		if existRow {
			let keyValueString: String = {
				var keyValueString = ""
				var keyValueArray = [String]()
				for (key, value) in keyValeuDictionary {
					keyValueArray.append("\(key) = \(value)")
				}
				if keyValueArray.count > 0 {
					keyValueString = " set " + keyValueArray.joined(separator: ", ")
				}
				return keyValueString
			}()
			let updateSql = "update '\(tableName)'" + keyValueString + whereString
			return execute(updateSql)
		} else {
			var keyArray = [String]()
			var valueArray = [String]()
			for (key, value) in keyValeuDictionary {
				keyArray.append(key)
				valueArray.append(value)
			}
			let keyString = " '\(tableName)' (" + keyArray.joined(separator: ", ") + ")"
			let valueString = " values (" + valueArray.joined(separator: ", ") + ")"
			let insertSql = "insert into " + keyString + valueString
			return execute(insertSql)
		}
	}
	
	@discardableResult
	open func update(tableName: String, array: [[String:String]]) -> Bool {
		let columnArray = tableInfo(tableName: tableName)
		for dictionary in array {
			let success = update(tableName: tableName, columnArray: columnArray, dictionary: dictionary)
			if success == false {
				return false
			}
		}
		return true
	}
}
