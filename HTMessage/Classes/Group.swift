//
//  Message.swift
//  Message
//
//  Created by hublot on 2018/2/9.
//

import UIKit
import HTSQLite

let center = CFNotificationCenterGetDarwinNotifyCenter()

let notifacationIdentifier = "com.hublot.cfnotifacation"

func noticationCallBack(center: CFNotificationCenter?, observer: UnsafeMutableRawPointer?, name: CFNotificationName?, object: UnsafeRawPointer?, userInfo: CFDictionary?) -> Swift.Void {
	if let name = name {
		NotificationCenter.default.post(name: NSNotification.Name(rawValue: notifacationIdentifier),
										object: name.rawValue as String)
	}
}

func listenNotifacation(_ identifier: String) {
	CFNotificationCenterAddObserver(center, nil, noticationCallBack, identifier as CFString, nil, .deliverImmediately)
}

func sendNotifacation(_ identifier: String) {
	CFNotificationCenterPostNotificationWithOptions(center, CFNotificationName.init(identifier as CFString), nil, nil, kCFNotificationDeliverImmediately)
}



open class Group {
	
	open let groupIndentifier: String
	
	open let userDefault: UserDefaults
	
	open let groupURL: URL
	
	public typealias MessageHandler = (_ identifier: String, _ message: String) -> Void

	open var callbackList = [String: [MessageHandler]]()
	
	open var sqlite: SQLite
	
	open var lastInsertList = [Int?]()
	
	open var defaultMinInsert = -1
	
	open static var messageQueue = DispatchQueue.init(label: "com.hublot.message.messageQueue")
	
	open static var callbackQueue = DispatchQueue.init(label: "com.hublot.message.callbackQueue")
	
	deinit {
		NotificationCenter.default.removeObserver(self)
	}
	
	public init?(_ groupIndentifier: String) {
		self.groupIndentifier = groupIndentifier
		guard let userDefault = UserDefaults.init(suiteName: groupIndentifier) else {
			return nil
		}
		guard let groupURL = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: groupIndentifier) else {
			return nil
		}
		self.userDefault = userDefault
		self.groupURL = groupURL
		let path = groupURL.appendingPathComponent("com.hublot.message.sqlite").path
		let create = """
			create table if not exists message (
				id integer primary key autoincrement,
				identifier text default '' not null,
				message text default '' not null
			)
		"""
		self.sqlite = SQLite.init(path: path, create: create)
		NotificationCenter.default.addObserver(self,
											   selector: #selector(notifacationObserver(_:)),
											   name: NSNotification.Name(rawValue: notifacationIdentifier),
											   object: nil)
	}
	
	@objc
	open func notifacationObserver(_ notifacation: Notification) {
		if let object = notifacation.object, let identifier = object as? String {
			let list = callbackList[identifier] ?? []
			for callback in list {
				callback(identifier, "")
			}
		}
	}
	
	open func ssveToUserDefault(_ key: String, _ value: Any?) {
		userDefault.setValue(value, forKey: key)
	}
	
	open func valueFromUserDefault(_ key: String) -> Any? {
		return userDefault.value(forKey: key)
	}
	
	open func _post(_ identifier: String) {
		let fullidentifier = groupIndentifier + identifier
		sendNotifacation(fullidentifier)
	}
	
	open func post(_ identifier: String, _ message: String = "") {
		type(of: self).messageQueue.async {
			self.syncPost(identifier, message)
		}
	}
	
	open func syncPost(_ identifier: String, _ message: String = "") {
		let keyValueDictionary = ["identifier": identifier,
								  "message": message]
		var bind = SQLBind.insert(keyValueDictionary)
		bind = "insert into message " + bind
		if self.sqlite.execute(bind) != nil {
			self._post(identifier)
		}
	}
	
	private func _listen(identifier: String, _ handler: @escaping MessageHandler) {
		let fullidentifier = groupIndentifier + identifier
		if callbackList[fullidentifier] == nil {
			callbackList[fullidentifier] = [MessageHandler]()
		}
		let rehandler: MessageHandler = { _, message in
			handler(identifier, message)
		}
		callbackList[fullidentifier]?.append(rehandler)
		listenNotifacation(fullidentifier)
	}
	
	open func listen(identifier: String, _ handler: @escaping MessageHandler) {
		let selfclass = type(of: self)
		selfclass.messageQueue.async {
			let index = self.lastInsertList.count
			let result = self.sqlite.execute("select max(id) from message where identifier = '\(identifier)'")
			if let first = result?.first, let count = first["max(id)"], let id = Int(count) {
				self.lastInsertList.append(id)
			} else {
				self.lastInsertList.append(self.defaultMinInsert)
			}
			let rehandler: MessageHandler = { identifier, _ in
				let last = String(self.lastInsertList[index] ?? self.defaultMinInsert)
				let result = self.sqlite.execute("select id, message from message where identifier = '\(identifier)' and id > \(last)") ?? [[String: String]]()
				for row in result {
					guard let count = row["id"], let id = Int(count), let message = row["message"]  else {
						continue
					}
					self.lastInsertList[index] = id
					selfclass.callbackQueue.async {
						handler(identifier, message)
					}
				}
			}
			self._listen(identifier: identifier, rehandler)
		}
	}
	
	open func clear() {
		self.sqlite.execute(SQLBind.init("delete from message"))
		self.lastInsertList.removeAll()
	}
	
}
