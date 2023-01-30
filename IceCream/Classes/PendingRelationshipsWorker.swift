//
//  File.swift
//  
//
//  Created by Soledad on 2021/2/7.
//

import Foundation
import RealmSwift

/// PendingRelationshipsWorker is responsible for temporarily storing relationships when objects recovering from CKRecord
final class PendingRelationshipsWorker<Element: Object> {
    
    var realm: Realm?
    var db: DatabaseManager?
    
    var pendingListElementPrimaryKeyValue: [AnyHashable: (String, Object)] = [:]
    
    func addToPendingList(elementPrimaryKeyValue: AnyHashable, propertyName: String, owner: Object) {
        pendingListElementPrimaryKeyValue[elementPrimaryKeyValue] = (propertyName, owner)
    }
    
    func resolvePendingListElements() {
        guard let realm = realm, pendingListElementPrimaryKeyValue.count > 0 else {
            // Maybe we could add one log here
            return
        }
        BackgroundWorker.shared.start {
            for (primaryKeyValue, (propName, owner)) in self.pendingListElementPrimaryKeyValue {
                guard let list = owner.value(forKey: propName) as? List<Element> else { continue }
                
                if let existListElementObject = realm.object(ofType: Element.self, forPrimaryKey: primaryKeyValue) {
                    try! realm.write {
                        list.append(existListElementObject)
                    }
                } else {
                    // list item hasn't been downloaded, so fetch it from cloud
                    if let pdb = self.db as? PublicDatabaseManager,
                        let recordName = (primaryKeyValue as? String) ?? (primaryKeyValue as? ObjectId)?.stringValue {
                        pdb.fetchChangesInDatabase(forRecordType: Element.className(), andNames: [recordName]) { error in
                            if let err = error {
                                print("== Failed to resolve record \(primaryKeyValue) of \(Element.self): \(err)")
                            } else { // link it back to list
                                BackgroundWorker.shared.start {
                                    if let o = realm.object(ofType: Element.self, forPrimaryKey: primaryKeyValue) {
                                        try! realm.write {
                                            list.append(o)
                                        }
                                        print("== Patch resolved record \(primaryKeyValue) of \(Element.self) to \(String(describing: owner.value(forKey: "_id")))")
                                    }
                                }
                            }
                        }
                    } else {
                        print("== Failed to setup remote fetch")
                    }
                }
                self.pendingListElementPrimaryKeyValue[primaryKeyValue] = nil
            }
        }
    }
    
}
