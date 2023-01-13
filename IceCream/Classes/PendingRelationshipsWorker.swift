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
                    print("== try fetching unresolved record \(primaryKeyValue) of \(Element.self) in Cloud")
                    // try get them from cloud
                    if let pdb = self.db as? PublicDatabaseManager,
                        let recordName = (primaryKeyValue as? String) ?? (primaryKeyValue as? ObjectId)?.stringValue {
                        pdb.fetchChangesInDatabase(forRecordType: Element.className(), andName: recordName) { error in
                            if let err = error {
                                print("== failed unresolving record \(primaryKeyValue) of \(Element.self): \(err)")
                            } else { // link it back to owner
                                BackgroundWorker.shared.start {
                                    if let o = realm.object(ofType: Element.self, forPrimaryKey: primaryKeyValue) {
                                        try! realm.write {
                                            list.append(o)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                self.pendingListElementPrimaryKeyValue[primaryKeyValue] = nil
            }
        }
    }
    
}
