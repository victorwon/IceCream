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
                    self.pendingListElementPrimaryKeyValue[primaryKeyValue] = nil
                }
            }
            
            // for items hasn't been downloaded, fetch them from cloud
            if self.pendingListElementPrimaryKeyValue.count > 0 {
                let pks = self.pendingListElementPrimaryKeyValue.compactMap { ($0.0 as? ObjectId)?.stringValue ?? $0.0 as? String }
                self.pendingListElementPrimaryKeyValue.removeAll() // clean the pending before fetch as it could come back recursively
                if let pdb = self.db as? PublicDatabaseManager {
                    pdb.fetchChangesInDatabase(forRecordType: Element.className(), andNames: pks) { error in
                        if let err = error {
                            print("++ Failed to resolve records:", Element.self, err)
                        }
                    }
                } // else TODO: private database
            }
        }
    }
    
}
