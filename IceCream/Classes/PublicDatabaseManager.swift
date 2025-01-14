//
//  PublicDatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

#if os(macOS)
import Cocoa
#else
import UIKit
#endif

import CloudKit

public final class PublicDatabaseManager: DatabaseManager {

    public let container: CKContainer
    public let database: CKDatabase
    
    public var syncObjects: [Syncable]
    public let qos: QualityOfService
    
    public init(objects: [Syncable], container: CKContainer, qualityOfService: QualityOfService) {
        self.syncObjects = objects
        self.container = container
        self.database = container.publicCloudDatabase
        self.qos = qualityOfService
    }
    
    public func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        syncObjects.forEach { [weak self] syncObject in
            let predicate = NSPredicate(value: true)
            let query = CKQuery(recordType: syncObject.recordType, predicate: predicate)
            let queryOperation = CKQueryOperation(query: query)
            self?.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: callback)
        }
    }
    
    public func fetchChangesInDatabase(forRecordType recordType: String, andNames recordNames: [String], _ callback: ((Error?) -> Void)?) {
        let predicate = recordNames.isEmpty ? NSPredicate(value: true) : NSPredicate(format: "recordID IN %@", recordNames.map { CKRecord.ID(recordName: $0) } )
        syncObjects.forEach { [weak self] syncObject in
            if syncObject.recordType == recordType {
                let query = CKQuery(recordType: syncObject.recordType, predicate: predicate)
                let queryOperation = CKQueryOperation(query: query)
                self?.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: callback)
                return
            }
        }
        
    }
    
    public func fetchChangesInDatabase(queryOperation: CKQueryOperation,on syncObject: Syncable, _ callback: ((Error?) -> Void)?) {
        self.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: callback)
    }
    
    public func createCustomZonesIfAllowed() {
        
    }
    
    public func createDatabaseSubscriptionsForAll() {
        #if os(iOS) || os(tvOS) || os(macOS)
        syncObjects.forEach { createSubscriptionInPublicDatabase(on: $0, with: NSPredicate(value: true)) }
        #endif
    }
    
    public func createSubscriptionInPublicDatabase(on syncObject: Syncable, with predicate: NSPredicate?,
                                                   args: [String]? = nil,
                                                   options: CKQuerySubscription.Options = [CKQuerySubscription.Options.firesOnRecordCreation,
                                                                                           CKQuerySubscription.Options.firesOnRecordUpdate,
                                                                                           CKQuerySubscription.Options.firesOnRecordDeletion] ) {
        #if os(iOS) || os(tvOS) || os(macOS)
        let subId = IceCreamSubscription.PREFIX + syncObject.recordType
        // subscription needs to be updated when app becomes/resigns active, it's dynamic, can't be cached.
        print("== Registering subscription:", subId)
        // must delete old one if any, otherwise it won't update
        let deleteOp = CKModifySubscriptionsOperation(subscriptionsToSave: nil, subscriptionIDsToDelete: [subId])
        deleteOp.modifySubscriptionsCompletionBlock = { _, _, error in
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                print("== Old subscription deleted successfully", subId)
                // if predicate is nil, then don't create new subscription. as NSPredicate(value: false) will cause partial failure
                if let predicate = predicate {
                    let subscription = CKQuerySubscription(recordType: syncObject.recordType, predicate: predicate,
                                                           subscriptionID: subId,
                                                           options: options)
                    let notificationInfo = CKSubscription.NotificationInfo()
                    notificationInfo.shouldSendContentAvailable = true // must be true or nothing will arrive. triple tested and it doesn't work even when app is in foreground.
                    notificationInfo.alertLocalizationArgs = args
                    subscription.notificationInfo = notificationInfo
                    let createOp = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: nil)
                    createOp.addDependency(deleteOp) // create only after deleting is done
                    createOp.modifySubscriptionsCompletionBlock = { _, _, error in
                        switch ErrorHandler.shared.resultType(with: error) {
                        case .success:
                            print("== New subscription created successfully", subId)
                        case .retry(let timeToWait, _):
                            ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                                self.createSubscriptionInPublicDatabase(on: syncObject, with: predicate, args: args, options: options)
                            })
                        default:
                            break
                        }
                    }
                    createOp.qualityOfService = self.qos
                    self.database.add(createOp)
                }
                
            case .retry(let timeToWait, _):
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                    self.createSubscriptionInPublicDatabase(on: syncObject, with: predicate, args: args, options: options)
                })
            default:
                break
            }
        }
        deleteOp.qualityOfService = self.qos
        database.add(deleteOp)
        
        #endif
    }
    
    public func startObservingTermination() {
        #if os(iOS) || os(tvOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willTerminateNotification, object: nil)
        
        #elseif os(macOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willTerminateNotification, object: nil)
        
        #endif
    }
    
    public func registerLocalDatabase() {
        syncObjects.forEach { object in
            DispatchQueue.main.async {
                object.registerLocalDatabase()
            }
        }
    }
    
    // MARK: - Private Methods
    private func excuteQueryOperation(queryOperation: CKQueryOperation,on syncObject: Syncable, callback: ((Error?) -> Void)? = nil) {
        queryOperation.recordFetchedBlock = { record in
            syncObject.add(record: record, databaseManager: self)
            print("== Fetched record:", record.recordType, record.recordID.recordName)
        }
        
        queryOperation.queryCompletionBlock = { [weak self] cursor, error in
            guard let self = self else { return }
            if let cursor = cursor {
                let subsequentQueryOperation = CKQueryOperation(cursor: cursor)
                self.excuteQueryOperation(queryOperation: subsequentQueryOperation, on: syncObject, callback: callback)
                return
            }
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                DispatchQueue.main.async {
                    self.syncObjects.forEach {
                        $0.resolvePendingRelationships()
                    }
                    callback?(nil)
                }
            case .retry(let timeToWait, _):
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                    self.excuteQueryOperation(queryOperation: queryOperation, on: syncObject, callback: callback)
                })
            default:
                callback?(error)
            }
        }
        
        queryOperation.qualityOfService = self.qos
        database.add(queryOperation)
    }

    
    @objc public func cleanUp() {
        for syncObject in syncObjects {
            syncObject.cleanUp()
        }
    }
}
