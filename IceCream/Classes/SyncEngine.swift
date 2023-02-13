//
//  SyncEngine.swift
//  IceCream
//
//  Created by 蔡越 on 08/11/2017.
//

import CloudKit

/// SyncEngine talks to CloudKit directly.
/// Logically,
/// 1. it takes care of the operations of **CKDatabase**
/// 2. it handles all of the CloudKit config stuffs, such as subscriptions
/// 3. it hands over CKRecordZone stuffs to SyncObject so that it can have an effect on local Realm Database

public final class SyncEngine {
    
    private let databaseManager: DatabaseManager
    
    public convenience init(objects: [Syncable], databaseScope: CKDatabase.Scope = .private, container: CKContainer = .default(),
                            autoSync: Bool = true, qualityOfService: QualityOfService = .utility) {
        switch databaseScope {
        case .private:
            let privateDatabaseManager = PrivateDatabaseManager(objects: objects, container: container, qualityOfService: qualityOfService)
            self.init(databaseManager: privateDatabaseManager, autoSync: autoSync)
        case .public:
            let publicDatabaseManager = PublicDatabaseManager(objects: objects, container: container, qualityOfService: qualityOfService)
            self.init(databaseManager: publicDatabaseManager, autoSync: autoSync)
        default:
            fatalError("Shared database scope is not supported yet")
        }
    }
    
    private init(databaseManager: DatabaseManager, autoSync: Bool) {
        self.databaseManager = databaseManager
        setup(autoSync: autoSync)
    }
    
    private func setup(autoSync: Bool) {
        databaseManager.prepare()
        databaseManager.container.accountStatus { [weak self] (status, error) in
            guard let self = self else { return }
            switch status {
            case .available:
                self.databaseManager.registerLocalDatabase()
                self.databaseManager.createCustomZonesIfAllowed()
                if autoSync {
                    self.databaseManager.fetchChangesInDatabase(nil)
                    self.databaseManager.createDatabaseSubscriptionsForAll()
                }
                self.databaseManager.resumeLongLivedOperationIfPossible()
                self.databaseManager.startObservingRemoteChanges()
                self.databaseManager.startObservingTermination()
            case .noAccount, .restricted:
                guard self.databaseManager is PublicDatabaseManager else { break }
                if autoSync {
                    self.databaseManager.fetchChangesInDatabase(nil)
                    self.databaseManager.createDatabaseSubscriptionsForAll()
                }
                self.databaseManager.resumeLongLivedOperationIfPossible()
                self.databaseManager.startObservingRemoteChanges()
                self.databaseManager.startObservingTermination()
            case .temporarilyUnavailable:
                break
            case .couldNotDetermine:
                break
            @unknown default:
                break
            }
        }
    }
    
}

// MARK: Public Method
extension SyncEngine {
    
    public func getDatabaseManager() -> DatabaseManager {
        return self.databaseManager
    }
    
    /// Fetch data on the CloudKit and merge with local
    ///
    /// - Parameter completionHandler: Supported in the `privateCloudDatabase` when the fetch data process completes, completionHandler will be called. The error will be returned when anything wrong happens. Otherwise the error will be `nil`.
    public func pull(completionHandler: ((Error?) -> Void)? = nil) {
        databaseManager.fetchChangesInDatabase(completionHandler)
    }
    
    /// Push all existing local data to CloudKit
    /// You should NOT to call this method too frequently
    public func pushAll() {
        databaseManager.syncObjects.forEach { $0.pushLocalObjectsToCloudKit() }
    }
    
}

public enum Notifications: String, NotificationName {
    case cloudKitDataDidChangeRemotely
}

public enum IceCreamKey: String {
    /// Notifications
    case affectedRecordType
    case affectedRecordName
    case affectedReason
    
    /// Tokens
    case databaseChangesTokenKey
    case zoneChangesTokenKey
    
    /// Flags
    case subscriptionIsLocallyCachedKey
    case hasCustomZoneCreatedKey
    
    public var value: String {
        return "icecream.keys." + rawValue
    }
}

/// Dangerous part:
/// In most cases, you should not change the string value cause it is related to user settings.
/// e.g.: the cloudKitSubscriptionID, if you don't want to use "private_changes" and use another string. You should remove the old subsription first.
/// Or your user will not save the same subscription again. So you got trouble.
/// The right way is remove old subscription first and then save new subscription.
/// Vic:
///     1) Subscription for publicDB is based on recordType and predicate, same combination won't be allowed to register twice, no error will be returned
///     2) Original icecream chose to fetch everything from publicDB, no matter what subscription was hit, the logic works but confusing and not effective
///     3) we want to make it to create proper subscription and let developer to choose which record to download by parsing the remote notification received.
public enum IceCreamSubscription: String, CaseIterable {
    public static let PREFIX = "IC_"
    
    case cloudKitPrivateDatabaseSubscriptionID = "private_changes"
    case cloudKitPublicDatabaseSubscriptionID = "cloudKitPublicDatabaseSubcriptionID"
    
    var id: String {
        return rawValue
    }
    
    public static var allIDs: [String] {
        return IceCreamSubscription.allCases.map { $0.rawValue }
    }
}
