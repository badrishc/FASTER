---
title: "Subset Index"
permalink: /docs/subsetindex-extension/
excerpt: "Subset Index"
last_modified_at: 2020-12-08
toc: true
---

## FASTER Subset Index User's Guide
The FASTER SubsetIndex is based upon the PSFs (Predicate Subset Functions) defined in the [FishStore](https://github.com/microsoft/FishStore) prototype; they allow defining predicates that records will match, possibly non-uniquely, for secondary indexing. The SubsetIndex is designed to be used by any data provider. Currently there is only an implementation using FasterKV as the provider, so this document will mostly focus on their use as a secondary index (implemented using "secondary FasterKVs") for a primary FasterKV store, with occasional commentary on other possible stores.

## FasterKV: Primary Index With Unique Keys
FasterKV is essentially a hash table; as such, it has a single primary key for a given record, and there are zero or one records available for a given key.

- An Upsert (insert or blind update) will replace an identical key, or insert a new record if an exact key match is not found
- An RMW (Read-Modify-Write) will find an exact key match and update the record, or insert a new record if an identical key match is not found
- A Read will find either a single record matching the key, or no records

The FasterKV Key and Value may be [blittable](https://docs.microsoft.com/en-us/dotnet/framework/interop/blittable-and-non-blittable-types) fixed-length, blittable variable length, or .NET objects.

## SubsetIndex Overview: A Secondary Indexes With Nonunique Keys
The FASTER SubsetIndex implements a secondary index by allowing the user to register one or more Predicates that return an alternate key for the record. For example, a record might be { Id: 42, Species: "cat" }. The "primary index" is the key inserted into the primary FasterKV; in this example it is Id, and there will be only one record with an Id of 42. A SubsetIndex might be created for such records by defining a Predicate that returns the Species property.

### Predicates
A Predicate is a function (in C# terms, a delegate, or a lambda such as "v => v.Species;") that operates on the Value of a record and returns a Key that is distinct from the Key in the primary FasterKV instance.

The return from a Predicate is nullable, reflecting the "predicate" and "subset" terminology, which means that the record may or may not "match" the Predicate. For example, a record with no pets would return null, and the record would not be stored for that Predicate. On the other hand, dogs and cats are quite common. This design allows zero, one, or more records to be stored for a single Predicate key, entirely depending on the Predicate definition.

### Predicate Groups
Predicates are registered with the SubsetIndex in Groups. For space efficiency, Predicates should be organized into groups such that it is expected that a record will match all or none of the Predicates in that Group. That is, if a record results in a non-null key for one Predicate in the Group, it results in a non-null key for all Predicates in the Group; and if a record results in a null key for one Predicate in the Group, it results in a null key for all Predicates in the Group).

### Assemblies

As shown in this diagram, there are 3 assemblies involved when defining a SubsetIndex in a FasterKV app:
![Assembly Overview](../assets/subsetindex/assembly-overview.png "Assembly Overview")

Note the direction of arrows in the diagram. For a FasterKV app that uses a SubsetIndex, the app talks to the `FASTER.indexes.SubsetIndex` assembly, and indirectly to the `FASTER.libraries.SubsetIndex` assembly because it holds data members of its types, in particular `IPredicate` which is used for queries. `FASTER.indexes.SubsetIndex` then talks to `FASTER.core` and `FASTER.libraries.SubsetIndex`. However, `FASTER.core` is completely unaware of the SubsetIndex.

Similarly, a non-FasterKV app will use `FASTER.libraries.SubsetIndex` directly, and no other `FASTER` assemblies (except indirectly, because FasterKV instances are used to implement the SubsetIndex).

#### `FASTER.core`
This is the core FASTER assembly, and has no code that is specific to the SubsetIndex (some enhancements, such as `ReadAtAddress` and `IAdvancedFunctions`, were motivated initially by the SubsetIndex, but are core FASTER features usable by any FASTER app). An app that uses a SubsetIndex will mostly still make the same FASTER calls, except when defining or querying the SubsetIndex.

#### `FASTER.indexes.SubsetIndex`
This can be thought of as an extension to core FASTER; it uses extension functions to provide the SubsetIndex functionality (this is described further below). It has no storage itself; rather, it is a redirection and coordination layer between `FASTER.core` and `FASTER.libraries.SubsetIndex`. It provides extension wrappers for:
  - Registering Predicate groups
  - Obtaining SubsetIndex-enabled new sessions (other sessions are not allowed)
  - Providing wrappers for some FasterKV operations such as `Flush`, `Checkpoint`, `Recover` to coordinate with the SubsetIndex
  - Providing a session wrapper that:
    - Exposes all the usual `ClientSession` operations such as `Read`, `Upsert`, `RMW`, and `Delete`, wrapping them to keep the SubsetIndex updated.
    - Adds methods for querying Predicates singly or in groups

#### `FASTER.libraries.SubsetIndex`
This is the actual implementation of the SubsetIndex. Each Group has its own FasterKV instance (Using different Keys and Values than 'FASTER.core`), but these are an implementation detail and are not seen by the client of this assembly other than the Predicate registration options that are passed through to the FasterKV constructor. For an app that uses the SubsetIndex over a different store than FasterKV, this is the only FASTER assembly that app will use.

### Limitations
The current implementation of PSFs has some limitations.

#### No Range Indexes
Because PSFs use hash indexes (and conceptually store multiple records for a given key as that key's collision chain), we have only equality comparisons, not ranges. This can be worked around in some cases by specifying "binned" keys; for example, a date range can be represented as bins of one minute each. In this case, the query will pass an enumeration of keys and all records for all keys will be queried; the caller must post-process each record to ensure it is within the desired range. The caller must decide the bin size, trading off the inefficiency of multiple key enumerations with the inefficiency of returning unwanted values (including the lookuup of the logical address in the primary FasterKV).

#### Fixed-Length `TPKey`
PSFs currently use fixed-length keys; the `TPKey` type returned by a PSF execution has a type constraint of being `struct`. It must be a blittable type; the PSF API does not provide for `IVariableLengthStruct` or `SerializerSettings`, nor does it accept strings as keys. Rather than passing a string, the caller must pass some sort of string identifier, such as a hash (but do not use string.GetHashCode() for this, because it is AppDomain-specific (and also dependent on .NET version)). In this case, the hashcode becomes a "bin" of all strings (or string prefixes) corresponding to that hash code.

## Public API
This section discusses the SubsetIndex public API. As mentioned above, there are two levels: The interface for FASTER clients to add SubsetIndexing to their apps, and the interface for non-FASTER clients.

### FasterKV Client Changes to Use the SubsetIndex
There are very few changes required to enable the SubsetIndex in a FasterKV app.

<a name="creating-faster-for-si"></a>
#### Creating a SubsetIndex-enabled Instance of FasterKV<K, V>
A FASTER app that uses a SubsetIndex must not instantiate a `FasterKV<K, V>` directly; instead must obtain a SubsetIndex-enabled subclass using `SubsetIndexExtensions.NewFasterKV<K, V>(...)`. This returns an instance of `FasterKVForSI<K, V>`, which is an internal subclass of `FasterKV<K, V>`. Thus it is identical for non-SubsetIndex operations; the application talks to a `FasterKV<K, V>` instance as usual.

<a name="creating-session-for-si"></a>
#### Creating a SubsetIndex-enabled Session on FasterKV<K, V>
A FASTER app that uses a SubsetIndex must not use the `FasterKV<K, V>` methods for obtaining a session; instead it must call the `SubsetIndexExtensions` method `.ForSI` on the `FasterKVForSI<K, V>` to obtain a `ClientSessionForSI<I, O, C, F>` object. This does not inherit from `(Advanced)ClientSession` but presents an identical interface, including Read, Upsert, RMW, and Delete operations; internally it manages these calls to update both the primary `FasterKV<K, V>` and the SubsetIndex. 

The SubsetIndex API to create a new session parallels that of the normal FasterKV API, but due to C# overloading rules, it is not possible to overload the `For` methods on return type. Therefore the name `ForSI` is used for the corresponding method that returns a new `ClientSessionBuilderForSI`; however, the usual NewSession naming is used on this object to obtain a `ClientSessionForSI`. An exception is thrown if `.For` is called on a `FasterKVForSI<K, V>` instance. This is one of the very few API differences in non-SubsetIndex operations.

### Example Apps

FASTER provides two levels of example apps: `cs/samples` provides simple examples of basic functionality, and `/cs/playground` provides a more comprehensive exercise of functionality.

For the SubsetIndex examples, an easy way to see the differences from a non-SubsetIndex FASTER app is to compare the sample apps in the `cs/samples/SubsetIndex` with other samples.

Following are the SubsetIndex-specific examples.

#### The `BasicPredicate` Sample App
This illustrates the simplest form of defining and querying a predicate, with the Predicate using a lambda that simply returns a property of the object rather than defining a key struct.

#### The `SingleGroup` Sample App
This illustrates defining two Predicates in a single group, using a key struct that knows which property of the value it should use as the secondary key. It uses the synchronous `Query` method to illustrate simple boolean AND/OR operations.

#### The `MultiGroup` Sample App
This illustrates defining two Predicates each in their own group, using two separate key structs, each dedicated to a single property of the value to be used as the secondary key. It uses the asynchronous `Query` method to illustrate simple boolean AND/OR operations.

#### The `SubsetIndex` Playground App
The [SubsetIndex playground app](../../cs/playground/SubsetIndex/SubsetIndexApp.cs) app demonstrates much more comprehensive (and complex) registration and querying of the SubsetIndex, using all overloads of the `Query` API.

<a name="registering-si"></a>
### Defining (Registering) the SubsetIndex on FasterKV<K, V>
First obtain a SubsetIndex-enabled `FasterKV<K, V>` as described [above](#creating-faster-for-si).

This `FasterKV<K, V>` instance enables `SubsetIndexExtensions.Register(...)` for registering the Predicate groups. Each `Register` call is forwarded to the [`SubsetIndex`](#subsetindex-object) implementation, which creates a [`Group`](#group-object) internally; this `Group` contains its own `FasterKV` instance, using the Predicate Key type and a RecordId that is the logical address of the record in the Primary `FasterKV` for its Value. All non-null keys returned from the Predicate are linked in chains within that FasterKV instance. 

There are a number of overloads of `Register` that take various forms of predicate specification:
- Simple lambdas that merely return a property of the Value, or complex lambdas that calculate and return a Key type. Lambdas are wrapped in an instance of [`FasterKVPredicateDefinition`](#fkv-predicate-definition-class), wrapping the lambda in its delegate.
- A [`FasterKVPredicateDefinition`](#fkv-predicate-definition-class) instance with the delegate already created.

All `Predicate`s in a `Group` have the same Key type and same form for specifying the predicate logic (lambda vs. [`FasterKVPredicateDefinition`](#fkv-predicate-definition-class)). Creating Predicates in groups has the following advantages:
- A single hashtable can be used for all Predicates in the group, using the ordinal of the Predicate as part of the hashing logic. This can save space.
- Predicates should be registered in groups where it is expected that a record will match all or none of the Predicates (that is, if a record results in a non-null key for one Predicate in the group, it results in a non-null key for all Predicates in the group, and if a record results in a null key for one Predicate in the group, it results in a null key for all Predicates in the group). This saves some overhead in processing variable-length composite keys in the secondary FasterKV; this [KeyPointer](#keypointer) structure is described more fully below.
- All Predicates in a group have the same `TPKey` type, but different groups can have different `TPKey` types.

#### How Predicates are Called
For `VarLen` (variable-length blittable) types and large fixed-length blittable types, it is not feasible to pass the entire object; this is why "ref Key" and "ref Value" are prevalent in the `FasterKV<K, V>` API. Similarly, `Predicate` execution must take a "ref Key" and "ref Value". However, in many cases (such as [pending operations](#data-update-operations)), the SubsetIndex must store the Key and Value pass on the `FasterKV<K, V>` call, and eventually pass them to the `Predicate` call.

Because of this, the SubsetIndex extension has a `FasterKVProviderData` which holds the Key and Value until it is ready to be executed. This execution happens in each [`Group`](#group-class) when [`ClientSessionForSI`](#clientsessionforsi-class) calls [`ClientSessionSI`](#clientsessionsi-class)'s update methods.

<a name="updating-si"></a>
### Updating the SubsetIndex Through Session Operations
A FASTER app that uses a SubsetIndex must not use the `FasterKV<K, V>` methods for obtaining a session; instead it must call the `SubsetIndexExtensions` method `.ForSI` on the `FasterKVForSI<K, V>` to obtain a [`ClientSessionForSI<I, O, C, F>`](#clientsessionforsi-class) object. This presents an identical interface to the `(Advanced)ClientSession` for Upsert, RMW, or Delete operations, but internally it passes these calls through to the same methods on its contained `(Advanced)ClientSession` instance, then calls the Upsert, Update, or Delete methods on the [`SubsetIndex`](#subsetindex-object) to update the index.

When a record is inserted into the primary FasterKV (via Upsert or RMW), it is inserted at a unique "logical address" (essentially a page/offset combination). Predicates are used to implement secondary indexes in Faster by allowing the user to register a delegate that returns an alternate key for the record; then the logical address of the record in the primary FasterKV is the value that is inserted into the secondary FasterKV instance using the alternate key. This value is referred to as a RecordId; note that this is *not* the actual record value in the primary FasterKV, only its address. The type is termed `TRecordId` and for the primary FasterKV it is a long integer; other data providers may use a different type, as long as it is blittable.

The distinction between the Key and Value defined for the primary FasterKV and the Key and RecordId (Value) in the secondary FasterKV is critical; the secondary FasterKV has no idea of the primary datastore's Value type, nor does it know if the primary datastore even has a Key.

Unlike the primary FasterKV's keys, the Predicate keys may chain multiple records. (The primary FasterKV can support this by returning false from `InPlaceUpdater` and `ConcurrentWriter` methods on the `I(Advanced)Functions` implementations, then using the session's `ReadAtAddress` variants, but the SubsetIndex suports this automatically). To query a Predicate, the user passes a value for the alternate key; all RecordIds that were inserted with that key are returned, and then `ReadAtAddress` is called on the primary FasterKV instance to retrieve the actual records for those RecordIds. Whereas the primary FasterKV returns only a single record (if found) via the `I(Advanced)Functions` implementation on `Read` operations, queries on the SubsetIndex return an `IEnumerable<TRecordId>` or `IAsyncEnumerable<TRecordId>`.

### Querying the SubsetIndex
The `ClientSessionForSI<I, O, C, F>` instance contains several overloads of `Query` and corresponding `QueryAsync` methods, taking various combinations of [`IPredicate`](../../cs/src/libraries/SubsetIndex/IPredicate.cs), `TPKey` types and individual keys, and, for queries across multiple `IPredicates`, `matchPredicate`s (lambdas that are called for each RecordId returned from the Predicates' chains, take boolean parameters that indicate which Predicate(s) the RecordId is present in, and return a boolean indicating whether that RecordId is to be included in the query result).

The simplest query takes only a single [`IPredicate`](../../cs/src/libraries/SubsetIndex/IPredicate.cs) and `TPKey` instance and returns all records that match that key for that Predicate. More complicated forms of `Query` allow specifying multiple Predicates, multiple keys per Predicate, and multiple `TPKey` types (each possibly with multiple Predicates, each possibly with multiple keys).

Because the SubsetIndex stores `TRecordId`s, the SubsetIndex client (that is, the data provider, such as a primary FasterKV) must wrap the `Query` call with its own translation layer to map the `TRecordId` to the actual data record; see `CreateProviderData` in [`ClientSessionForSI`](../../cs/src/indexes/SubsetIndex/ClientSessionForSI.cs).

#### Liveness Checking
Some `FasterKV<K, V>` update operations do not guarantee that the old values are available:
- `Upsert` is designed for speed, and if the record is not in the mutable region, it inserts a new record. If the data is not in memory (is below HeadAddress), it does not issue an IO to retrieve the old value from disk.
- Similarly, `Delete` does not issue an IO to retrieve the old value; it inserts a new record with the Tombstone bit set.
  - An `RMW` does issue an IO to obtain the old record.

For an `Upsert` or `Delete` where the record was in the mutable region, the SubsetIndex is able to mark the old record as "deleted", and it is not returned by the query. However, if the record is not retrieved from disk, the SubsetIndex could not even know if it was for the current Key, or was a colliding Key.

Therefore, it is necessary that for each record returned by a SubsetIndex query, we check for "liveness":
- The `ClientSessionForSI` contains a separate session whose Functions is an instance of `LivenessFunctions` (also implementing `IAdvancedFunctions`. Its purpose is to support issuing a ReadAtAddress on the primary `FasterKV<K, V>` using the returned `TRecordId` as its address, and store the Key.
- Once the Key for the `TRecordId`'s address has been obtained, a Read is done on that Key to verify that the address returned is the same as the `TRecordId`. If it is not, then the record is no longer live, and it is not returned from the query.

## Non-FasterKV Client API
As discussed above, the `FASTER.libraries.SubsetIndex` API is intended to be used by any data provider needing a hash-based index that is capable of storing a `TRecordId` from which the provider can extract its full record. However, SubsetIndex update operations must be able to execute the Predicate, which requires knowledge of the provider's Key and Value types. Therefore, [`SubsetIndex`](../../cs/src/libraries/SubsetIndex/SubsetIndex.cs) has two generic types, both of which are opaque to the SubsetIndex:
- `TProviderData`, which is the data passed to PSF execution (the PSF must, of course, know how to operate on the provider data and form a `TPKey` key from it)
- `TRecordId`, which is the record identifier stored as the Value in the secondary FasterKV.

The `TRecordId` has a type constraint of being `struct`; it must be blittable.

<a name="re-registering-si"></a>
### Re-Registering SubsetIndex on `Restore`
TODO: This section is not current
[IFasterKV](../Interfaces/IFasterKV.cs) provides a `GetRegisteredPSFNames` method that returns the names of all PSFs that were registered. Another provider would have to expose similar functionality. At `Restore` time, before any operations are done on the Primary FasterKV, the aplication must call `RegisterPSF` on those names for those groups; it *must not* change a group definition by adding, removing, or supplying a different name or functionality (lambda or definition) for a PSF in the group; doing so will break access to existing records in the group.

If an application creates a new version of a PSF, it should encode the version information into the PSF's name, e.g. "Dog v1.1". The application must keep track internally of all PSF names and functionality (lambda or definition) for any groups it has created.

Dropping a group is done by omitting it from the `RegisterPSF` calls done at `Restore` time. This is the only supported versioning mechanism: "drop" a group (by not registering it) and then create a new group with the updated definitions (and possibly changed PSF membership).

## Faster SubsetIndex Technical Details
This section describes implementation details of the FASTER SubsetIndex.

### Extending FasterKV With the SubsetIndex: `FASTER.indexes.SubsetIndex`
This assembly is a mostly-thin layer over `FasterKV` that redirects between normal FasterKV operations and the SubsetIndex. This assembly uses the naming convention that a public subclass, wrapper class, or function is named the same as the corresponding FasterKV element, with the suffix `ForSI`.

A basic principle of this implementation is that it minimizes differences from the core FasterKV API (`ForSI` being one of the few exceptions). This is done by a combination of subclasses, wrapper classes, and C# extension functions.

#### Internal: `FasterKVForSI`
This is a subclass of `FasterKV`. Rather than exposing this class externally, it is an internal subclass; the application still talks to a `FasterKV` instance.

As a subclass, it overrides operations such as growth, checkpointing, and recovery to ensure the corresponding SubsetIndex methods are called as well.

Other `FasterKVForSI` methods are provided via C# extension methods.

##### Public: `SubsetIndexExtensions`
This is a static C# extension class whose methods take a `FasterKV` instance as the `this` parameter. Internally it casts this to `FasterKVForSI` to call internal methods to implement SubsetIndex capabilities:
- [Predicate registration](#registering-si)
- New Session operations

<a name="clientsessionforsi-class"></a>
#### Public: `ClientSessionForSI`
`ClientSessionForSI` is a wrapper class (not a subclass) that implements all `ClientSession` methods, including operations such as Read, Upsert, RMW, and Delete, which are intercepted to update the SubsetIndex after the operation completes.

`ClientSessionForSI` contains an internal `AdvancedClientSession` on the primary FasterKV; its Functions instance is an instance of the [`IndexingFunctions`](#indexingfunctions-class) class, which implements `IAdvancedFunctions`. [`IndexingFunctions`](#indexingfunctions-class) is the layer that manages data storage between the core FasterKV and the SubsetIndex, as well as containing the client Functions instance (wrapped in `BasicFunctionsWrapper` if it does not implement `IAdvancedFunctions`).

`ClientSessionForSI` also holds an instance of `ClientSessionSI`, which is a "session" on the [`SubsetIndex`](#subsetindex-class) top-level object in the [SubsetIndex implementation](#implementing-si) hierarchy.

<a name="data-update-operations"></a>
#### Data Update Operations
The flow of operations through the `ClientSessionForSI` during a non-pending data-update operation is:
- Call the corresponding method on the contained client session (on the primary FasterKV), which:
  - Calls methods on the [`IndexingFunctions`](#indexingfunctions-class), which:
    - Call the methods on the `IAdvancedFunctions` passed in from the user, or on the `BasicFunctionsWrapper` around the `IFunctions` passed in from the user.
    - Store the data and other information related to the operation. Normally this is stored in a [`ChangeTracker`](#changetracker-class), but for Upsert there is a fast path that does not do heap allocations.
- Call methods on the [`SubsetIndex`](#subsetindex-class) instance to update the index.

In the event of pending data-update operations, the data is stored in a [`ChangeTracker`](#changetracker-class) instance during the completion callback, which [`IndexingFunctions`](#indexingfunctions-class) stores in a queue. When the `ClientSessionForSI`'s `CompletePending` method is called, all enqueued [`ChangeTracker`](#changetracker-class) instances are retrieved and sent to the [`SubsetIndex`](#subsetindex-class) instance to update the index.

<a name="fkv-predicate-definition-class"></a>
#### Internal: `FasterKVPredicateDefinition`
TODO

<a name="implementing-si"></a>
### Implementing the SubsetIndex: `FASTER.libraries.SubsetIndex`
This assembly is the actual implementation of the SubsetIndex. This assembly uses the naming convention that a public subclass, wrapper class, or function is named the same as the corresponding FasterKV element, with the suffix `SI`.

<a name="subsetindex-class"></a>
#### Public: `SubsetIndex`
TODO

<a name="clientsessionsi-class"></a>
#### Public: `ClientSessionSI`

<a name="group-class"></a>
#### Internal: `Group`
TODO

#### The Structure of a Record in a `Group`'s FasterKV
Each [`Group`](#group-class) maintains a single FasterKV instance. The records in this FasterKV instance have the following structure:
![Record Structure](../assets/subsetindex/record-structure.png "Record Structure")
- The usual RecordInfo header. Note that its `PreviousAddress` is not used for `Predicate` key chaining.
- A `CompositeKey` that consists of one or more `KeyPointer` structures (one for each `Predicate` in the `Group`).
  - Each `KeyPointer` contains:
    - The 8-byte address of the `KeyPointer` in the previous record in the chain for this Predicate Key's value. Two things are different here than from the usual FasterKV record:
      - The `RecordInfo`'s `PreviousAddress` is not used; chain traversal is done via a `KeyPointer`'s `PreviousAddress`.
      - The address does *not* point to the start of the previous `RecordInfo`; instead, it points to the start of the previous `KeyPointer`.
    - A two-byte offset to the start of the Key portion of the record.
    - A one-byte ordinal, which is the index of this `Predicate` in its containing `Group`'s list of `Predicates`. 
      - There is an enforced maximum of 255 `Predicates` per `Group`.
      - The size of the `CompositeKey` is fixed; all `Predicates` (or their are `KeyPointers`) are always in the list. (I.e., if there are 3 `Predicates` in a `Group`, then there will always be 3 `KeyPointers` in this list.)
      - A one-byte Flags field
        - One bit is for a null indicator
        - Other bits are reserved for a possible future enhancement to update records in-place.
      - The Key value. If this is 4 bytes it fits within the 8-byte alignment of the basic `KeyPointer` record.
- The record Value, which is a `TRecordId`.

![KeyPointer Chains](../assets/subsetindex/keypointer-chains.png "KeyPointer Chains")

<a name="indexingfunctions-class"></a>
#### Internal: `IndexingFunctions`
TODO

<a name="changetracker-class"></a>
#### Public: `ChangeTracker`
TODO


