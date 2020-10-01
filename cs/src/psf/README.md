Faster Predicate Subset Functions (PSFs)
----------------------------------------

PSFs are based upon the [FishStore](https://github.com/microsoft/FishStore) prototype. PSFs function as secondary indexes, allowing an application to query on keys other than the single key used by the FasterKV ("Faster Key/Value store").

<!-- Use markdown-toc by [Jon Schlinkert](https://github.com/jonschlinkert) to insert the Table of Contents between the toc/tocstop comments; commandline is: markdown-toc -i <this file> -->

<!-- toc -->

- [Overview](#overview)
  * [FasterKV: Primary Index With Unique Keys](#fasterkv-primary-index-with-unique-keys)
  * [PSFs: Secondary Indexes With Nonunique Keys](#psfs-secondary-indexes-with-nonunique-keys)
    + [Defining Secondary Indexes](#defining-secondary-indexes)
    + [Updating Secondary Indexes](#updating-secondary-indexes)
    + [Querying Secondary Indexes](#querying-secondary-indexes)
    + [Limitations](#limitations)
      - [No Range Indexes](#no-range-indexes)
      - [Fixed-Length `TPSFKey`](#fixed-length-tpsfkey)
- [Public API](#public-api)
  * [The Core PSF API](#the-core-psf-api)
    + [`PSFManager`](#psfmanager)
    + [`IPSF`](#ipsf)
  * [The FasterKV PSF API](#the-fasterkv-psf-api)
    + [Registering PSFs](#registering-psfs)
    + [Querying PSFs](#querying-psfs)
    + [The FasterPSFSample playground app](#the-fasterpsfsample-playground-app)
    + [Registering PSFs on FasterKV](#registering-psfs-on-fasterkv)
      - [Registering PSFs on `Restore`](#registering-psfs-on-restore)
  * [Querying PSFs on Session](#querying-psfs-on-session)
- [Code internals](#code-internals)
  * [KeyPointer](#keypointer)

<!-- tocstop -->

# Overview
PSFs are "Predicate Subset Functions"; they allow defining predicates that records will match, possibly non-uniquely, for secondary indexing. PSFs are designed to be used by any data provider. Currently there is only an implementation using FasterKV as the provider, so this document will mostly reference the implementation of them as a secondary index (using "secondary FasterKVs") for a primary FasterKV store, with occasional commentary on other possible stores.

## FasterKV: Primary Index With Unique Keys
FasterKV is essentially a hash table; as such, it has a single primary key for a given record, and there are zero or one records available for a given key.

- An Upsert (insert or blind update) will replace an identical key, or insert a new record if an exact key match is not found
- An RMW (Read-Modify-Write) will find an exact key match and update the record, or insert a new record if an identical key match is not found
- A Read will find either a single record matching the key, or no records

The FasterKV Key and Value may be blittable, variable length, or objects.

## PSFs: Secondary Indexes With Nonunique Keys
PSFs implement secondary indexes by allowing the user to register a delegate that returns an alternate key for the record. For example, a record might be { Id: 42, Species: "cat" }. The "primary index" is the key inserted into the primary FasterKV; in this example it is Id, and there will be only one record with an Id of 42. A PSF might be defined for such records that returns the Species property (in C# terms, a simple lambda such as "() => this.Species;"). This return is nullable, reflecting the "predicate" terminology, which means that the record may or may not "match" the PSF; for example, a record with no pets would return null, and the record would not be stored for that PSF. This design allows zero, one, or more records to be stored for a single PSF key, entirely depending on the PSF definition.

### Defining Secondary Indexes 
PSF definition is done using the [`RegisterPSF` APIs](#registering-psfs-on-fasterkv) on the [`PSFManager`](./PSFManager.cs) class; for the FasterKV provider, a thin wrapper over these exists on the `IFasterKV` interface and is implemented by FasterKV. Each `RegisterPSF` call creates a [`PSFGroup`](./PSFGroup.cs) internally; the [`PSFGroup`](./PSFGroup.cs) contains its own FasterKV instance, and all specified PSF keys are linked in chains within that FasterKV instance. More details of this are shown below. Allowing [`PSFGroup`](./PSFGroup.cs)s has the following advantages:
- A single hashtable can be used for all PSFs, using the ordinal of the PSF as part of the hashing logic. This can save space.
- PSFs should be registered in groups where it is expected that a record will match all or none of the PSFs (that is, if a record results in a non-null key forone PSF in the group, it results in a non-null key for all PSFs in the group, and if a record results in a null key for one PSF in the group, it results in a null key for all PSFs in the group). This saves some overhead in processing variable-length composite keys in the secondary FasterKV; this [KeyPointer](#keypointer) structure is described more fully below.
- All PSFs in a [`PSFGroup`](./PSFGroup.cs) have the same `TPSFKey` type, but different groups can have different `TPSFKey` types.

Internally, PSFs are implemented using secondary FasterKV instances with separate Key and Value types, as described in the following sections.

### Updating Secondary Indexes
When a record is inserted into FasterKV (via Upsert or RMW), it is inserted at a unique "logical address" (essentially a page/offset combination). PSFs implement secondary indexes in Faster by allowing the user to register a delegate that returns an alternate key for the record; then the logical address of an insert (termed a RecordId; note that this is *not* the actual record value) is the value that is inserted into the secondary FasterKV instance using the alternate key. The distinction between the Key and Value defined for the primary FasterKV and the secondary Key and Value (the RecordId) is critical; the secondary FasterKV has no idea of the primary datastore's Key and Value definitions.

Unlike the primary FasterKV's keys, the PSF keys may return multiple records. To query a PSF, the user passes a value for the alternate key; all logical addresses that were inserted with that key are returned, and then the primary FasterKV instance retrieves the actual records at those logical addresses. Whereas the primary FasterKV returns only a single record (if found) via the IFunctions callback implementation supplied by the client, PSF queries return an `IEnumerable<TRecordId>` or `IAsyncEnumerable<TRecordId>`.

The logical address as the RecordId is specific the the FasterKV's use of PSFs; other data provider could provide any other record identifier that fits with their design.

PSF updating is done by the Primary FasterKV's Upsert, RMW, or Delete operations, which call the Upsert, Update, or Delete methods on the [`PSFManager`](./PSFManager.cs).

### Querying Secondary Indexes
The `ClientSession` object contains several overloads of `QueryPSF`, taking various combinations of [`IPSF`](./IPSF.cs), `TPSFKey` types and individual keys, and `matchPredicate`.

The simplest query takes only a single [`IPSF`](./IPSF.cs) and `TPSFKey` key instance, returning all records that match that key for that [`IPSF`](./IPSF.cs). More complicated forms allow specifying multiple [`IPSF`](./IPSF.cs)s, multiple keys per [`IPSF`](./IPSF.cs), and multiple `TPSFKey` types (each with multiple [`IPSF`](./IPSF.cs)s, each with multiple keys).

Because [`PSFGroup`](./PSFGroup.cs)s store `TRecordId`s, the [`PSFManager`](./PSFManager.cs) client (that is, the data provider, such as a primary FasterKV) must wrap the `QueryPSF` call with its own translation of the `TRecordId` to the actual data record; see `CreateProviderData` in `FasterPSFSessionOperations.cs`.

The [PSFQuery API](#querying-psfs-on-session) is described in detail below.

### Limitations
The current implementation of PSFs has some limitations.

#### No Range Indexes
Because PSFs use hash indexes (and conceptually store multiple records for a given key as that key's collision chain), we have only equality comparisons, not ranges. This can be worked around in some cases by specifying "binned" keys; for example, a date range can be represented as bins of one minute each. In this case, the query will pass an enumeration of keys and all records for all keys will be queried; the caller must post-process each record to ensure it is within the desired range. The caller must decide the bin size, trading off the inefficiency of multiple key enumerations with the inefficiency of returning unwanted values (including the lookuup of the logical address in the primary FasterKV).

#### Fixed-Length `TPSFKey`
PSFs currently use fixed-length keys; the `TPSFKey` type returned by a PSF execution has a type constraint of being `struct`. It must be a blittable type; the PSF API does not provide for `IVariableLengthStruct` or `SerializerSettings`, nor does it accept strings as keys. Rather than passing a string, the caller must pass some sort of string identifier, such as a hash (but do not use string.GetHashCode() for this, because it is AppDomain-specific (and also dependent on .NET version)). In this case, the hashcode becomes a "bin" of all strings (or string prefixes) corresponding to that hash code.

# Public API
This section discusses the PSF API in more detail. There are two levels: The interface to PSFs themselves, which is [`PSFManager`](./PSFManager.cs), and how FasterKV is a client of PSFs (as well as providing code for the implementation).

## The Core PSF API
This section describes the core PSF API on the [`PSFManager`](./PSFManager.cs) class. For examples of its use, see [The FasterKV PSF API](#the-fasterkv-psf-api), which wraps the core PSF API.

### [`PSFManager`](./PSFManager.cs) 
As discussed above, the PSF API is intended to be used by any data provider needing a hash-based index that is capable of storing a `TRecordId` from which the provider can extract its full record. However, PSF update operations must be able to execute the PSF, which requires knowledge of the provider's Key and Value types. Therefore, [`PSFManager`](./PSFManager.cs) has two generic types, both of which are opaque to PSFs:
- `TProviderData`, which is the data passed to PSF execution (the PSF must, of course, know how to operate on the provider data and form a `TPSFKey` key from it)
- `TRecordId`, which is the record identifier stored as the Value in the secondary FasterKV.

The `TRecordId` has a type constraint of being `struct`; it must be blittable.

### [`IPSF`](./IPSF.cs)

## The FasterKV PSF API

### Registering PSFs

### Querying PSFs

### The FasterPSFSample playground app
The [FasterPSFSample](../../../../playground/FasterPSFSample/FasterPSFSample.cs) app demonstrates registering and querying PSFs.

### Registering PSFs on FasterKV
[Defining Secondary Indexes](#defining-secondary-indexes) provides an overview of registering PSFs. For the FasterKV provider, this is done on the [IFasterKV](../Interfaces/IFasterKV.cs) interface, because it does not do any session operations itself. TODO revisit for indexing of existing records.

[FPSF.cs](../../../../playground/FasterPSFSample/FPSF.cs) illustrates registering the PSFs. There are several overloads, depending on the number of PSFs per group and whether the PSF can be defined by a simple lambda vs. a more complex definition (which requires an implementation of [`IPSFDefinition`](./IPSFDefinition.cs)).

[`PSFGroup`](./PSFGroup.cs)s are not visible to the client; they are entirely internal. The return from a `RegisterPSF` call is a vector of [`IPSF`](./IPSF.cs), and various combinations of [`IPSF`](./IPSF.cs) and `TPSFKey` values are passed the the [`QueryPSF` API](#querying-secondary-indexes).

Each [`PSFGroup`](./PSFGroup.cs)s contains its own internal "secondary" FasterKV instance, including a hash table (shared by all PSFs, including the PSF ordinal in the hash operation) and log. Because this contains hashed records for all PSFs in the group, PSFs cannot be modified once created, nor can they be added to or removed from the group individually.

#### Registering PSFs on `Restore`
[IFasterKV](../Interfaces/IFasterKV.cs) provides a `GetRegisteredPSFNames` method that returns the names of all PSFs that were registered. Another provider would have to expose similar functionality. At `Restore` time, before any operations are done on the Primary FasterKV, the aplication must call `RegisterPSF` on those names for those groups; it *must not* change a group definition by adding, removing, or supplying a different name or functionality (lambda or definition) for a PSF in the group; doing so will break access to existing records in the group.

If an application creates a new version of a PSF, it should encode the version information into the PSF's name, e.g. "Dog v1.1". The application must keep track internally of all PSF names and functionality (lambda or definition) for any groups it has created.

Dropping a group is done by omitting it from the `RegisterPSF` calls done at `Restore` time. This is the only supported versioning mechanism: "drop" a group (by not registering it) and then create a new group with the updated definitions (and possibly changed PSF membership).

## Querying PSFs on Session
[Querying Secondary Indexes](#querying-secondary-indexes) provides an overview of querying PSFs. For the FasterKV provider, these are done on the [ClientSession](FasterPSFSessionOperations.cs) class, because they must enter the session lock.

# Code internals
This section presents a high-level overview of the PSF internal design and implementation.

## KeyPointer
TODO

Notes:
- generics don't have type constraint for ": nullable" (but they can for notnull)
-