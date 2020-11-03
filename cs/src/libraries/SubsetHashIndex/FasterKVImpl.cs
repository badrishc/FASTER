// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//#define INSERT_TRACE

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.libraries.SubsetHashIndex
{
    // Internal function implementations for the secondary FasterKV implementing the SubsetHashIndex; these correspond to the similarly-named
    // functions in FasterImpl.cs.
    internal unsafe partial class FasterKVSHI<TPKey, TRecordId> : FasterKV<TPKey, TRecordId>
        where TPKey : struct
        where TRecordId : struct
    {
        internal KeyAccessor<TPKey, TRecordId> KeyAccessor => (KeyAccessor<TPKey, TRecordId>)this.comparer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus IndexInternalRead<TInput, TOutput, TContext, FasterSession>(
                                    ref TPKey queryKeyPointerRefAsKeyRef,
                                    ref TInput input,
                                    ref TOutput output,
                                    long startAddress,
                                    ref TContext context,
                                    ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<TInput, TOutput, TContext> sessionCtx,
                                    long lsn)
            where FasterSession : IFasterSession<TPKey, TRecordId, TInput, TOutput, TContext>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref queryKeyPointerRefAsKeyRef);
            var tag = (ushort)((ulong)hash >> core.Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;

            OperationStatus status;
            long logicalAddress;
            var usePreviousAddress = startAddress != core.Constants.kInvalidAddress;
            bool tagExists;
            if (!usePreviousAddress)
            {
                tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            }
            else
            {
                logicalAddress = startAddress;
                tagExists = logicalAddress >= hlog.BeginAddress;
                entry.Address = logicalAddress;
            }

            // The addresses stored in the hash table point to KeyPointer entries, not the record header.
            ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
            InsertTrace($"ReadKey: {this.KeyAccessor?.GetString(ref queryKeyPointer)} | hash {hash} |");

            if (tagExists)
            {
                logicalAddress = entry.Address;

#if false // TODOdcr: Support ReadCache in SubsetHashIndex (must call this.KeyAccessor.GetRecordAddressFromKeyLogicalAddress) 
                if (UseReadCache)
                {
                    // We don't let "read by address" use read cache
                    if (usePreviousAddress)
                    {
                        SkipReadCache(ref logicalAddress);
                    }
                    else if (ReadFromCache(ref key, ref logicalAddress, ref physicalAddress))
                    {
                        if (sessionCtx.phase == Phase.PREPARE && GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            goto CreatePendingContext; // Pivot thread
                        }

                        // This is not called when looking up by address, so we do not set pendingContext.recordInfo.
                        fasterSession.SingleReader(ref key, ref input, ref readcache.GetValue(physicalAddress), ref output, Constants.kInvalidAddress);
                        return OperationStatus.SUCCESS;
                    }
                }
#endif // ReadCache

                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);

                    // Note: we never have NoKey in the SubsetHashIndex flavor of this Read.

                    // The comparer is called during AsyncGetFromDiskCallback with a ref to the beginning of the stored CompsiteKey, so match that here.
                    var recordPhysicalAddress = this.KeyAccessor.GetRecordAddressFromKeyPointerAddress(physicalAddress);
                    if (!comparer.Equals(ref queryKeyPointerRefAsKeyRef, ref hlog.GetKey(recordPhysicalAddress)))
                    {
                        logicalAddress = queryKeyPointer.PreviousAddress;
                        TraceBackForKeyMatch(ref queryKeyPointer,
                                                logicalAddress,
                                                hlog.HeadAddress,
                                                out logicalAddress,
                                                out physicalAddress);
                    }
                }
            }
            else
            {
                // no tag found
                return OperationStatus.NOTFOUND;
            }
#endregion

            if (sessionCtx.phase == Phase.PREPARE && GetLatestRecordVersion(ref entry, sessionCtx.version) > sessionCtx.version)
            {
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot thread
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                long recordPhysicalAddress = KeyAccessor.GetRecordAddressFromKeyPointerAddress(physicalAddress);
                pendingContext.recordInfo = hlog.GetInfo(recordPhysicalAddress);
                if (pendingContext.recordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.ConcurrentReader(ref queryKeyPointerRefAsKeyRef, ref input, ref hlog.GetValue(recordPhysicalAddress), ref output, logicalAddress);
                return OperationStatus.SUCCESS;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                long recordPhysicalAddress = this.KeyAccessor.GetRecordAddressFromKeyPointerAddress(physicalAddress);
                pendingContext.recordInfo = hlog.GetInfo(recordPhysicalAddress);
                if (pendingContext.recordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.SingleReader(ref queryKeyPointerRefAsKeyRef, ref input, ref hlog.GetValue(recordPhysicalAddress), ref output, logicalAddress);
                return OperationStatus.SUCCESS;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;

                if (sessionCtx.phase == Phase.PREPARE)
                {
                    Debug.Assert(heldOperation != LatchOperation.Exclusive);
                    if (usePreviousAddress)
                    {
                        Debug.Assert(heldOperation == LatchOperation.None);
                    }
                    else if (heldOperation == LatchOperation.Shared || HashBucket.TryAcquireSharedLatch(bucket))
                    {
                        heldOperation = LatchOperation.Shared;
                    }
                    else
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                    }

                    if (RelaxedCPR) // don't hold on to shared latched during IO
                    {
                        if (heldOperation == LatchOperation.Shared)
                            HashBucket.ReleaseSharedLatch(bucket);
                        heldOperation = LatchOperation.None;
                    }
                }

                goto CreatePendingContext;
            }

            // No record found
            else
            {
                return OperationStatus.NOTFOUND;
            }

#endregion

#region Create pending context
        CreatePendingContext:
            {
                pendingContext.type = OperationType.READ;
                pendingContext.key = new QueryKeyContainer(ref queryKeyPointerRefAsKeyRef, this.KeyAccessor, this.hlog.bufferPool);
                pendingContext.input = fasterSession.GetHeapContainer(ref input);
                pendingContext.output = output;
                pendingContext.userContext = context;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress - queryKeyPointer.OffsetToStartOfKeys - RecordInfo.GetLength();
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
                pendingContext.recordInfo.PreviousAddress = startAddress;
            }
#endregion

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(
                                    ref KeyPointer<TPKey> keyPointer,
                                    long fromLogicalAddress,
                                    long minOffset,
                                    out long foundLogicalAddress,
                                    out long foundPhysicalAddress)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minOffset)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                // The comparer is called during AsyncGetFromDiskCallback with a ref to the beginning of the stored CompsiteKey, so match that here.
                var recordPhysicalAddress = this.KeyAccessor.GetRecordAddressFromKeyPointerAddress(foundPhysicalAddress);
                if (comparer.Equals(ref keyPointer.Key, ref hlog.GetKey(recordPhysicalAddress)))
                    return true;

                ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromPhysicalAddress(foundPhysicalAddress);
                foundLogicalAddress = queryKeyPointer.PreviousAddress;
            }
            foundPhysicalAddress = core.Constants.kInvalidAddress;
            return false;
        }

        [Conditional("INSERT_TRACE")]
        private void InsertTrace(string message) => Console.Write(message);

        [Conditional("INSERT_TRACE")]
        private void IndexTraceLine(string message = null) => Console.WriteLine(message ?? string.Empty);

        // QueryKeyContainer is necessary because VarLenBlittableAllocator.GetKeyContainer will use the size of the full
        // composite key (KeyPointerSize * PredicateCount), but the query key has only one KeyPointer.
        private class QueryKeyContainer : IHeapContainer<TPKey>
        {
            private readonly SectorAlignedMemory mem;

            public unsafe QueryKeyContainer(ref TPKey key, KeyAccessor<TPKey, TRecordId> keyAccessor, SectorAlignedBufferPool pool)
            {
                var len = keyAccessor.KeyPointerSize;
                this.mem = pool.Get(len);
                Buffer.MemoryCopy(Unsafe.AsPointer(ref key), mem.GetValidPointer(), len, len);
            }

            public unsafe ref TPKey Get() => ref Unsafe.AsRef<TPKey>(this.mem.GetValidPointer());

            public void Dispose() => this.mem.Return();
        }

        unsafe struct CASHelper
        {
            internal HashBucket* bucket;
            internal HashBucketEntry entry;
            internal long hash;
            internal int slot;
            internal bool isNull;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus IndexInternalInsert<TInput, TOutput, TContext, FasterSession>(
                        ref TPKey inputFirstKeyPointerRefAsKeyRef, ref TRecordId value, ref TInput input, ref TContext context,
                        ref PendingContext<TInput, TOutput, TContext> pendingContext,
                        FasterSession fasterSession,
                        FasterExecutionContext<TInput, TOutput, TContext> sessionCtx, long lsn)
            where FasterSession : IFasterSession<TPKey, TRecordId, TInput, TOutput, TContext>
        {
            var status = default(OperationStatus);
            var latestRecordVersion = -1;

            ref CompositeKey<TPKey> inputCompositeKey = ref CompositeKey<TPKey>.CastFromFirstKeyPointerRefAsKeyRef(ref inputFirstKeyPointerRefAsKeyRef);

            // Update the KeyPointer links for chains with IsNullAt false (indicating a match with the
            // corresponding Predicate) to point to the previous records for all keys in the composite key.
            // Note: We're not checking for a previous occurrence of the input value (the recordId) because
            // we are doing insert only here. At this time we do not do actual updates (this may be done
            // in a future IndexInternalUpdate()).
            var predCount = this.KeyAccessor.KeyCount;
            CASHelper* casHelpers = stackalloc CASHelper[predCount];
            int startOfKeysOffset = 0;
            var inputAccessor = (context as Context).Functions as IInputAccessor<TInput>;

            InsertTrace($"Insert: {this.KeyAccessor.GetString(ref inputCompositeKey)} | rId {value} |");
            for (var predOrdinal = 0; predOrdinal < predCount; ++predOrdinal)
            {
                ref KeyPointer<TPKey> inputKeyPointer = ref KeyAccessor.GetKeyPointerRef(ref inputCompositeKey, predOrdinal);

                // For RCU, or in case we had to retry due to CPR_SHIFT and somehow managed to delete
                // the previously found record, clear out the chain link pointer.
                inputKeyPointer.PreviousAddress = core.Constants.kInvalidAddress;

                inputKeyPointer.OffsetToStartOfKeys = startOfKeysOffset;
                startOfKeysOffset += this.KeyAccessor.KeyPointerSize;

                ref CASHelper casHelper = ref casHelpers[predOrdinal];
                if (inputKeyPointer.IsNull)
                {
                    casHelper.isNull = true;
                    InsertTrace($" null");
                    continue;
                }

                // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                if (inputAccessor.IsDelete(ref input))
                    inputKeyPointer.IsDeleted = true;

                casHelper.hash = this.KeyAccessor.GetHashCode64(ref inputCompositeKey, predOrdinal);
                var tag = (ushort)((ulong)casHelper.hash >> core.Constants.kHashTagShift);

                if (sessionCtx.phase != Phase.REST)
                    HeavyEnter(casHelper.hash, sessionCtx, fasterSession);

#region Look up record in in-memory HybridLog
                FindOrCreateTag(casHelper.hash, tag, ref casHelper.bucket, ref casHelper.slot, ref casHelper.entry, hlog.BeginAddress);

                // The addresses stored in the hash table point to KeyPointer entries, not the record header.
                var logicalAddress = casHelper.entry.Address;
                if (logicalAddress >= hlog.BeginAddress)
                {
                    InsertTrace($" {logicalAddress}");

                    if (logicalAddress < hlog.BeginAddress)
                        continue;

                    if (logicalAddress >= hlog.HeadAddress)
                    {
                        // Note that we do not backtrace here because we are not replacing the value at the key; 
                        // instead, we insert at the top of the hash chain. Track the latest record version we've seen.
                        long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                        ref RecordInfo recordInfo = ref hlog.GetInfo(this.KeyAccessor.GetRecordAddressFromKeyPointerAddress(physicalAddress));
                        ref KeyPointer<TPKey> prevKeyPointer = ref KeyPointer<TPKey>.CastFromPhysicalAddress(physicalAddress);
                        if (recordInfo.Tombstone || prevKeyPointer.IsDeleted)
                        {
                            // The chain might extend past a tombstoned/deleted record so we must include it in the chain.
                            // unless its previousAddress at predOrdinal is invalid, in which case we can elide it from the chain.
                            if (prevKeyPointer.PreviousAddress < hlog.BeginAddress)
                                continue;
                        }
                        latestRecordVersion = Math.Max(latestRecordVersion, recordInfo.Version);
                    }

                    inputKeyPointer.PreviousAddress = logicalAddress;
                }
                else
                {
                    InsertTrace($" 0");
                }
#endregion
            }

#region Entry latch operation
            // No actual checkpoint locking will be done because this is Insert; only the current thread can write to
            // the record we're about to create, and no readers can see it until it is successfully inserted. However, we
            // must pivot and retry any insertions if we have seen a later version in any record in the hash table.
            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                IndexTraceLine("CPR_SHIFT_DETECTED");
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot Thread
            }
            Debug.Assert(latestRecordVersion <= sessionCtx.version);
            goto CreateNewRecord;
#endregion

#region Create new record in the mutable region
            CreateNewRecord:
            {
                // Create the new record. Because we are updating multiple hash buckets, mark the record as invalid to start,
                // so it is not visible until we have successfully updated all chains.
                var recordSize = hlog.GetRecordSize(ref inputFirstKeyPointerRefAsKeyRef, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                                     // tombstone is always false; see above setting of keyPointer.IsDelete.
                                     final: true, tombstone: false, invalidBit:true,
                                     core.Constants.kInvalidAddress);  // We manage all prev addresses within CompositeKey
                ref TPKey storedFirstKeyPointerRefAsKeyRef = ref hlog.GetKey(newPhysicalAddress);
                ref CompositeKey<TPKey> storedCompositeKey = ref CompositeKey<TPKey>.CastFromFirstKeyPointerRefAsKeyRef(ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref inputFirstKeyPointerRefAsKeyRef, ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref value, ref hlog.GetValue(newPhysicalAddress));

                IndexTraceLine();
                newLogicalAddress += RecordInfo.GetLength();
                for (var predOrdinal = 0; predOrdinal < predCount; ++predOrdinal, newLogicalAddress += this.KeyAccessor.KeyPointerSize)
                {
                    ref KeyPointer<TPKey> storedKeyPointer = ref KeyAccessor.GetKeyPointerRef(ref storedCompositeKey, predOrdinal);

                    var casHelper = casHelpers[predOrdinal];
                    var tag = (ushort)((ulong)casHelper.hash >> core.Constants.kHashTagShift);

                    InsertTrace($"    ({predOrdinal}): {casHelper.hash} {tag} | newLA {newLogicalAddress} | prev {casHelper.entry.word}");
                    if (casHelper.isNull)
                    {
                        IndexTraceLine(" null");
                        continue;
                    }

                    var newEntry = default(HashBucketEntry);
                    newEntry.Tag = tag;
                    newEntry.Address = newLogicalAddress & core.Constants.kAddressMask;
                    newEntry.Pending = casHelper.entry.Pending;
                    newEntry.Tentative = false;

                    var foundEntry = default(HashBucketEntry);
                    while (true)
                    {
                        // If we do not succeed on the exchange, another thread has updated the slot, or we have done so
                        // with a colliding hash value from earlier in the current record. As long as we satisfy the
                        // invariant that the chain points downward (to lower addresses), we can retry.
                        foundEntry.word = Interlocked.CompareExchange(ref casHelper.bucket->bucket_entries[casHelper.slot],
                                                                      newEntry.word, casHelper.entry.word);
                        if (foundEntry.word == casHelper.entry.word)
                            break;

                        if (foundEntry.word < newEntry.word)
                        {
                            InsertTrace($" / {foundEntry.Address}");
                            casHelper.entry.word = foundEntry.word;
                            storedKeyPointer.PreviousAddress = foundEntry.Address;
                            continue;
                        }

                        // We can't satisfy the always-downward invariant, so leave the record marked Invalid and go
                        // around again to try inserting another record.
                        IndexTraceLine("RETRY_NOW");
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease;
                    }

                    // Success for this Predicate.
                    IndexTraceLine(" ins");
                    hlog.GetInfo(newPhysicalAddress).Invalid = false;
                }

                storedCompositeKey.ClearUpdateFlags(this.KeyAccessor.KeyCount, this.KeyAccessor.KeyPointerSize);
                status = OperationStatus.SUCCESS;
                goto LatchRelease;
            }
#endregion

#region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.INSERT;
                pendingContext.key = hlog.GetKeyContainer(ref inputFirstKeyPointerRefAsKeyRef);  // The Insert key has the full PredicateCount of KeyPointers
                pendingContext.value = hlog.GetValueContainer(ref value);
                pendingContext.input = fasterSession.GetHeapContainer(ref input);
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = core.Constants.kInvalidAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
#endregion

#region Latch release
            LatchRelease:
            // No actual latching was done.
#endregion

            return status == OperationStatus.RETRY_NOW
                ? IndexInternalInsert(ref inputFirstKeyPointerRefAsKeyRef, ref value, ref input, ref context, ref pendingContext, fasterSession, sessionCtx, lsn)
                : status;
        }
   }
}