// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//#define PSF_TRACE

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace PSF.Index
{
    // PSF-related internal function implementations for secondary FasterKV; these correspond to the similarly-named
    // functions in FasterImpl.cs.
    internal unsafe partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
        where TPSFKey : struct
        where TRecordId : struct
    {
        internal KeyAccessor<TPSFKey> KeyAccessor => (KeyAccessor<TPSFKey>)this.comparer;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PsfInternalRead<TInput, TOutput, TContext, FasterSession>(
                                    ref TPSFKey queryKeyPointerRefAsKeyRef,
                                    ref TInput input,
                                    ref TOutput output,
                                    long startAddress,
                                    ref TContext context,
                                    ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                    FasterSession fasterSession,
                                    FasterExecutionContext<TInput, TOutput, TContext> sessionCtx,
                                    long lsn)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, TInput, TOutput, TContext>
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var physicalAddress = default(long);
            var heldOperation = LatchOperation.None;

            var hash = comparer.GetHashCode64(ref queryKeyPointerRefAsKeyRef);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(hash, sessionCtx, fasterSession);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default;

            OperationStatus status;
            long logicalAddress;
            var usePreviousAddress = startAddress != Constants.kInvalidAddress;
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

            // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
            ref KeyPointer<TPSFKey> queryKeyPointer = ref KeyPointer<TPSFKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
            PsfTrace($"ReadKey: {this.KeyAccessor?.GetString(ref queryKeyPointer)} | hash {hash} |");

            if (tagExists)
            {
                logicalAddress = entry.Address;

#if false // TODOdcr: Support ReadCache in PSFs (must call this.KeyAccessor.GetRecordAddressFromKeyLogicalAddress) 
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

                    if (!comparer.Equals(ref queryKeyPointerRefAsKeyRef, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(physicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref queryKeyPointerRefAsKeyRef,
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
                pendingContext.recordInfo = hlog.GetInfo(physicalAddress);
                if (pendingContext.recordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.ConcurrentReader(ref queryKeyPointerRefAsKeyRef, ref input, ref hlog.GetValue(physicalAddress), ref output, logicalAddress);
                return OperationStatus.SUCCESS;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                pendingContext.recordInfo = hlog.GetInfo(physicalAddress);
                if (pendingContext.recordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                fasterSession.SingleReader(ref queryKeyPointerRefAsKeyRef, ref input, ref hlog.GetValue(physicalAddress), ref output, logicalAddress);
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
                pendingContext.key = new PsfQueryKeyContainer(ref queryKeyPointerRefAsKeyRef, this.KeyAccessor, this.hlog.bufferPool);
                pendingContext.input = input;
                pendingContext.output = output;
                pendingContext.userContext = context;
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = this.KeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.heldLatch = heldOperation;
                pendingContext.recordInfo.PreviousAddress = startAddress;
            }
#endregion

            return status;
        }

        [Conditional("PSF_TRACE")]
        private void PsfTrace(string message) => Console.Write(message);

        [Conditional("PSF_TRACE")]
        private void PsfTraceLine(string message = null) => Console.WriteLine(message ?? string.Empty);

        // PsfKeyContainer is necessary because VarLenBlittableAllocator.GetKeyContainer will use the size of the full
        // composite key (KeyPointerSize * PsfCount), but the query key has only one KeyPointer.
        private class PsfQueryKeyContainer : IHeapContainer<TPSFKey>
        {
            private readonly SectorAlignedMemory mem;

            public unsafe PsfQueryKeyContainer(ref TPSFKey key, KeyAccessor<TPSFKey> keyAccessor, SectorAlignedBufferPool pool)
            {
                var len = keyAccessor.KeyPointerSize;
                this.mem = pool.Get(len);
                Buffer.MemoryCopy(Unsafe.AsPointer(ref key), mem.GetValidPointer(), len, len);
            }

            public unsafe ref TPSFKey Get() => ref Unsafe.AsRef<TPSFKey>(this.mem.GetValidPointer());

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
        internal OperationStatus PsfInternalInsert<TInput, TOutput, TContext, FasterSession>(
                        ref TPSFKey firstKeyPointerRefAsKeyRef, ref TRecordId value, ref TInput input, ref TContext context,
                        ref PendingContext<TInput, TOutput, TContext> pendingContext,
                        FasterSession fasterSession,
                        FasterExecutionContext<TInput, TOutput, TContext> sessionCtx, long lsn)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, TInput, TOutput, TContext>
        {
            var status = default(OperationStatus);
            var latestRecordVersion = -1;

            ref CompositeKey<TPSFKey> compositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref firstKeyPointerRefAsKeyRef);

            // Update the KeyPointer links for chains with IsNullAt false (indicating a match with the
            // corresponding PSF) to point to the previous records for all keys in the composite key.
            // Note: We're not checking for a previous occurrence of the input value (the recordId) because
            // we are doing insert only here. At this time we do not do actual updates (this may be done
            // in a future PsfInternalUpdate).
            var psfCount = this.KeyAccessor.KeyCount;
            CASHelper* casHelpers = stackalloc CASHelper[psfCount];
            int startOfKeysOffset = 0;
            PsfTrace($"Insert: {this.KeyAccessor.GetString(ref compositeKey)} | rId {value} |");
            for (var psfOrdinal = 0; psfOrdinal < psfCount; ++psfOrdinal)
            {
                // For RCU, or in case we had to retry due to CPR_SHIFT and somehow managed to delete
                // the previously found record, clear out the chain link pointer.
                this.KeyAccessor.SetPreviousAddress(ref compositeKey, psfOrdinal, Constants.kInvalidAddress);

                this.KeyAccessor.SetOffsetToStartOfKeys(ref compositeKey, psfOrdinal, startOfKeysOffset);
                startOfKeysOffset += this.KeyAccessor.KeyPointerSize;

                ref CASHelper casHelper = ref casHelpers[psfOrdinal];
                if (this.KeyAccessor.IsNullAt(ref compositeKey, psfOrdinal))
                {
                    casHelper.isNull = true;
                    PsfTrace($" null");
                    continue;
                }

                casHelper.hash = this.KeyAccessor.GetHashCode64(ref compositeKey, psfOrdinal);
                var tag = (ushort)((ulong)casHelper.hash >> Constants.kHashTagShift);

                if (sessionCtx.phase != Phase.REST)
                    HeavyEnter(casHelper.hash, sessionCtx, fasterSession);

#region Look up record in in-memory HybridLog
                FindOrCreateTag(casHelper.hash, tag, ref casHelper.bucket, ref casHelper.slot, ref casHelper.entry, hlog.BeginAddress);

                // For PSFs, the addresses stored in the hash table point to KeyPointer entries, not the record header.
                var logicalAddress = casHelper.entry.Address;
                if (logicalAddress >= hlog.BeginAddress)
                {
                    PsfTrace($" {logicalAddress}");

                    if (logicalAddress < hlog.BeginAddress)
                        continue;

                    if (logicalAddress >= hlog.HeadAddress)
                    {
                        // Note that we do not backtrace here because we are not replacing the value at the key; 
                        // instead, we insert at the top of the hash chain. Track the latest record version we've seen.
                        long physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                        var recordAddress = this.KeyAccessor.GetRecordAddressFromKeyPhysicalAddress(physicalAddress);
                        if (hlog.GetInfo(physicalAddress).Tombstone)
                        {
                            // The chain might extend past a tombstoned record so we must include it in the chain
                            // unless its prevLink at psfOrdinal is invalid.
                            var prevAddress = this.KeyAccessor.GetPreviousAddress(physicalAddress);
                            if (prevAddress < hlog.BeginAddress)
                                continue;
                        }
                        latestRecordVersion = Math.Max(latestRecordVersion, hlog.GetInfo(recordAddress).Version);
                    }

                    this.KeyAccessor.SetPreviousAddress(ref compositeKey, psfOrdinal, logicalAddress);
                }
                else
                {
                    PsfTrace($" 0");
                }
#endregion
            }

#region Entry latch operation
            // No actual checkpoint locking will be done because this is Insert; only the current thread can write to
            // the record we're about to create, and no readers can see it until it is successfully inserted. However, we
            // must pivot and retry any insertions if we have seen a later version in any record in the hash table.
            if (sessionCtx.phase == Phase.PREPARE && latestRecordVersion != -1 && latestRecordVersion > sessionCtx.version)
            {
                PsfTraceLine("CPR_SHIFT_DETECTED");
                status = OperationStatus.CPR_SHIFT_DETECTED;
                goto CreatePendingContext; // Pivot Thread
            }
            Debug.Assert(latestRecordVersion <= sessionCtx.version);
            goto CreateNewRecord;
#endregion

#region Create new record in the mutable region
            CreateNewRecord:
            {
                var functions = (context as PSFContext).Functions as IInputAccessor<TInput>;

                // Create the new record. Because we are updating multiple hash buckets, mark the record as invalid to start,
                // so it is not visible until we have successfully updated all chains.
                var recordSize = hlog.GetRecordSize(ref firstKeyPointerRefAsKeyRef, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress, sessionCtx, fasterSession);
                var newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newPhysicalAddress), sessionCtx.version,
                                     final:true, tombstone: functions.IsDelete(ref input), invalidBit:true,
                                     Constants.kInvalidAddress);  // We manage all prev addresses within CompositeKey
                ref TPSFKey storedFirstKeyPointerRefAsKeyRef = ref hlog.GetKey(newPhysicalAddress);
                ref CompositeKey<TPSFKey> storedKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref firstKeyPointerRefAsKeyRef, ref storedFirstKeyPointerRefAsKeyRef);
                hlog.ShallowCopy(ref value, ref hlog.GetValue(newPhysicalAddress));

                PsfTraceLine();
                newLogicalAddress += RecordInfo.GetLength();
                for (var psfOrdinal = 0; psfOrdinal < psfCount; ++psfOrdinal, newLogicalAddress += this.KeyAccessor.KeyPointerSize)
                {
                    var casHelper = casHelpers[psfOrdinal];
                    var tag = (ushort)((ulong)casHelper.hash >> Constants.kHashTagShift);

                    PsfTrace($"    ({psfOrdinal}): {casHelper.hash} {tag} | newLA {newLogicalAddress} | prev {casHelper.entry.word}");
                    if (casHelper.isNull)
                    {
                        PsfTraceLine(" null");
                        continue;
                    }

                    var newEntry = default(HashBucketEntry);
                    newEntry.Tag = tag;
                    newEntry.Address = newLogicalAddress & Constants.kAddressMask;
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
                            PsfTrace($" / {foundEntry.Address}");
                            casHelper.entry.word = foundEntry.word;
                            this.KeyAccessor.SetPreviousAddress(ref storedKey, psfOrdinal, foundEntry.Address);
                            continue;
                        }

                        // We can't satisfy the always-downward invariant, so leave the record marked Invalid and go
                        // around again to try inserting another record.
                        PsfTraceLine("RETRY_NOW");
                        status = OperationStatus.RETRY_NOW;
                        goto LatchRelease;
                    }

                    // Success for this PSF.
                    PsfTraceLine(" ins");
                    hlog.GetInfo(newPhysicalAddress).Invalid = false;
                }

                storedKey.ClearUpdateFlags(this.KeyAccessor.KeyCount, this.KeyAccessor.KeyPointerSize);
                status = OperationStatus.SUCCESS;
                goto LatchRelease;
            }
#endregion

#region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.INSERT;
                pendingContext.key = hlog.GetKeyContainer(ref firstKeyPointerRefAsKeyRef);  // The Insert key has the full PsfCount of KeyPointers
                pendingContext.value = hlog.GetValueContainer(ref value);
                pendingContext.input = input;
                pendingContext.userContext = default;
                pendingContext.entry.word = default;
                pendingContext.logicalAddress = Constants.kInvalidAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
            }
#endregion

#region Latch release
            LatchRelease:
            // No actual latching was done.
#endregion

            return status == OperationStatus.RETRY_NOW
                ? PsfInternalInsert(ref firstKeyPointerRefAsKeyRef, ref value, ref input, ref context, ref pendingContext, fasterSession, sessionCtx, lsn)
                : status;
        }
   }
}