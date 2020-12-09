// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetIndex;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Collections.Generic;

namespace FASTER.indexes.SubsetIndex
{
    internal class IndexingFunctions<TKVKey, TKVValue, Input, Output, Context, UserFunctions> : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
        where UserFunctions : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
    {
        // Permanent
        private readonly UserFunctions userFunctions;
        private readonly LogAccessor<TKVKey, TKVValue> logAccessor;
        private readonly RecordAccessor<TKVKey, TKVValue> recordAccessor;
        private readonly SubsetIndex<FasterKVProviderData<TKVKey, TKVValue>, long> subsetIndex;

        // Ephemeral
        internal ChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long> ChangeTracker;
        internal long LogicalAddress;
        internal Queue<ChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long>> Queue = new Queue<ChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long>>();
        internal IndexableOperation IndexableOp;

        internal IndexingFunctions(UserFunctions userFunctions, LogAccessor<TKVKey, TKVValue> logAccessor, RecordAccessor<TKVKey, TKVValue> recAcc,
                                  SubsetIndex<FasterKVProviderData<TKVKey, TKVValue>, long> subsetIndex)
        {
            this.userFunctions = userFunctions;
            this.logAccessor = logAccessor;
            this.recordAccessor = recAcc;
            this.subsetIndex = subsetIndex;
        }

        internal void Clear()
        {
            this.ChangeTracker = null;
            this.LogicalAddress = Constants.kInvalidAddress;
            this.IndexableOp = IndexableOperation.None;
        }

        internal bool IsSet => this.ChangeTracker is {} || this.LogicalAddress != Constants.kInvalidAddress;

        #region IFunctions implementations
        public void ConcurrentReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
            => this.userFunctions.ConcurrentReader(ref key, ref input, ref value, ref output, logicalAddress);
        public void SingleReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddresss)
            => this.userFunctions.SingleReader(ref key, ref input, ref value, ref output, logicalAddresss);

        public void SingleWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress)
        {
            this.userFunctions.SingleWriter(ref key, ref src, ref dst, logicalAddress);

            // This is called in the following cases:
            //  IndexableOperation.Upsert:
            //      - Upsert did not find the key so this is a pure insert.
            //      - Upsert found a key that is on-disk; by design it does not fetch the record from disk to verify a key match. Instead, fasterKV
            //        writes a new record at the tail, and the old record will fail the liveness check at query time which avoids duplicates.
            //      - Upsert found a deleted record, so a new record is inserted (and we don't have the previous value).
            //  IndexableOperation.CompletePending: a pending read from disk is completing and copying the record to:
            //      - The readcache, which should not be considered in indexing, and does not affect the validity of the RecordId.
            //      - The tail of the log, which means we must update it.
            // For all these cases, we have no oldAddress to do an RCU, so we simply insert a new record.

            // Skip writes to the read cache.
            if (!this.recordAccessor.IsValid(logicalAddress))
                return;

            // In all non-readcache cases, the record should be in the primary FKV.Log memory.
            Debug.Assert(this.recordAccessor.IsInMemory(logicalAddress));
            if (this.IndexableOp == IndexableOperation.Upsert)
            {
                // Do an insert via the fast path (without changeTracker).
                this.LogicalAddress = logicalAddress;
            }
            else
            {
                // We are in pending read completion, so create the changeTracker and let ReadCompletionCallback store it in the context.
                SetBeforeData(ref key, ref src, logicalAddress, isIpu: false);
            }
            return;
        }

        public bool ConcurrentWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress)
        {
            // Save the PreUpdate values. Note: this is only called for in-memory writes.
            var isDelete = this.recordAccessor.IsTombstone(logicalAddress);
            SetBeforeData(ref key, ref dst, logicalAddress, isIpu: !isDelete);
            if (this.userFunctions.ConcurrentWriter(ref key, ref src, ref dst, logicalAddress))
            {
                this.ChangeTracker.UpdateOp = isDelete ? UpdateOperation.Delete : UpdateOperation.IPU;
                if (!isDelete)
                    SetAfterData(ref key, ref dst, logicalAddress);
                return true;
            }
            return false;
        }

        public void InitialUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress)
        {
            this.userFunctions.InitialUpdater(ref key, ref input, ref value, logicalAddress);

            // Key not found or the record was tombstoned, so this is an Insert only. This cannot go through the fast path because it does
            // not have a value passed in (instead, it has the input).
            SetBeforeData(ref key, ref value, logicalAddress, isIpu: false);
            this.ChangeTracker.UpdateOp = UpdateOperation.Insert;
        }
        public bool NeedCopyUpdate(ref TKVKey key, ref Input input, ref TKVValue oldValue)
            => this.userFunctions.NeedCopyUpdate(ref key, ref input, ref oldValue);
        public void CopyUpdater(ref TKVKey key, ref Input input, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress)
        {
            // The old record was valid but not in mutable range, so this is an RCU
            this.userFunctions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, oldLogicalAddress, newLogicalAddress);

            // Note: oldLogicalAddress may not be in memory.
            SetRCU(ref key, ref oldValue, ref newValue, oldLogicalAddress, newLogicalAddress);
        }
        public bool InPlaceUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress)
        {
            // Get the PreUpdate values (or the secondary FKV position in the IPUCache).
            SetBeforeData(ref key, ref value, logicalAddress, isIpu: true);
            if (this.userFunctions.InPlaceUpdater(ref key, ref input, ref value, logicalAddress))
            {
                SetAfterData(ref key, ref value, logicalAddress);
                this.ChangeTracker.UpdateOp = UpdateOperation.IPU;
                return true;
            }
            this.ChangeTracker = null;
            return false;
        }

        private void EnqueueTracker()
        {
            if (this.ChangeTracker is { })
                this.Queue.Enqueue(this.ChangeTracker);
            this.Clear();
        }

        public void ReadCompletionCallback(ref TKVKey key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
        {
            this.EnqueueTracker();
            this.userFunctions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordInfo);
        }

        public void RMWCompletionCallback(ref TKVKey key, ref Input input, Context ctx, Status status)
        {
            this.EnqueueTracker();
            this.userFunctions.RMWCompletionCallback(ref key, ref input, ctx, status);
        }

        public void UpsertCompletionCallback(ref TKVKey key, ref TKVValue value, Context ctx)
        {
            if (this.ChangeTracker is {})
                this.ChangeTracker.UpdateOp = UpdateOperation.Insert;
            this.EnqueueTracker();
            this.userFunctions.UpsertCompletionCallback(ref key, ref value, ctx);
        }

        public void DeleteCompletionCallback(ref TKVKey key, Context ctx)
        {
            this.userFunctions.DeleteCompletionCallback(ref key, ctx);
            if (this.ChangeTracker is { })
            {
                this.ChangeTracker.UpdateOp = UpdateOperation.Delete;
                this.EnqueueTracker();
            }
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            => this.userFunctions.CheckpointCompletionCallback(sessionId, commitPoint);

        #endregion IFunctions implementations

        #region Utilities

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private FasterKVProviderData<TKVKey, TKVValue> CreateProviderData(ref TKVKey key, ref TKVValue value) 
            => new FasterKVProviderData<TKVKey, TKVValue>(this.logAccessor.GetKeyContainer(ref key), this.logAccessor.GetValueContainer(ref value));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetBeforeData(ref TKVKey key, ref TKVValue value, long logicalAddress, bool isIpu)
        {
            // If the value has objects, then an in-place RMW to the data in that object will also affect BeforeData, so we must execute the predicates now for IPU. // TODOperf this is in session lock
            // If you Read an object value and modify that fetched "ref value" directly, you will break the hash index (the before data is overwritten before we have
            // a chance to see it and create the keys). An Upsert must use a separate value.
            this.ChangeTracker ??= this.subsetIndex.CreateChangeTracker();
            this.subsetIndex.SetBeforeData(this.ChangeTracker, CreateProviderData(ref key, ref value), logicalAddress, isIpu && this.recordAccessor.ValueHasObjects());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetAfterData(ref TKVKey key, ref TKVValue value, long logicalAddress)
            => this.subsetIndex.SetAfterData(this.ChangeTracker, CreateProviderData(ref key, ref value), logicalAddress);

        private void SetRCU(ref TKVKey key, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress)
        {
            this.ChangeTracker = this.subsetIndex.CreateChangeTracker();
            SetBeforeData(ref key, ref oldValue, oldLogicalAddress, isIpu: false);
            SetAfterData(ref key, ref newValue, newLogicalAddress);
            this.ChangeTracker.UpdateOp = UpdateOperation.RCU;
        }
        #endregion Utilities
    }
}
