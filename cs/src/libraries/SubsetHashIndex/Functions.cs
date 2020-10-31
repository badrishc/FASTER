// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using FASTER.core;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.libraries.SubsetHashIndex
{
    internal unsafe partial class FasterKVSHI<TPKey, TRecordId> : FasterKV<TPKey, TRecordId>
    {
        internal class Functions : IAdvancedFunctions<TPKey, TRecordId, Input, Output, Context>, IInputAccessor<Input>
        {
            readonly KeyAccessor<TPKey, TRecordId> keyAccessor;
            readonly RecordAccessor<TPKey, TRecordId> recordAccessor;
            internal readonly Queue<Output> Queue;

            internal Functions(KeyAccessor<TPKey, TRecordId> keyAcc, RecordAccessor<TPKey, TRecordId> recordAcc)
            {
                this.keyAccessor = keyAcc;
                this.recordAccessor = recordAcc;
                this.Queue = new Queue<Output>();
            }

            internal void Clear()
            {
                // We should have drained any of these remaining, unless there was an error.
                this.Queue.Clear();
            }

            #region IInputAccessor

            public long GroupId { get; }

            public bool IsDelete(ref Input input) => input.IsDelete;
            public bool SetDelete(ref Input input, bool value) => input.IsDelete = value;

            #endregion IInputAccessor

            #region IFunctions implementation

            private const string NotUsedForSubsetHashIndex = "SubsetHashIndex-implementing FasterKVs should not use this IFunctions method";

            #region Upserts
            public bool ConcurrentWriter(ref TPKey _, ref TRecordId src, ref TRecordId dst, long logicalAddress) => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void SingleWriter(ref TPKey _, ref TRecordId src, ref TRecordId dst, long logicalAddress) => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void UpsertCompletionCallback(ref TPKey _, ref TRecordId value, Context ctx) => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);
#endregion Upserts

            #region Reads
            public void ConcurrentReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref TRecordId value, ref Output output, long logicalAddress)
            {
                // Note: ConcurrentReader is not called for ReadCache, even if we eventually support ReadCache in SubsetHashIndex secondary KVs.
                Debug.Assert(this.recordAccessor.IsInMemory(logicalAddress));
                CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyInMemoryDataToOutput(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref TRecordId value, ref Output output, long logicalAddress)
            {
                ref KeyPointer<TPKey> storedKeyPointer = ref this.keyAccessor.GetKeyPointerRefFromKeyPointerLogicalAddress(logicalAddress);
                Debug.Assert(input.PredicateOrdinal == storedKeyPointer.PredicateOrdinal, "Mismatched input and stored Predicate ordinal");

                output.RecordId = value;
                output.PreviousAddress = storedKeyPointer.PreviousAddress;
                
                output.IsDeleted = storedKeyPointer.IsDeleted;

#if DEBUG
                ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
                Debug.Assert(input.PredicateOrdinal == queryKeyPointer.PredicateOrdinal, "Mismatched input and query Predicate ordinal");
#endif
            }

            public unsafe void SingleReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref TRecordId value, ref Output output, long logicalAddress)
            {
                if (!recordAccessor.IsLogAddress(logicalAddress))
                {
                    // This is a ReadCache record. Note if we do support ReadCache in SubsetHashIndex secondary KVs: ReadCompletionCallback won't be called, so no need to flag it in Output.
                    Debug.Fail("ReadCache is not supported in SubsetHashIndex secondary KVs");
                    return;
                }

                if (this.recordAccessor.IsInMemory(logicalAddress))
                {
                    CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
                    return;
                }

                // This record is not in memory, which means we're being called from InternalCompletePendingRead. We can't dereference logicalAddress,
                // but KeyAccessor can help us navigate to the query key.
                long recordPhysicalAddress = this.keyAccessor.GetRecordAddressFromValueRef(ref value);

                ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
                ref KeyPointer<TPKey> storedKeyPointer = ref this.keyAccessor.GetKeyPointerRefFromRecordPhysicalAddress(recordPhysicalAddress, queryKeyPointer.PredicateOrdinal);

                output.RecordId = value;
                output.PreviousAddress = storedKeyPointer.PreviousAddress;
                output.IsDeleted = storedKeyPointer.IsDeleted;

                Debug.Assert(input.PredicateOrdinal == queryKeyPointer.PredicateOrdinal, "Mismatched input and query Predicate ordinal");
                Debug.Assert(input.PredicateOrdinal == storedKeyPointer.PredicateOrdinal, "Mismatched input and stored Predicate ordinal");
            }

            public void ReadCompletionCallback(ref TPKey _, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
            {
                output.PendingResultStatus = status;
                this.Queue.Enqueue(output);
            }
            #endregion Reads

            #region RMWs
            public bool NeedCopyUpdate(ref TPKey _, ref Input input, ref TRecordId value)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void CopyUpdater(ref TPKey _, ref Input input, ref TRecordId oldValue, ref TRecordId newValue, long oldAddress, long newAddress)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void InitialUpdater(ref TPKey _, ref Input input, ref TRecordId value, long logicalAddress)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public bool InPlaceUpdater(ref TPKey _, ref Input input, ref TRecordId value, long logicalAddress)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void RMWCompletionCallback(ref TPKey _, ref Input input, Context ctx, Status status)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);
#endregion RMWs

            public void DeleteCompletionCallback(ref TPKey _, Context ctx)
                => throw new InternalErrorExceptionSHI(NotUsedForSubsetHashIndex);

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
            }
            #endregion IFunctions implementation
        }
    }
}
