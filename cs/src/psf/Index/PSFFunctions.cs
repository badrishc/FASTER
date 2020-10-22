// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using FASTER.core;
using System.Collections.Generic;
using System.Diagnostics;

namespace PSF.Index
{
    internal unsafe partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        internal class PSFFunctions : IAdvancedFunctions<TPSFKey, TRecordId, PSFInput, PSFOutput, PSFContext>, IInputAccessor<PSFInput>
        {
            readonly KeyAccessor<TPSFKey, TRecordId> keyAccessor;
            readonly RecordAccessor<TPSFKey, TRecordId> recordAccessor;
            internal readonly Queue<PSFOutput> Queue;

            internal PSFFunctions(KeyAccessor<TPSFKey, TRecordId> keyAcc, RecordAccessor<TPSFKey, TRecordId> recordAcc)
            {
                this.keyAccessor = keyAcc;
                this.recordAccessor = recordAcc;
                this.Queue = new Queue<PSFOutput>();
            }

            internal void Clear()
            {
                // We should have drained any of these remaining, unless there was an error.
                this.Queue.Clear();
            }

            #region IInputAccessor

            public long GroupId { get; }

            public bool IsDelete(ref PSFInput input) => input.IsDelete;
            public bool SetDelete(ref PSFInput input, bool value) => input.IsDelete = value;

            #endregion IInputAccessor

            #region IFunctions implementation

            private const string NotUsedForPSF = "PSF-implementing FasterKVs should not use this IFunctions method";

            #region Upserts
            public bool ConcurrentWriter(ref TPSFKey _, ref TRecordId src, ref TRecordId dst, long logicalAddress) => throw new PSFInternalErrorException(NotUsedForPSF);

            public void SingleWriter(ref TPSFKey _, ref TRecordId src, ref TRecordId dst, long logicalAddress) => throw new PSFInternalErrorException(NotUsedForPSF);

            public void UpsertCompletionCallback(ref TPSFKey _, ref TRecordId value, PSFContext ctx) => throw new PSFInternalErrorException(NotUsedForPSF);
#endregion Upserts

            #region Reads
            public void ConcurrentReader(ref TPSFKey queryKeyPointerRefAsKeyRef, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long logicalAddress)
            {
                // Note: ConcurrentReader is not called for ReadCache, even if we eventually support ReadCache in PSF secondary KVs.
                Debug.Assert(this.recordAccessor.IsInMemory(logicalAddress));
                CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
            }

            private void CopyInMemoryDataToOutput(ref TPSFKey queryKeyPointerRefAsKeyRef, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long logicalAddress)
            {
                output.RecordId = value;
                output.Tombstone = this.recordAccessor.IsTombstone(logicalAddress);

                ref KeyPointer<TPSFKey> storedKeyPointer = ref this.keyAccessor.GetKeyPointerRefFromLogicalAddress(logicalAddress);

#if DEBUG
                ref KeyPointer<TPSFKey> queryKeyPointer = ref KeyPointer<TPSFKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
                Debug.Assert(queryKeyPointer.PsfOrdinal == input.PsfOrdinal, "Mismatched query and input PSF ordinal");
                Debug.Assert(storedKeyPointer.PsfOrdinal == input.PsfOrdinal, "Mismatched stored and input PSF ordinal");
#endif
                output.PreviousAddress = storedKeyPointer.PreviousAddress;
            }

            public unsafe void SingleReader(ref TPSFKey queryKeyPointerRefAsKeyRef, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long logicalAddress)
            {
                if (!recordAccessor.IsLogAddress(logicalAddress))
                {
                    // This is a ReadCache record. Note if we do support ReadCache in PSF secondary KVs: ReadCompletionCallback won't be called, so no need to flag it in Output.
                    Debug.Fail("ReadCache is not supported in PSF secondary KVs");
                    return;
                }

                if (this.recordAccessor.IsInMemory(logicalAddress))
                {
                    CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
                    return;
                }

                // This record is not in memory, which means we're being called from InternalCompletePendingRead. We can't dereference logicalAddress,
                // but KeyAccessor can help us navigate to the query key.


                output.RecordId = value;
            }

            public void ReadCompletionCallback(ref TPSFKey _, ref PSFInput input, ref PSFOutput output, PSFContext ctx, Status status, RecordInfo recordInfo)
            {
                output.Tombstone = recordInfo.Tombstone;
                this.Queue.Enqueue(output);
            }
            #endregion Reads

            #region RMWs
            public bool NeedCopyUpdate(ref TPSFKey _, ref PSFInput input, ref TRecordId value)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public void CopyUpdater(ref TPSFKey _, ref PSFInput input, ref TRecordId oldValue, ref TRecordId newValue, long oldAddress, long newAddress)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public void InitialUpdater(ref TPSFKey _, ref PSFInput input, ref TRecordId value, long logicalAddress)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public bool InPlaceUpdater(ref TPSFKey _, ref PSFInput input, ref TRecordId value, long logicalAddress)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public void RMWCompletionCallback(ref TPSFKey _, ref PSFInput input, PSFContext ctx, Status status)
                => throw new PSFInternalErrorException(NotUsedForPSF);
#endregion RMWs

            public void DeleteCompletionCallback(ref TPSFKey _, PSFContext ctx)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
            }
            #endregion IFunctions implementation
        }
    }
}
