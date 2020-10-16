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
        internal class PSFFunctions : IFunctions<TPSFKey, TRecordId, PSFInput, PSFOutput, PSFContext>, IInputAccessor<PSFInput>
        {
            readonly KeyAccessor<TPSFKey> keyAccessor;
            readonly RecordAccessor<TPSFKey, TRecordId> recordAccessor;
            internal readonly Queue<PSFOutput> Queue;

            internal PSFFunctions(KeyAccessor<TPSFKey> keyAcc, RecordAccessor<TPSFKey, TRecordId> recordAcc)
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
            public bool ConcurrentWriter(ref TPSFKey _, ref TRecordId src, ref TRecordId dst, long address) => throw new PSFInternalErrorException(NotUsedForPSF);

            public void SingleWriter(ref TPSFKey _, ref TRecordId src, ref TRecordId dst, long address) => throw new PSFInternalErrorException(NotUsedForPSF);

            public void UpsertCompletionCallback(ref TPSFKey _, ref TRecordId value, PSFContext ctx) => throw new PSFInternalErrorException(NotUsedForPSF);
#endregion Upserts

            #region Reads
            public void ConcurrentReader(ref TPSFKey key, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long address)
            {
                // Note: ConcurrentReader is not called for ReadCache.
                Debug.Assert(this.recordAccessor.IsInMemory(address));
                StoreOutput(ref key, ref input, ref value, ref output, address);
            }

            private void StoreOutput(ref TPSFKey key, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long address)
            {
                if (!recordAccessor.IsLogAddress(address))
                {
                    // This is a ReadCache record. Note: ReadCompletionCallback won't be called, so no need to flag it in Output.
                    return;
                }

                output.RecordId = value;
                if (this.recordAccessor.IsInMemory(address))
                    output.Tombstone = this.recordAccessor.IsTombstone(address);

                ref CompositeKey<TPSFKey> compositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref key);
                ref KeyPointer<TPSFKey> keyPointer = ref this.keyAccessor.GetKeyPointerRef(ref compositeKey, input.PsfOrdinal);
                Debug.Assert(keyPointer.PsfOrdinal == input.PsfOrdinal, "Visit found mismatched PSF ordinal");
                output.PreviousLogicalAddress = keyPointer.PreviousAddress;
            }

            public unsafe void SingleReader(ref TPSFKey key, ref PSFInput input, ref TRecordId value, ref PSFOutput output, long address)
            {
                // Note: if !this.recordAccessor.IsInMemory(address), we'll get the RecordInfo in ReadCompletionCallback and set tombstone there.
                StoreOutput(ref key, ref input, ref value, ref output, address);
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

            public void InitialUpdater(ref TPSFKey _, ref PSFInput input, ref TRecordId value, long address)
                => throw new PSFInternalErrorException(NotUsedForPSF);

            public bool InPlaceUpdater(ref TPSFKey _, ref PSFInput input, ref TRecordId value, long address)
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
