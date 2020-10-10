// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

extern alias FasterCore;

using FC = FasterCore::FASTER.core;
using FASTER.core;
using PSF.Index;
using System.Runtime.CompilerServices;

namespace FASTER.PSF
{
    internal class LivenessFunctions<TKVKey, TKVValue> : FC.IFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>
    {
        public struct Input
        {
            internal FC.LogAccessor<TKVKey, TKVValue> logAccessor;
        }

        public struct Output
        {
            private FC.IHeapContainer<TKVKey> keyContainer;
            private FC.IHeapContainer<TKVValue> valueContainer;
            internal long currentAddress;
            internal FC.RecordInfo recordInfo;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void SetHeapContainers(FC.IHeapContainer<TKVKey> kc, FC.IHeapContainer<TKVValue> vc)
            {
                this.keyContainer = kc;
                this.valueContainer = vc;
            }

            internal ref TKVKey GetKey() => ref this.keyContainer.Get();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void DetachHeapContainers(out FC.IHeapContainer<TKVKey> kc, out FC.IHeapContainer<TKVValue> vc)
            {
                kc = this.keyContainer;
                this.keyContainer = null;
                vc = this.valueContainer;
                this.valueContainer = null;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Set(ref Output other)
            {
                other.DetachHeapContainers(out this.keyContainer, out this.valueContainer);
                this.currentAddress = other.currentAddress;
                this.recordInfo = other.recordInfo;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Dispose()
            {
                this.keyContainer.Dispose();
                this.keyContainer = null;
                this.valueContainer.Dispose();
                this.valueContainer = null;
            }
        }

        public class Context
        {
            internal Output output;
            internal FC.Status status;
            
            internal Context()
            {
                this.output.recordInfo = default;
                this.status = FC.Status.OK;
            }
        }

        #region Supported IFunctions operations

        public virtual void ConcurrentReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (!(input.logAccessor is null))
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // because this is only called on in-memory addresses, not on a Read that has gone pending.
                output.currentAddress = logicalAddress;
        }

        public virtual void SingleReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (!(input.logAccessor is null))
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // if this is an in-memory call, or via ReadCompletionCallback's (..., RecordInfo recordInfo) argument if the Read has gone pending.
                output.currentAddress = logicalAddress;
        }

        public virtual void ReadCompletionCallback(ref TKVKey key, ref Input input, ref Output output, Context ctx, FC.Status status, FC.RecordInfo recordInfo)
        {
            ctx.output.Set(ref output);
            ctx.output.recordInfo = recordInfo;
            ctx.status = status;
        }

        #endregion Supported IFunctions operations

        #region Unsupported IFunctions operations
        const string errorMsg = "This IFunctions method should not be called in this context";

        public virtual bool ConcurrentWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);
        public virtual void SingleWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);

        public virtual void InitialUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);
        public virtual void CopyUpdater(ref TKVKey key, ref Input input, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress) => throw new PSFInternalErrorException(errorMsg);
        public virtual bool InPlaceUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);

        public virtual void RMWCompletionCallback(ref TKVKey key, ref Input input, Context ctx, FC.Status status) => throw new PSFInternalErrorException(errorMsg);
        public virtual void UpsertCompletionCallback(ref TKVKey key, ref TKVValue value, Context ctx) => throw new PSFInternalErrorException(errorMsg);
        public virtual void DeleteCompletionCallback(ref TKVKey key, Context ctx) => throw new PSFInternalErrorException(errorMsg);
        public virtual void CheckpointCompletionCallback(string sessionId, FC.CommitPoint commitPoint) => throw new PSFInternalErrorException(errorMsg);
        #endregion Unsupported IFunctions operations
    }
}
