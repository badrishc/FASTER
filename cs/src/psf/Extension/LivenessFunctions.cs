// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using PSF.Index;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.PSF
{
    internal class LivenessFunctions<TKVKey, TKVValue> : IAdvancedFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, 
                                                                            LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>
    {
        public struct Input
        {
            internal LogAccessor<TKVKey, TKVValue> logAccessor;
        }

        public struct Output
        {
            private IHeapContainer<TKVKey> keyContainer;
            private IHeapContainer<TKVValue> valueContainer;
            internal long currentAddress;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void SetHeapContainers(IHeapContainer<TKVKey> kc, IHeapContainer<TKVValue> vc)
            {
                this.keyContainer = kc;
                this.valueContainer = vc;
            }

            internal ref TKVKey GetKey() => ref this.keyContainer.Get();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void DetachHeapContainers(out IHeapContainer<TKVKey> kc, out IHeapContainer<TKVValue> vc)
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
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Dispose()
            {
                if (this.keyContainer is {})
                {
                    this.keyContainer.Dispose();
                    this.keyContainer = null;
                }
                if (this.valueContainer is {})
                {
                    this.valueContainer.Dispose();
                    this.valueContainer = null;
                }
            }
        }

        public class Context
        {
            internal Output output;
            internal Status PendingResultStatus;
            
            internal Context()
            {
                this.PendingResultStatus = Status.OK;
            }
        }

        private readonly FasterKV<TKVKey, TKVValue> fkv;

        internal LivenessFunctions(FasterKV<TKVKey, TKVValue> fkv) => this.fkv = fkv;

        #region Supported IFunctions operations

        public virtual void ConcurrentReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (input.logAccessor is {})
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // because this is only called on in-memory addresses, not on a Read that has gone pending.
                output.currentAddress = logicalAddress;
        }

        public virtual void SingleReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (input.logAccessor is {})
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // if this is an in-memory call, or via ReadCompletionCallback's (..., RecordInfo recordInfo) argument if the Read has gone pending.
                output.currentAddress = logicalAddress;
        }

        public virtual void ReadCompletionCallback(ref TKVKey key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
        {
            // If ctx is null, this was an async call, and we'll get output via Complete().
            if (ctx is {})
            {
                ctx.output.Set(ref output);
                ctx.PendingResultStatus = status;
            }
        }

        public virtual void SingleWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress)
        {
            // This is called when the primary FKV copies reads from IO to the read cache or tail of the log.
            this.fkv.hlog.ShallowCopy(ref src, ref dst);
        }

        #endregion Supported IFunctions operations

        #region Unsupported IAdvancedFunctions operations
        const string errorMsg = "This IAdvancedFunctions method should not be called in this context";

        public virtual bool ConcurrentWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);

        public virtual void InitialUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);
        public virtual bool NeedCopyUpdate(ref TKVKey key, ref Input input, ref TKVValue value) => throw new PSFInternalErrorException(errorMsg);
        public virtual void CopyUpdater(ref TKVKey key, ref Input input, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress) => throw new PSFInternalErrorException(errorMsg);
        public virtual bool InPlaceUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new PSFInternalErrorException(errorMsg);

        public virtual void RMWCompletionCallback(ref TKVKey key, ref Input input, Context ctx, Status status) => throw new PSFInternalErrorException(errorMsg);
        public virtual void UpsertCompletionCallback(ref TKVKey key, ref TKVValue value, Context ctx) => throw new PSFInternalErrorException(errorMsg);
        public virtual void DeleteCompletionCallback(ref TKVKey key, Context ctx) => throw new PSFInternalErrorException(errorMsg);
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) => throw new PSFInternalErrorException(errorMsg);
        #endregion Unsupported IAdvancedFunctions operations
    }
}
