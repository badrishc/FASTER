// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetIndex;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.SubsetIndex
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

        public void ConcurrentReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (input.logAccessor is {})
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // because this is only called on in-memory addresses, not on a Read that has gone pending.
                output.currentAddress = logicalAddress;
        }

        public void SingleReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
        {
            if (input.logAccessor is {})
                output.SetHeapContainers(input.logAccessor.GetKeyContainer(ref key), input.logAccessor.GetValueContainer(ref value));
            else
                // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                // if this is an in-memory call, or via ReadCompletionCallback's (..., RecordInfo recordInfo) argument if the Read has gone pending.
                output.currentAddress = logicalAddress;
        }

        public void ReadCompletionCallback(ref TKVKey key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
        {
            // If ctx is null, this was an async call, and we'll get output via Complete().
            if (ctx is {})
            {
                ctx.output.Set(ref output);
                ctx.PendingResultStatus = status;
            }
        }

        public void SingleWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress)
        {
            // For reads, this is called when the primary FKV copies reads from IO to the read cache (Liveness only uses ReadAtAddress variants,
            // which set pendingContext.ReadAtAddress, which bypasses copying pending reads to the tail of the log).
            Debug.Assert(!this.fkv.CopyReadsToTail, "Liveness check should not copy pending reads to the tail of the log");
            Debug.Assert(this.fkv.RecordAccessor.IsReadCacheAddress(logicalAddress), "Liveness check SingleWriter() should be called only when copying pending reads to the readcache");
            var log = this.fkv.readcache;
            if (log is GenericAllocator<TKVKey, TKVValue>) {  // TODO is there a cleaner way to do this?
                dst = src;
            } else
            {
                Debug.Assert(log is BlittableAllocator<TKVKey, TKVValue> || log is GenericAllocator<TKVKey, TKVValue>);
                long physicalAddress = log.GetPhysicalAddress(logicalAddress & ~Constants.kReadCacheBitMask);
                unsafe { Debug.Assert((long)Unsafe.AsPointer(ref dst) == (long)Unsafe.AsPointer(ref log.GetValue(physicalAddress))); }
                log.Serialize(ref src, physicalAddress);
            }
        }

        #endregion Supported IFunctions operations

        #region Unsupported IAdvancedFunctions operations
        const string errorMsg = "This IAdvancedFunctions method should not be called in this context";

        public bool ConcurrentWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress) => throw new InternalErrorExceptionSI(errorMsg);

        public void InitialUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new InternalErrorExceptionSI(errorMsg);
        public bool NeedCopyUpdate(ref TKVKey key, ref Input input, ref TKVValue value) => throw new InternalErrorExceptionSI(errorMsg);
        public void CopyUpdater(ref TKVKey key, ref Input input, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress) => throw new InternalErrorExceptionSI(errorMsg);
        public bool InPlaceUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress) => throw new InternalErrorExceptionSI(errorMsg);

        public void RMWCompletionCallback(ref TKVKey key, ref Input input, Context ctx, Status status) => throw new InternalErrorExceptionSI(errorMsg);
        public void UpsertCompletionCallback(ref TKVKey key, ref TKVValue value, Context ctx) => throw new InternalErrorExceptionSI(errorMsg);
        public void DeleteCompletionCallback(ref TKVKey key, Context ctx) => throw new InternalErrorExceptionSI(errorMsg);
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) => throw new InternalErrorExceptionSI(errorMsg);
        #endregion Unsupported IAdvancedFunctions operations
    }
}
