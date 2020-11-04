// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// Provides access to the <see cref="CompositeKey{TPKey}"/> internals that are hidden behind
    /// the Key typeparam of the secondary FasterKV.
    /// </summary>
    /// <typeparam name="TPKey">The type of the Key returned by a Predicate function</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    internal unsafe class KeyAccessor<TPKey, TRecordId> : IFasterEqualityComparer<TPKey>
    {
        private readonly IFasterEqualityComparer<TPKey> userComparer;
        private AllocatorBase<TPKey, TRecordId> hlog;

        internal KeyAccessor(IFasterEqualityComparer<TPKey> userComparer, int keyCount, int keyPointerSize)
        {
            this.userComparer = userComparer;
            this.KeyCount = keyCount;
            this.KeyPointerSize = keyPointerSize;
        }

        internal void SetLog(AllocatorBase<TPKey, TRecordId> hlog)
        {
            // Some logic here assumes we are using VarLenBlittableAllocator.
            Debug.Assert(hlog is VariableLengthBlittableAllocator<TPKey, TRecordId>, "Expected allocator type not found");
            this.hlog = hlog;
        }

        public int KeyCount { get; }

        public int KeyPointerSize { get; }

        // Note: TotalKeySize, and similar logic here and in CompositeKey, assume we do not skip null TPKeys in the record.
        public int TotalKeySize => KeyCount * KeyPointerSize;

        #region IFasterEqualityComparer implementation

        public long GetHashCode64(ref TPKey queryKeyPointerRefAsKeyRef) 
            => this.GetHashCode64(ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef));

        public bool Equals(ref TPKey queryKeyPointerRefAsKeyRef, ref TPKey storedKeyPointerRefAsKeyRef)
        {
            ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);

            // storedKeyPointerAsKeyRef is always a reference to the start of the full compositeKey (i.e. hlog.GetKey(recordLogicalAddresss)).
            ref CompositeKey<TPKey> storedCompositeKey = ref CompositeKey<TPKey>.CastFromFirstKeyPointerRefAsKeyRef(ref storedKeyPointerRefAsKeyRef);
            ref KeyPointer<TPKey> storedKeyPointer = ref this.GetKeyPointerRef(ref storedCompositeKey, queryKeyPointer.PredicateOrdinal);
            Debug.Assert(queryKeyPointer.PredicateOrdinal == storedKeyPointer.PredicateOrdinal, "Mismatched query and stored Predicate ordinal");
            return KeysEqual(ref queryKeyPointer, ref storedKeyPointer);
        }

        #endregion IFasterEqualityComparer implementation

        #region KeyPointer accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref KeyPointer<TPKey> keyPointer)
            => Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PredicateOrdinal + 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref CompositeKey<TPKey> key, int predOrdinal)
        {
            ref KeyPointer<TPKey> keyPointer = ref key.GetKeyPointerRef(predOrdinal, this.KeyPointerSize);
            return Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PredicateOrdinal + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool KeysEqual(ref KeyPointer<TPKey> queryKeyPointer, ref KeyPointer<TPKey> storedKeyPointer)
            => queryKeyPointer.PredicateOrdinal == storedKeyPointer.PredicateOrdinal && this.userComparer.Equals(ref queryKeyPointer.Key, ref storedKeyPointer.Key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPKey> GetKeyPointerRef(ref CompositeKey<TPKey> key, int predOrdinal)
            => ref key.GetKeyPointerRef(predOrdinal, this.KeyPointerSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPKey> GetKeyPointerRefFromKeyPointerLogicalAddress(long logicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPKey>>((byte*)this.hlog.GetPhysicalAddress(logicalAddress));
#endregion KeyPointer accessors

#region Address manipulation

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromKeyPointerAddress(long address)
            // This just manipulates the offset, so works for both logicalAddress and physicalAddress.
            => address - KeyPointer<TPKey>.CastFromPhysicalAddress(address).OffsetToStartOfKeys - RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromValueRef(ref TRecordId valueRef)
            // This assumes we are using VarLenBlittableAllocator, because it's called by Functions.SingleReader out of InternalCompletePendingRead,
            // and VarLenBlittableAllocator allocates a context record of the same layout as in the log.
            => (long)(byte*)Unsafe.AsPointer(ref valueRef) - this.TotalKeySize - RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPKey> GetKeyPointerRefFromRecordPhysicalAddress(long recordPhysicalAddress, int predOrdinal)
            // Note: assumes null TPKeys are in the key list.
            => ref Unsafe.AsRef<KeyPointer<TPKey>>((byte*)(recordPhysicalAddress + RecordInfo.GetLength() + this.KeyPointerSize * predOrdinal));

        #endregion Address manipulation

        internal string GetString(ref CompositeKey<TPKey> compositeKey, int predOrdinal = -1)
        {
            if (predOrdinal == -1)
            {
                var sb = new StringBuilder("{");
                for (var ii = 0; ii < this.KeyCount; ++ii)
                {
                    if (ii > 0)
                        sb.Append(", ");
                    ref KeyPointer<TPKey> keyPointer = ref this.GetKeyPointerRef(ref compositeKey, ii);
                    sb.Append(keyPointer.IsNull ? "null" : keyPointer.Key.ToString());
                }
                sb.Append("}");
                return sb.ToString();
            }
            return this.GetString(ref this.GetKeyPointerRef(ref compositeKey, predOrdinal));
        }

        internal string GetString(ref KeyPointer<TPKey> keyPointer)
            => $"{{{(keyPointer.IsNull ? "null" : keyPointer.Key.ToString())}}}";
    }
}