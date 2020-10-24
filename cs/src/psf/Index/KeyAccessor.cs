// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace PSF.Index
{
    /// <summary>
    /// Provides access to the <see cref="CompositeKey{TPSFKey}"/> internals that are hidden behind
    /// the Key typeparam of the secondary FasterKV.
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the Key returned by a PSF function</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    internal unsafe class KeyAccessor<TPSFKey, TRecordId> : IFasterEqualityComparer<TPSFKey>
    {
        private readonly IFasterEqualityComparer<TPSFKey> userComparer;
        private AllocatorBase<TPSFKey, TRecordId> hlog;

        internal KeyAccessor(IFasterEqualityComparer<TPSFKey> userComparer, int keyCount, int keyPointerSize)
        {
            this.userComparer = userComparer;
            this.KeyCount = keyCount;
            this.KeyPointerSize = keyPointerSize;
        }

        internal void SetLog(AllocatorBase<TPSFKey, TRecordId> hlog)
        {
            // Some logic here assumes we are using VarLenBlittableAllocator.
            Debug.Assert(hlog is VariableLengthBlittableAllocator<TPSFKey, TRecordId>, "Expected allocator type not found");
            this.hlog = hlog;
        }

        public int KeyCount { get; }

        public int KeyPointerSize { get; }

        // Note: TotalKeySize, and similar logic here and in CompositeKey, assume we do not skip null TPSFKeys in the record.
        public int TotalKeySize => KeyCount * KeyPointerSize;

        #region IFasterEqualityComparer implementation

        public long GetHashCode64(ref TPSFKey queryKeyPointerRefAsKeyRef) 
            => this.GetHashCode64(ref KeyPointer<TPSFKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef));

        public bool Equals(ref TPSFKey queryKeyPointerRefAsKeyRef, ref TPSFKey storedKeyPointerRefAsKeyRef)
        {
            ref KeyPointer<TPSFKey> queryKeyPointer = ref KeyPointer<TPSFKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);

            // storedKeyPointerAsKeyRef is always a reference to the start of the full compositeKey (i.e. hlog.GetKey(recordLogicalAddresss)).
            ref CompositeKey<TPSFKey> storedCompositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref storedKeyPointerRefAsKeyRef);
            ref KeyPointer<TPSFKey> storedKeyPointer = ref this.GetKeyPointerRef(ref storedCompositeKey, queryKeyPointer.PsfOrdinal);
            Debug.Assert(queryKeyPointer.PsfOrdinal == storedKeyPointer.PsfOrdinal, "Mismatched query and stored PSF ordinal");
            return KeysEqual(ref queryKeyPointer, ref storedKeyPointer);
        }

        #endregion IFasterEqualityComparer implementation

        #region KeyPointer accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref KeyPointer<TPSFKey> keyPointer)
            => Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PsfOrdinal + 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref CompositeKey<TPSFKey> key, int psfOrdinal)
        {
            ref KeyPointer<TPSFKey> keyPointer = ref key.GetKeyPointerRef(psfOrdinal, this.KeyPointerSize);
            return Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PsfOrdinal + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool KeysEqual(ref KeyPointer<TPSFKey> queryKeyPointer, ref KeyPointer<TPSFKey> storedKeyPointer)
            => queryKeyPointer.PsfOrdinal == storedKeyPointer.PsfOrdinal && this.userComparer.Equals(ref queryKeyPointer.Key, ref storedKeyPointer.Key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPSFKey> GetKeyPointerRef(ref CompositeKey<TPSFKey> key, int psfOrdinal)
            => ref key.GetKeyPointerRef(psfOrdinal, this.KeyPointerSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPSFKey> GetKeyPointerRefFromKeyPointerLogicalAddress(long logicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)this.hlog.GetPhysicalAddress(logicalAddress));
#endregion KeyPointer accessors

#region Address manipulation

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromKeyPointerAddress(long address)
            // This just manipulates the offset, so works for both logicalAddress and physicalAddress.
            => address - KeyPointer<TPSFKey>.CastFromPhysicalAddress(address).OffsetToStartOfKeys - RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromValueRef(ref TRecordId valueRef)
            // This assumes we are using VarLenBlittableAllocator, because it's called by PSFFunctions.SingleReader out of InternalCompletePendingRead,
            // and VarLenBlittableAllocator allocates a context record of the same layout as in the log.
            => (long)(byte*)Unsafe.AsPointer(ref valueRef) - this.TotalKeySize - RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPSFKey> GetKeyPointerRefFromRecordPhysicalAddress(long recordPhysicalAddress, int psfOrdinal)
            // Note: assumes null TPSFKeys are in the key list.
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)(recordPhysicalAddress + RecordInfo.GetLength() + this.KeyPointerSize * psfOrdinal));

        #endregion Address manipulation

        internal string GetString(ref CompositeKey<TPSFKey> compositeKey, int psfOrdinal = -1)
        {
            if (psfOrdinal == -1)
            {
                var sb = new StringBuilder("{");
                for (var ii = 0; ii < this.KeyCount; ++ii)
                {
                    if (ii > 0)
                        sb.Append(", ");
                    ref KeyPointer<TPSFKey> keyPointer = ref this.GetKeyPointerRef(ref compositeKey, ii);
                    sb.Append(keyPointer.IsNull ? "null" : keyPointer.Key.ToString());
                }
                sb.Append("}");
                return sb.ToString();
            }
            return this.GetString(ref this.GetKeyPointerRef(ref compositeKey, psfOrdinal));
        }

        internal string GetString(ref KeyPointer<TPSFKey> keyPointer)
            => $"{{{(keyPointer.IsNull ? "null" : keyPointer.Key.ToString())}}}";
    }
}