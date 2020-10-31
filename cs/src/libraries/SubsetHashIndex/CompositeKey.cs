// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// Wraps the set of TPKeys for a record in the secondary FasterKV instance.
    /// </summary>
    /// <typeparam name="TPKey"></typeparam>
    public unsafe struct CompositeKey<TPKey>
    {
        // This class is essentially a "reinterpret_cast<KeyPointer<TPKey>*>" implementation; there are no data members.

        /// <summary>
        /// Get a reference to the key for the Predicate identified by <paramref name="predOrdinal"/>.
        /// </summary>
        /// <param name="predOrdinal">The ordinal of the Predicate in its parent Group</param>
        /// <param name="keyPointerSize">Size of the KeyPointer{TPKey} struct</param>
        /// <returns>A reference to the key for the Predicate identified by predOrdinal.</returns>
        /// <remarks>TODOperf: if we omit IsNull keys, then this will have to walk to the key with predOrdinal.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPKey> GetKeyPointerRef(int predOrdinal, int keyPointerSize) 
            => ref Unsafe.AsRef<KeyPointer<TPKey>>((byte*)Unsafe.AsPointer(ref this) + keyPointerSize * predOrdinal);

        /// <summary>
        /// Returns a reference to the CompositeKey from a reference to the first <see cref="KeyPointer{TPKey}"/>
        /// </summary>
        /// <param name="firstKeyPointerRef">A reference to the first <see cref="KeyPointer{TPKey}"/>, typed as TPKey</param>
        /// <remarks>Used when converting the CompositeKey to/from the TPKey type for secondary FKV operations</remarks>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref CompositeKey<TPKey> CastFromFirstKeyPointerRefAsKeyRef(ref TPKey firstKeyPointerRef)
            => ref Unsafe.AsRef<CompositeKey<TPKey>>((byte*)Unsafe.AsPointer(ref firstKeyPointerRef));

        /// <summary>
        /// Converts this CompositeKey reference to a reference to the first <see cref="KeyPointer{TPKey}"/>, typed as TPKey.
        /// </summary>
        /// <remarks>Used when converting the CompositeKey to/from the TPKey type for secondary FKV operations</remarks>
        /// <returns>A reference to the first <see cref="KeyPointer{TPKey}"/>, typed as TPKey</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TPKey CastToFirstKeyPointerRefAsKeyRef()
            => ref Unsafe.AsRef<TPKey>((byte*)Unsafe.AsPointer(ref this));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearUpdateFlags(int predCount, int keyPointerSize)
        {
            for (var ii = 0; ii < predCount; ++ii)
                this.GetKeyPointerRef(ii, keyPointerSize).ClearUpdateFlags();
        }

        internal class VarLenLength : IVariableLengthStruct<TPKey>
        {
            private readonly int size;

            // Note: This assumes null TPKeys are part of the key list.
            internal VarLenLength(int keyPointerSize, int predCount) => this.size = keyPointerSize * predCount;

            public int GetInitialLength() => this.size;

            public int GetLength(ref TPKey _) => this.size;

            public unsafe void Serialize(ref TPKey source, void* destination)
                => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source))
            ;
        }

        /// <summary>
        /// This is the unused key comparer passed to the secondary FasterKV
        /// </summary>
        internal class UnusedKeyComparer : IFasterEqualityComparer<TPKey>
        {
            public long GetHashCode64(ref TPKey cKey)
                => throw new InternalErrorExceptionSHI("Must use KeyAccessor instead (predOrdinal is required)");

            public bool Equals(ref TPKey cKey1, ref TPKey cKey2)
                => throw new InternalErrorExceptionSHI("Must use KeyAccessor instead (predOrdinal is required)");
        }
    }
}
