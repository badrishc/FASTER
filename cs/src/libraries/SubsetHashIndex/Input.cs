// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.libraries.SubsetHashIndex
{
    internal unsafe partial class FasterKVSHI<TPKey, TRecordId> : FasterKV<TPKey, TRecordId>
    {
        internal interface IInputAccessor<TInput>
        {
            long GroupId { get; }

            bool IsDelete(ref TInput input);
            bool SetDelete(ref TInput input, bool value);
        }

        /// <summary>
        /// Input to Read operations on the secondary FasterKV instance
        /// </summary>
        internal unsafe struct Input : IDisposable
        {
            private SectorAlignedMemory keyPointerMem;

            internal Input(long groupId, int predOrdinal)
            {
                this.keyPointerMem = null;
                this.GroupId = groupId;
                this.PredicateOrdinal = predOrdinal;
                this.IsDelete = false;
            }

            internal void SetQueryKey(SectorAlignedBufferPool pool, KeyAccessor<TPKey, TRecordId> keyAccessor, ref TPKey key)
            {
                // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to Query.
                this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyPointerMem.GetValidPointer());
                keyPointer.Initialize(this.PredicateOrdinal, ref key, keyAccessor.KeyPointerSize);
            }

            /// <summary>
            /// The ID of the <see cref="Group{TProviderData, TPKey, TRecordId}"/> for this operation.
            /// </summary>
            public long GroupId { get; }

            /// <summary>
            /// The ordinal of the <see cref="Predicate{TPKey, TRecordId}"/> in the group <see cref="GroupId"/>for this operation.
            /// </summary>
            public int PredicateOrdinal { get; set; }

            /// <summary>
            /// Whether this is a Delete (or the Delete part of an RCU)
            /// </summary>
            public bool IsDelete { get; set; }

            /// <summary>
            /// The query key for a Query method
            /// </summary>
            public ref TPKey QueryKeyRef => ref Unsafe.AsRef<TPKey>(this.keyPointerMem.GetValidPointer());

            /// <summary>
            /// The query key for a Query method
            /// </summary>
            public ref KeyPointer<TPKey> QueryKeyPointerRef => ref Unsafe.AsRef<KeyPointer<TPKey>>(this.keyPointerMem.GetValidPointer());

            public void Dispose()
            {
                if (this.keyPointerMem is {})
                {
                    this.keyPointerMem.Return();
                    this.keyPointerMem = null;
                }
            }

            public override string ToString() 
                => $"qKeyPtr {this.QueryKeyPointerRef}, groupId {this.GroupId}, predOrd {this.PredicateOrdinal}, isDel {this.IsDelete}";
        }
    }
}