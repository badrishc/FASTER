// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace PSF.Index
{
    internal unsafe partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        internal interface IInputAccessor<TInput>
        {
            long GroupId { get; }

            bool IsDelete(ref TInput input);
            bool SetDelete(ref TInput input, bool value);
        }

        /// <summary>
        /// Input to PsfRead operations on the secondary FasterKV instance
        /// </summary>
        internal unsafe struct PSFInput : IDisposable
        {
            private SectorAlignedMemory keyPointerMem;

            internal PSFInput(long groupId, int psfOrdinal)
            {
                this.keyPointerMem = null;
                this.GroupId = groupId;
                this.PsfOrdinal = psfOrdinal;
                this.IsDelete = false;
            }

            internal void SetQueryKey(SectorAlignedBufferPool pool, KeyAccessor<TPSFKey, TRecordId> keyAccessor, ref TPSFKey key)
            {
                // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to QueryPSF.
                this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
                ref KeyPointer<TPSFKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPSFKey>>(keyPointerMem.GetValidPointer());
                keyPointer.Initialize(this.PsfOrdinal, ref key, keyAccessor.KeyPointerSize);
            }

            /// <summary>
            /// The ID of the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> for this operation.
            /// </summary>
            public long GroupId { get; }

            /// <summary>
            /// The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> in the group <see cref="GroupId"/>for this operation.
            /// </summary>
            public int PsfOrdinal { get; set; }

            /// <summary>
            /// Whether this is a Delete (or the Delete part of an RCU)
            /// </summary>
            public bool IsDelete { get; set; }

            /// <summary>
            /// The query key for a QueryPSF method
            /// </summary>
            public ref TPSFKey QueryKeyRef => ref Unsafe.AsRef<TPSFKey>(this.keyPointerMem.GetValidPointer());

            /// <summary>
            /// The query key for a QueryPSF method
            /// </summary>
            public ref KeyPointer<TPSFKey> QueryKeyPointerRef => ref Unsafe.AsRef<KeyPointer<TPSFKey>>(this.keyPointerMem.GetValidPointer());

            public void Dispose()
            {
                if (this.keyPointerMem is {})
                {
                    this.keyPointerMem.Return();
                    this.keyPointerMem = null;
                }
            }

            public override string ToString() 
                => $"qKeyPtr {this.QueryKeyPointerRef}, groupId {this.GroupId}, psfOrd {this.PsfOrdinal}, isDel {this.IsDelete}";
        }
    }
}