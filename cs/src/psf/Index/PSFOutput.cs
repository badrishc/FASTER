// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using FASTER.core;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace PSF.Index
{
    /// <summary>
    /// Output from operations on the secondary FasterKV instance (stores PSF chains).
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the key returned from a <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
    /// <typeparam name="TRecordId">The type of the provider's record identifier</typeparam>
    public unsafe struct PSFOutput<TPSFKey, TRecordId>
        where TPSFKey : new()
        where TRecordId : new()
    {
        internal readonly KeyAccessor<TPSFKey> keyAccessor;

        internal TRecordId RecordId { get; set; }

        internal bool Tombstone { get; set; }

        internal long PreviousLogicalAddress { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PSFOutput(KeyAccessor<TPSFKey> keyAcc)
        {
            this.keyAccessor = keyAcc;
            this.RecordId = default;
            this.Tombstone = false;
            this.PreviousLogicalAddress = Constants.kInvalidAddress;
        }

#if false    // TODO
        public PSFOperationStatus Visit(int psfOrdinal, ref TPSFKey key,
                                        ref TRecordId value, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            this.RecordId = value;
            this.Tombstone = tombstone;
            ref CompositeKey<TPSFKey> compositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref key);
            ref KeyPointer<TPSFKey> keyPointer = ref this.keyAccessor.GetKeyPointerRef(ref compositeKey, psfOrdinal);
            Debug.Assert(keyPointer.PsfOrdinal == (ushort)psfOrdinal, "Visit found mismatched PSF ordinal");
            this.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus Visit(int psfOrdinal, long physicalAddress,
                                        ref TRecordId value, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            this.RecordId = value;
            this.Tombstone = tombstone;
            ref KeyPointer<TPSFKey> keyPointer = ref this.keyAccessor.GetKeyPointerRef(physicalAddress);
            Debug.Assert(keyPointer.PsfOrdinal == (ushort)psfOrdinal, "Visit found mismatched PSF ordinal");
            this.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }
#endif
    }
}
