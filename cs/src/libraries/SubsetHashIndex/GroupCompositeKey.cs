// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

namespace FASTER.libraries.SubsetHashIndex
{
    internal struct GroupCompositeKey : IDisposable
    {
        // This cannot be typed to a TPKey because there may be different TPKeys across groups.
        private SectorAlignedMemory KeyPointerMem;

        internal void Set(SectorAlignedMemory keyMem) => this.KeyPointerMem = keyMem;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref TCompositeOrIndividualKey CastToKeyRef<TCompositeOrIndividualKey>()
            => ref Unsafe.AsRef<TCompositeOrIndividualKey>(this.KeyPointerMem.GetValidPointer());

        public void Dispose() => this.KeyPointerMem?.Return();
    }

    internal struct GroupCompositeKeyPair : IDisposable
    {
        internal long GroupId;

        // If the Group found the RecordId in its IPUCache, we carry it here.
        internal long LogicalAddress;

        internal GroupCompositeKey Before;
        internal GroupCompositeKey After;

        internal GroupCompositeKeyPair(long id)
        {
            this.GroupId = id;
            this.LogicalAddress = core.Constants.kInvalidAddress;
            this.Before = default;
            this.After = default;
            this.HasChanges = false;
        }

        internal bool HasAddress => this.LogicalAddress != core.Constants.kInvalidAddress;

        internal ref TCompositeKey GetBeforeKey<TCompositeKey>() => ref this.Before.CastToKeyRef<TCompositeKey>();

        internal ref TCompositeKey GetAfterKey<TCompositeKey>() => ref this.After.CastToKeyRef<TCompositeKey>();

        internal bool HasChanges;

        public void Dispose()
        {
            this.Before.Dispose();
            this.After.Dispose();
        }
    }
}
