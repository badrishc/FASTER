// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using FASTER.core;

namespace PSF.Index
{
    internal unsafe partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        /// <summary>
        /// Output from Reads on the secondary FasterKV instance (stores PSF chains).
        /// </summary>
        internal struct PSFOutput
        {
            public TRecordId RecordId { get; set; }

            public bool Tombstone { get; set; }

            public long PreviousLogicalAddress { get; set; }
        }
    }
}
