// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
            internal TRecordId RecordId;

            internal long PreviousAddress;

            internal bool IsDeleted;

            // Used only for ReadCompletionCallback.
            internal Status PendingResultStatus;

            public override string ToString() => $"rId {this.RecordId}, prevAddr {this.PreviousAddress}";
        }
    }
}
