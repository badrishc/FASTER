// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
namespace PSF.Index
{
    internal partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        /// <summary>
        /// Context for operations on the secondary FasterKV instance.
        /// </summary>
        internal class PSFContext
        {
            internal PSFFunctions Functions;

            internal PSFOutput Output;
        }
    }
}
