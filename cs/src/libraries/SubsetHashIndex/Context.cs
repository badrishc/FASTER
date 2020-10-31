// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.libraries.SubsetHashIndex
{
    internal partial class FasterKVSHI<TPKey, TRecordId> : FasterKV<TPKey, TRecordId>
    {
        /// <summary>
        /// Context for operations on the secondary FasterKV instance.
        /// </summary>
        internal class Context
        {
            internal Functions Functions;
        }
    }
}
