// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.indexes.SubsetHashIndex
{
    internal enum IndexableOperation
    {
        None,
        Read,
        RMW,
        Upsert,
        Delete,
        CompletePending
    }
}
