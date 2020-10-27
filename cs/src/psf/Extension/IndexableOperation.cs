// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.PSF
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
