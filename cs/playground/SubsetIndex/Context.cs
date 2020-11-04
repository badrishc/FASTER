// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Collections.Generic;

namespace SubsetIndexSample
{
    public class Context<TValue>
    {
        public List<(Status status, Key key, TValue value)> PendingResults { get; set; } = new List<(Status, Key, TValue)>();
    }
}
