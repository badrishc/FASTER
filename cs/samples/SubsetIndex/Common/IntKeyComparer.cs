// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSampleCommon
{
    public class IntKeyComparer : IFasterEqualityComparer<int>
    {
        public long GetHashCode64(ref int key) => Utility.GetHashCode(key);

        public bool Equals(ref int k1, ref int k2) => k1 == k2;
    }
}
