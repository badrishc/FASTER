// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSample
{
    public struct CountBinKey
    {
        internal const int BinSize = 100;
        internal const int MaxOrders = BinSize * 10;
        internal const int LastBin = 9; // 0-based

        public int Bin;

        public CountBinKey(int bin) => this.Bin = bin;

        internal static int GetBin(int numOrders) => numOrders / BinSize;

        internal static bool GetAndVerifyBin(int numOrders, out int bin)
        {
            // Skip the last bin during initial inserts to illustrate not matching the Predicate (returning null)
            bin = GetBin(numOrders);
            return bin < LastBin;
        }

        public class Comparer : IFasterEqualityComparer<CountBinKey>
        {
            // Make the hashcode for this distinct from size enum values
            public long GetHashCode64(ref CountBinKey key) => Utility.GetHashCode(key.Bin + 1000);

            public bool Equals(ref CountBinKey k1, ref CountBinKey k2) => k1.Bin == k2.Bin;
        }
    }
}
