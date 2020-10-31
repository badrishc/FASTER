// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetHashIndexSample
{
    public struct Key
    {
        // Note: int instead of long because we won't use enough values to overflow and having it a 
        // different length than TRecordId (which is long) makes sure the Value offsetting is in sync.
        public int Id { get; set; }

        public Key(int id) => this.Id = id;

        public (int, int, int, int) MemberTuple => SubsetHashIndexSampleApp.keyDict.TryGetValue(this, out var orders) ? orders.MemberTuple : (-1, -2, -3, -4);

        public override string ToString() => this.MemberTuple.ToString();

        public class Comparer : IFasterEqualityComparer<Key>
        {
            public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.Id);

            public bool Equals(ref Key k1, ref Key k2) => k1.Id == k2.Id;
        }
    }
}
