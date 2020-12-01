// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSampleCommon
{
    public struct Key
    {
        public int Id { get; set; }

        public Key(int id) => this.Id = id;

        public override string ToString() => this.Id.ToString();

        public class Comparer : IFasterEqualityComparer<Key>
        {
            public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.Id);

            public bool Equals(ref Key k1, ref Key k2) => k1.Id == k2.Id;
        }
    }
}
