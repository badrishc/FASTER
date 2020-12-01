// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSampleCommon
{
    public struct AgeKey
    {
        public int Age { get; set; }

        public AgeKey(int age) => this.Age = age;

        public override string ToString() => this.Age.ToString();

        public class Comparer : IFasterEqualityComparer<AgeKey>
        {
            public long GetHashCode64(ref AgeKey key) => Utility.GetHashCode(key.Age);

            public bool Equals(ref AgeKey k1, ref AgeKey k2) => k1.Age == k2.Age;
        }
    }
}
