// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSampleCommon
{
    public struct AgeOrPetKey
    {
        // Colors, strings, bools, and enums are not blittable so we use int
        public int KeyData;
        public int IsPetInt;

        public AgeOrPetKey(Species species)
        {
            this.KeyData = (int)species;
            this.IsPetInt = 1;
        }

        public AgeOrPetKey(int age)
        {
            this.KeyData = age;
            this.IsPetInt = 0;
        }

        public bool IsPet => this.IsPetInt != 0;

        public override string ToString()
        {
            return this.IsPet switch
            {
                true => $"Pet: {(Species)this.KeyData}",
                false => $"Age: {this.KeyData}"
            };
        }

        public class Comparer : IFasterEqualityComparer<AgeOrPetKey>
        {
            public long GetHashCode64(ref AgeOrPetKey key) => Utility.GetHashCode((long)(key.IsPet ? 1 : 2) << 32 | (uint)key.KeyData);

            public bool Equals(ref AgeOrPetKey k1, ref AgeOrPetKey k2) => k1.IsPet == k2.IsPet && k1.KeyData == k2.KeyData;
        }
    }
}
