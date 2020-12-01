// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace SubsetIndexSampleCommon
{
    public enum Species { Cat = 1, Dog };

    public struct PetKey
    {
        // Colors, strings, and enums are not blittable so we use int
        public int SpeciesInt;

        public PetKey(Species species) => this.SpeciesInt = (int)species;

        public Species Species => (Species)this.SpeciesInt;

        public override string ToString() => this.Species.ToString();

        public class Comparer : IFasterEqualityComparer<PetKey>
        {
            public long GetHashCode64(ref PetKey key) => Utility.GetHashCode(key.SpeciesInt);

            public bool Equals(ref PetKey k1, ref PetKey k2) => k1.SpeciesInt == k2.SpeciesInt;
        }
    }
}
