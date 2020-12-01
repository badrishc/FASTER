// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices.ComTypes;

namespace SubsetIndexSampleCommon
{
    public struct Value
    {
        // Colors, strings, and enums are not blittable so we use int
        public int Id;
        public int SpeciesInt;
        public int Age;

        public Value(int id, Species species, int age)
        {
            this.Id = id;
            this.SpeciesInt = (int)species;
            this.Age = age;
        }

        public Species Species => (Species)this.SpeciesInt;
    }
}
