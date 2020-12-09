// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.indexes.SubsetIndex;
using FASTER.libraries.SubsetIndex;
using SubsetIndexSampleCommon;

namespace BasicPredicateSample
{
    internal class Store : StoreBase
    {
        internal IPredicate PetPred;

        internal Store() : base(1, nameof(BasicPredicateSample))
        {
            this.PetPred = FasterKV.Register(CreateRegistrationSettings(0, new IntKeyComparer()), nameof(this.PetPred), (k, v) => v.SpeciesInt);
        }
    }
}
