// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.indexes.SubsetIndex;
using FASTER.libraries.SubsetIndex;
using SubsetIndexSampleCommon;

namespace SingleGroup
{
    internal class Store : StoreBase
    {
        internal IPredicate CombinedPetPred, CombinedAgePred;

        internal Store() : base(1, nameof(SingleGroup))
        {
            var preds = FasterKV.Register(CreateRegistrationSettings(0, new AgeOrPetKey.Comparer()),
                                         ("pet", (k, v) => new AgeOrPetKey(v.Species)),
                                         ("age", (k, v) => new AgeOrPetKey(v.Age)));
            this.CombinedPetPred = preds[0];
            this.CombinedAgePred = preds[1];
        }
    }
}
