// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.indexes.SubsetIndex;
using FASTER.libraries.SubsetIndex;
using SubsetIndexSampleCommon;

namespace MultiGroupAsync
{
    internal class Store : StoreBase
    {
        internal IPredicate PetPred, AgePred;

        internal Store() : base(2, nameof(MultiGroupAsync))
        {
            this.PetPred = FasterKV.Register(CreateRegistrationSettings(0, new PetKey.Comparer()),
                                           "pet", (k, v) => new PetKey(v.Species));
            this.AgePred = FasterKV.Register(CreateRegistrationSettings(1, new AgeKey.Comparer()),
                                           "age", (k, v) => new AgeKey(v.Age));
        }
    }
}
