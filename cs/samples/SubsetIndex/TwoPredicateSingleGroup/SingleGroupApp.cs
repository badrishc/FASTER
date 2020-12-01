// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.indexes.SubsetIndex;
using SubsetIndexSampleCommon;

namespace SingleGroup
{
    class SingleGroupApp
    {
        static void Main()
        {
            using var store = new Store();
            store.RunInitialInserts();
            var catsOfAge = QueryPredicates(store);
            store.UpdateCats(catsOfAge);
            QueryPredicates(store);
            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static FasterKVProviderData<Key, Value>[] QueryPredicates(Store store)
        {
            Console.WriteLine();
            using var session = store.FasterKV.ForSI(new Functions()).NewSession<Functions>();

            FasterKVProviderData<Key, Value>[] results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Cat)).ToArray();
            Console.WriteLine($"{results.Length} cats retrieved");

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Dog)).ToArray();
            Console.WriteLine($"{results.Length} dogs retrieved");

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Cat),
                                                                    store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge),
                                                                    (ll, rr) => ll && rr).ToArray();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge} retrieved");
            var catsOfAge = results;

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Cat),
                                                                    store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge + Constants.CatAgeIncrement),
                                                                    (ll, rr) => ll && rr).ToArray();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved");

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Dog),
                                                                    store.CombinedAgePred, new AgeOrPetKey(Constants.DogAge), (ll, rr) => ll && rr).ToArray();
            Console.WriteLine($"{results.Length} dogs age {Constants.DogAge} retrieved");

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Dog),
                                                                    store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge), (ll, rr) => ll || rr).ToArray();
            Console.WriteLine($"{results.Length} dogs or any pet age {Constants.CatAge} retrieved");
            return catsOfAge;
        }
    }
}
