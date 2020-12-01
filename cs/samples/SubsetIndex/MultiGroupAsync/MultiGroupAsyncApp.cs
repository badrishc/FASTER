// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using FASTER.indexes.SubsetIndex;
using SubsetIndexSampleCommon;

namespace MultiGroupAsync
{
    class MultiGroupAsyncApp
    {
        static async ValueTask Main()
        {
            using var store = new Store();
            store.RunInitialInserts();
            var catsOfAge = await QueryPredicatesAsync(store);
            store.UpdateCats(catsOfAge);
            await QueryPredicatesAsync(store);
            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static async ValueTask<FasterKVProviderData<Key, Value>[]> QueryPredicatesAsync(Store store)
        {
            Console.WriteLine();
            using var session = store.FasterKV.ForSI(new Functions()).NewSession<Functions>();

            FasterKVProviderData<Key, Value>[] results = await session.QueryAsync(store.PetPred, new PetKey(Species.Cat)).ToArrayAsync();
            Console.WriteLine($"{results.Length} cats retrieved");

            results = await session.QueryAsync(store.PetPred, new PetKey(Species.Dog)).ToArrayAsync();
            Console.WriteLine($"{results.Length} dogs retrieved");

            results = await session.QueryAsync(store.PetPred, new PetKey(Species.Cat),
                                                                    store.AgePred, new AgeKey(Constants.CatAge),
                                                                    (ll, rr) => ll && rr).ToArrayAsync();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge} retrieved");
            var catsOfAge = results;

            results = await session.QueryAsync(store.PetPred, new PetKey(Species.Cat),
                                                                    store.AgePred, new AgeKey(Constants.CatAge + Constants.CatAgeIncrement),
                                                                    (ll, rr) => ll && rr).ToArrayAsync();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved");

            results = await session.QueryAsync(store.PetPred, new PetKey(Species.Dog),
                                                                    store.AgePred, new AgeKey(Constants.DogAge), (ll, rr) => ll && rr).ToArrayAsync();
            Console.WriteLine($"{results.Length} dogs age {Constants.DogAge} retrieved");

            results = await session.QueryAsync(store.PetPred, new PetKey(Species.Dog),
                                                                    store.AgePred, new AgeKey(Constants.CatAge), (ll, rr) => ll || rr).ToArrayAsync();
            Console.WriteLine($"{results.Length} dogs or any pet age {Constants.CatAge} retrieved");
            return catsOfAge;
        }
    }
}
