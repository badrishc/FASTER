// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.indexes.SubsetIndex;
using SubsetIndexSampleCommon;

namespace BasicPredicateSample
{
    class BasicPredicateApp
    {
        private static Store store;

        static void Main()
        {
            store = new Store();
            store.RunInitialInserts();
            QueryPredicate();
            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static void QueryPredicate()
        {
            using var session = store.FasterKV.ForSI(new Functions()).NewSession<Functions>();

            FasterKVProviderData<Key, Value>[] results = session.Query(store.PetPred, (int)Species.Cat).ToArray();
            Console.WriteLine($"{results.Length} cats retrieved");

            results = session.Query(store.PetPred, (int)Species.Dog).ToArray();
            Console.WriteLine($"{results.Length} dogs retrieved");
        }
    }
}
