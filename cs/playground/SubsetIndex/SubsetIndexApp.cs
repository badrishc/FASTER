// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.SubsetIndex;
using FASTER.libraries.SubsetIndex;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SubsetIndexSample
{
    public partial class SubsetIndexApp
    {
        private static int blueCount, mediumCount, bin7Count, redCount;
        private static int intersectMediumBlueCount, unionMediumBlueCount;
        private static int intersectMediumBlue7Count, unionMediumBlue7Count;
        private static int unionMediumLargeCount, unionRedBlueCount;
        private static int unionMediumLargeRedBlueCount, intersectMediumLargeRedBlueCount;
        private static int xxlDeletedCount;
        
        internal static Dictionary<Key, IOrders> keyDict = new Dictionary<Key, IOrders>();

        private static int nextId = 1000000000;

        static async Task Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (useObjectValues)  // TODO add VarLenValue
                await RunSample<ObjectOrders, Input<ObjectOrders>, Output<ObjectOrders>, ObjectOrders.Functions, ObjectOrders.Serializer>();
            else
                await RunSample<BlittableOrders, Input<BlittableOrders>, Output<BlittableOrders>, BlittableOrders.Functions, NoSerializer>();
            return;
        }

        internal async static Task RunSample<TValue, TInput, TOutput, TFunctions, TSerializer>()
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var store = new Store<TValue, TInput, TOutput, TFunctions, TSerializer>();
            try
            {
                var sw = new Stopwatch();
                sw.Start();
                await RunInitialInserts(store);
                await RunReads(store);
                var ok = await QuerysWithoutBoolOps(store);
                ok &= await QuerysWithBoolOps(store);
                ok &= UpdateSizeByUpsert(store);
                ok &= await UpdateColorByRMW(store);
                ok &= UpdateCountByUpsert(store);
                ok &= await Delete(store);
                ok &= await QuerysForFinalVerification(store);
                sw.Stop();

                var pendingReadsString = (useReadCache, copyReadsToTail) switch {
                    (true, false) => "Cache",
                    (false, true) => "Tail",
                    _ => "<none>"
                };
                Console.WriteLine("--------------------------------------------------------");
                Console.WriteLine($"===--- Completed run: Async {useAsync}, MultiGroup {useMultiGroups}, ObjValues {useObjectValues}, PendingReadCopy {pendingReadsString}, Flush {flushAndEvict}, time = {Trim(sw.Elapsed)}");
                Console.WriteLine();
                Console.Write("===>>> ");
                Console.WriteLine(ok ? "Passed! All operations succeeded" : "*** Failed! *** One or more operations failed");
                Console.WriteLine();
                if (Debugger.IsAttached)
                {
                    Console.Write("Press ENTER to close this window . . .");
                    Console.ReadLine();
                }
            }
            finally
            {
                store.Close();
            }

            //Console.WriteLine("Press <ENTER> to end");
            //Console.ReadLine();
        }

        internal async static Task RunInitialInserts<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine($"Writing keys from 0 to {keyCount:N0} to FASTER");

            var rng = new Random(13);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();
            var context = new Context<TValue>();
            var input = default(TInput);
            int statusPending = 0;

            var sw = new Stopwatch();
            sw.Start();
            for (int ii = 0; ii < keyCount; ++ii)
            {
                // Leave the last value unassigned from each category (we'll use it to update later)
                var key = new Key(Interlocked.Increment(ref nextId) - 1);
                var value = new TValue
                {
                    Id = key.Id,
                    SizeInt = rng.Next((int)Constants.Size.NumSizes - 1),
                    ColorArgb = Constants.Colors[rng.Next(Constants.Colors.Length - 1)].ToArgb(),
                    Count = rng.Next(CountBinKey.MaxOrders - 1)
                };

                keyDict[key] = value;
                CountBinKey.GetAndVerifyBin(value.Count, out int bin);
                var isBlue = value.ColorArgb == Color.Blue.ToArgb();
                var isRed = value.ColorArgb == Color.Red.ToArgb();
                var isMedium = value.SizeInt == (int)Constants.Size.Medium;
                var isLarge = value.SizeInt == (int)Constants.Size.Large;
                var isBin7 = bin == 7;

                if (isBlue)
                {
                    ++blueCount;
                    if (isMedium)
                    {
                        ++intersectMediumBlueCount;
                        if (isBin7)
                            ++intersectMediumBlue7Count;
                    }
                }
                if (isMedium)
                    ++mediumCount;
                if (isRed)
                    ++redCount;

                if (isBin7) 
                    ++bin7Count;

                if (isMedium || isBlue)
                    ++unionMediumBlueCount;
                if (isMedium || isBlue || isBin7)
                    ++unionMediumBlue7Count;

                var isMediumOrLarge = isMedium || isLarge;
                var isRedOrBlue = isBlue || isRed;

                if (isMediumOrLarge)
                {
                    ++unionMediumLargeCount;
                    if (isRedOrBlue)
                        ++intersectMediumLargeRedBlueCount;
                }
                if (isRedOrBlue)
                    ++unionRedBlueCount;
                if (isMediumOrLarge || isRedOrBlue)
                    ++unionMediumLargeRedBlueCount;

                // Both Upsert and RMW do an insert when the key is not found.
                var status = Status.OK;
                if ((ii & 1) == 0)
                {
                    // Note: there is no UpsertAsync().
                    status = session.Upsert(ref key, ref value, context);
                }
                else
                {
                    input.InitialUpdateValue = value;
                    if (useAsync)
                        (await session.RMWAsync(ref key, ref input, context)).Complete();
                    else
                        status = session.RMW(ref key, ref input, context);
                }
                if (status == Status.PENDING)
                {
                    ++statusPending;
                }
            }

            if (!useAsync)
                session.CompletePending();

            sw.Stop();
            Console.WriteLine($"Inserted {keyCount:N0} elements; {statusPending:N0} pending, time = {Trim(sw.Elapsed)}");
        }

        internal async static Task RunReads<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var readCount = keyCount * 2;
            Console.WriteLine($"Reading {readCount:N0} random keys from FASTER");

            FlushAndEvictIfRequested(store);
            // Alternative to .For to obtain a session, but uses the interface so is slightly slower.
            using var session = store.FasterKV.NewSessionForSI(new TFunctions());

            var sw = new Stopwatch();
            sw.Start();

            var rng = new Random(0);
            int statusPending = 0;
            var output = new TOutput();
            var input = default(TInput);
            var context = new Context<TValue>();

            var keys = keyDict.Keys.ToArray();

            for (int ii = 0; ii < readCount; ++ii)
            {
                var key = keys[rng.Next(keys.Length)];
                var status = Status.OK;
                if (useAsync)
                {
                    (status, output) = (await session.ReadAsync(ref key, ref input, context)).Complete();
                }
                else
                {
                    status = session.Read(ref key, ref input, ref output, context);
                    if (status == Status.PENDING)
                        ++statusPending;
                }

                if (status == Status.OK && output.Value.MemberTuple != key.MemberTuple)
                    throw new SampleException($"Error: Value does not match key in {nameof(RunReads)}");
            }

            session.CompletePending(true);

            foreach (var (status, key, value) in context.PendingResults)
            {
                if (status == Status.OK) {
                    if (value.MemberTuple != key.MemberTuple)
                        throw new SampleException($"Error: Value does not match key in {nameof(RunReads)} pending results");
                } else
                {
                    throw new SampleException($"Error: Unexpected status {status} in {nameof(RunReads)} pending results");
                }
            }

            sw.Stop();
            Console.WriteLine($"Read {readCount:N0} random elements with {statusPending:N0} Pending, time = {Trim(sw.Elapsed)}");
        }

        const string indent2 = "  ";
        const string indent4 = "    ";

        static bool VerifyProviderDatas<TValue>(FasterKVProviderData<Key, TValue>[] providerDatas, string name, int expectedCount, ref long totalCount)
            where TValue : IOrders, new()
        {
            Console.Write($"{indent4}{name}: ");
            if (verbose)
            {
                foreach (var providerData in providerDatas)
                {
                    ref TValue value = ref providerData.GetValue();
                    Console.WriteLine(indent4 + value);
                }
            }
            Console.WriteLine(providerDatas.Length == expectedCount
                              ? $"Passed: expected == actual ({expectedCount:N0})"
                              : $"Failed: expected ({expectedCount:N0}) != actual ({providerDatas.Length:N0})");
            totalCount += providerDatas.Length;
            return expectedCount == providerDatas.Length;
        }

        private static void FlushAndEvictIfRequested<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            if (flushAndEvict)
                store.FasterKV.FlushAndEvict(wait: true);
        }

        private static string PassOrFail(bool ok) => ok ? "passed" : ">>> FAILED! <<<";

        private static string Trim(TimeSpan ts) => ts.ToString(@"hh\:mm\:ss\.fff");

        private static bool WriteStatus(string activity, bool ok, long count, Stopwatch sw, bool isUpdate)
        {
            var opString = isUpdate ? "updated" : "read";
            Console.WriteLine($"{indent4}{activity} {PassOrFail(ok)}, {count:N0} records {opString}, time = {Trim(sw.Elapsed)}");
            return ok;
        }

        internal async static Task<bool> QuerysWithoutBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Querying with no boolean ops";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            var sw = new Stopwatch();
            sw.Start();

            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;
            long count = 0;

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery<TPKey>(IPredicate pred, TPKey key) where TPKey : struct
                => useAsync
                    ? await session.QueryAsync(pred, key).ToArrayAsync()
                    : session.Query(pred, key).ToArray();

            providerDatas = useMultiGroups
                                ? await RunQuery(store.SizePred, new SizeKey(Constants.Size.Medium))
                                : await RunQuery(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium));
            ok &= VerifyProviderDatas(providerDatas, "Medium", mediumCount, ref count);
            
            providerDatas = useMultiGroups
                                ? await RunQuery(store.ColorPred, new ColorKey(Color.Blue))
                                : await RunQuery(store.CombinedColorPred, new CombinedKey(Color.Blue));
            ok &= VerifyProviderDatas(providerDatas, "Blue", blueCount, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.ColorPred, new ColorKey(Color.Red))
                                : await RunQuery(store.CombinedColorPred, new CombinedKey(Color.Red));
            ok &= VerifyProviderDatas(providerDatas, "Red", redCount, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.CountBinPred, new CountBinKey(7))
                                : await RunQuery(store.CombinedCountBinPred, new CombinedKey(7));
            ok &= VerifyProviderDatas(providerDatas, "Bin7", bin7Count, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.CountBinPred, new CountBinKey(CountBinKey.LastBin))
                                : await RunQuery(store.CombinedCountBinPred, new CombinedKey(CountBinKey.LastBin));
            ok &= VerifyProviderDatas(providerDatas, "LastBin", 0, ref count);  // Insert skipped (returned null from the Predicate) all that fall into the last bin

            sw.Stop();
            return WriteStatus(activity, ok, count, sw, isUpdate: false);
        }

        internal async static Task<bool> QuerysWithBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Querying with boolean ops";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            var sw = new Stopwatch();
            sw.Start();

            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;
            long count = 0;

            // Local functions can't be overloaded so make the name unique
            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery2<TPKey1, TPKey2>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate)
            where TPKey1 : struct
            where TPKey2 : struct
                => useAsync
                    ? await session.QueryAsync(pred1, key1, pred2, key2, matchPredicate).ToArrayAsync()
                    : session.Query(pred1, key1, pred2, key2, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery3<TPKey1, TPKey2, TPKey3>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
                => useAsync
                    ? await session.QueryAsync(pred1, key1, pred2, key2, pred3, key3, matchPredicate).ToArrayAsync()
                    : session.Query(pred1, key1, pred2, key2, pred3, key3, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery1EnumKeys<TPKey>(
                IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings = null)
            where TPKey : struct
                => useAsync
                    ? await session.QueryAsync(pred, keys).ToArrayAsync()
                    : session.Query(pred, keys).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery2Vec<TPKey1, TPKey2>(
                    IPredicate pred1, IEnumerable<TPKey1> keys1,
                    IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate)
            where TPKey1 : struct
            where TPKey2 : struct
                => useAsync
                    ? await session.QueryAsync(pred1, keys1, pred2, keys2, matchPredicate).ToArrayAsync()
                    : session.Query(pred1, keys1, pred2, keys2, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery1EnumTuple<TPKey>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predsAndKeys,
                    Func<bool[], bool> matchPredicate)
            where TPKey : struct
                => useAsync
                    ? await session.QueryAsync(predsAndKeys, matchPredicate).ToArrayAsync()
                    : session.Query(predsAndKeys, matchPredicate).ToArray();

            if (useMultiGroups)
            {
                providerDatas = await RunQuery2(store.SizePred, new SizeKey(Constants.Size.Medium),
                                               store.ColorPred, new ColorKey(Color.Blue), (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount, ref count);
                providerDatas = await RunQuery2(store.SizePred, new SizeKey(Constants.Size.Medium),
                                               store.ColorPred, new ColorKey(Color.Blue), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount, ref count);

                providerDatas = await RunQuery3(store.SizePred, new SizeKey(Constants.Size.Medium),
                                                store.ColorPred, new ColorKey(Color.Blue),
                                                store.CountBinPred, new CountBinKey(7), (sz, cl, ct) => sz && cl && ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count, ref count);
                providerDatas = await RunQuery3(store.SizePred, new SizeKey(Constants.Size.Medium),
                                                store.ColorPred, new ColorKey(Color.Blue),
                                                store.CountBinPred, new CountBinKey(7), (sz, cl, ct) => sz || cl || ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count, ref count);

                providerDatas = await RunQuery1EnumKeys(store.SizePred, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount, ref count);
                providerDatas = await RunQuery1EnumKeys(store.ColorPred, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount, ref count);

                providerDatas = await RunQuery2Vec(store.SizePred, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 store.ColorPred, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount, ref count);
                providerDatas = await RunQuery2Vec(store.SizePred, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 store.ColorPred, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount, ref count);
            }
            else
            {
                // Queries here are done twice, to illustrate the different ways of querying within a single group.
                providerDatas = await RunQuery2(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium),
                                               store.CombinedColorPred, new CombinedKey(Color.Blue), (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] { (store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] && sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount, ref count);
                // ---
                providerDatas = await RunQuery2(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium),
                                               store.CombinedColorPred, new CombinedKey(Color.Blue), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] { (store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] || sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount, ref count);
                // ---
                providerDatas = await RunQuery3(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium),
                                                store.CombinedColorPred, new CombinedKey(Color.Blue),
                                                store.CombinedCountBinPred, new CombinedKey(7), (sz, cl, ct) => sz && cl && ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] {(store.CombinedSizePred, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (store.CombinedCountBinPred, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] && sz[1] && sz[2]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count, ref count);
                // ---
                providerDatas = await RunQuery3(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium),
                                                store.CombinedColorPred, new CombinedKey(Color.Blue),
                                                store.CombinedCountBinPred, new CombinedKey(7), (sz, cl, ct) => sz || cl || ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] {(store.CombinedSizePred, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (store.CombinedCountBinPred, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] || sz[1] || sz[2]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count, ref count);
                // ---
                providerDatas = await RunQuery2(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium),
                                               store.CombinedSizePred, new CombinedKey(Constants.Size.Large), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount, ref count);
                providerDatas = await RunQuery1EnumKeys(store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount, ref count);
                providerDatas = await RunQuery2(store.CombinedColorPred, new CombinedKey(Color.Blue),
                                               store.CombinedColorPred, new CombinedKey(Color.Red), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount, ref count);
                providerDatas = await RunQuery1EnumKeys(store.CombinedColorPred, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount, ref count);
                // ---
                providerDatas = await RunQuery2Vec(store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 store.CombinedColorPred, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] {(store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] && sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount, ref count);
                // ---
                providerDatas = await RunQuery2Vec(store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 store.CombinedColorPred, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount, ref count);
                providerDatas = await RunQuery1EnumTuple(new[] {(store.CombinedSizePred, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (store.CombinedColorPred, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] || sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount, ref count);
            }

            sw.Stop();
            return WriteStatus(activity, ok, count, sw, isUpdate: false);
        }

        private static bool WriteUpdateResult(bool isInitial, string name, int expectedCount, int actualCount, bool allResultsMatch, bool isDelete = false)
        {
            var tag = isInitial ? "Initial" : (isDelete ? "Deleted" : "Updated");
            if (expectedCount == actualCount && allResultsMatch) { 
                Console.WriteLine($"{indent4}{tag} {name} Passed: expected == actual ({expectedCount:N0})");
                return true;
            }
            Console.WriteLine(expectedCount == actualCount
                                ? $"{indent4}{tag} {name} Failed: expected == actual ({expectedCount:N0}), but not all results matched"
                                : $"{indent4}{tag} {name} Failed: expected ({expectedCount:N0}) != actual ({actualCount:N0})");
            return false;
        }

        private static bool AllMatch<TValue>(FasterKVProviderData<Key, TValue>[] datas, Func<TValue, bool> pred) => datas.All(data => pred(data.GetValue()));

        internal static bool UpdateSizeByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Updating Sizes via Upsert";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            FasterKVProviderData<Key, TValue>[] GetSizeDatas(Constants.Size size)
                => useMultiGroups
                    ? session.Query(store.SizePred, new SizeKey(size)).ToArray()
                    : session.Query(store.CombinedSizePred, new CombinedKey(size)).ToArray();

            var xxlDatas = GetSizeDatas(Constants.Size.XXLarge);
            var ok = WriteUpdateResult(isInitial: true, "XXLarge", 0, xxlDatas.Length, true);
            var mediumDatas = GetSizeDatas(Constants.Size.Medium);
            ok &= WriteUpdateResult(isInitial: true, "Medium", mediumCount, mediumDatas.Length, AllMatch(mediumDatas, val => val.SizeInt == (int)Constants.Size.Medium));

            var expected = mediumDatas.Length;
            Console.WriteLine($"{indent2}Changing all Medium to XXLarge");

            var context = new Context<TValue>();

            var sw = new Stopwatch();
            sw.Start();

            foreach (var providerData in mediumDatas)
            {
                // Get the old value and confirm it's as expected. We cannot have ref locals because this is an async function.
                Debug.Assert(providerData.GetValue().SizeInt == (int)Constants.Size.Medium);

                // Clone the old value with updated Size; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass the Index.
                var newValue = new TValue
                {
                    Id = providerData.GetValue().Id,
                    SizeInt = (int)Constants.Size.XXLarge, // updated
                    ColorArgb = providerData.GetValue().ColorArgb,
                    Count = providerData.GetValue().Count
                };

                // Reuse the same key. Note: there is no UpsertAsync().
                session.Upsert(ref providerData.GetKey(), ref newValue, context);
            }

            sw.Stop();

            xxlDatas = GetSizeDatas(Constants.Size.XXLarge);
            mediumDatas = GetSizeDatas(Constants.Size.Medium);
            ok &= WriteUpdateResult(isInitial: false, "XXLarge", expected, xxlDatas.Length, AllMatch(xxlDatas, val => val.SizeInt == (int)Constants.Size.XXLarge));
            ok &= WriteUpdateResult(isInitial: false, "Medium", 0, mediumDatas.Length, true);

            return WriteStatus(activity, ok, expected, sw, isUpdate: true);
        }

        internal async static Task<bool> UpdateColorByRMW<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Updating Colors via RMW";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            FasterKVProviderData<Key, TValue>[] GetColorDatas(Color color)
                => useMultiGroups
                    ? session.Query(store.ColorPred, new ColorKey(color)).ToArray()
                    : session.Query(store.CombinedColorPred, new CombinedKey(color)).ToArray();

            var purpleDatas = GetColorDatas(Color.Purple);
            var ok = WriteUpdateResult(isInitial: true, "Purple", 0, purpleDatas.Length, true);
            var blueDatas = GetColorDatas(Color.Blue);
            ok &= WriteUpdateResult(isInitial: true, "Blue", blueCount, blueDatas.Length, AllMatch(blueDatas, val => val.ColorArgb == Color.Blue.ToArgb()));
            var expected = blueDatas.Length;
            Console.WriteLine($"{indent2}Changing all Blue to Purple");

            var context = new Context<TValue>();
            var input = new TInput { IPUColorInt = Color.Purple.ToArgb() };

            var sw = new Stopwatch();
            sw.Start();

            foreach (var providerData in blueDatas)
            {
                // This will call Functions<>.InPlaceUpdater if !flushAndEvict, else CopyUpdater.
                if (useAsync)
                    (await session.RMWAsync(ref providerData.GetKey(), ref input, context)).Complete();
                else
                    session.RMW(ref providerData.GetKey(), ref input, context);
            }
            sw.Stop();

            session.CompletePending(spinWait: true);

            purpleDatas = GetColorDatas(Color.Purple);
            blueDatas = GetColorDatas(Color.Blue);
            ok &= WriteUpdateResult(isInitial: false, "Purple", expected, purpleDatas.Length, AllMatch(purpleDatas, val => val.ColorArgb == Color.Purple.ToArgb()));
            ok &= WriteUpdateResult(isInitial: false, "Blue", 0, blueDatas.Length, true);

            return WriteStatus(activity, ok, expected, sw, isUpdate: true);
        }

        internal static bool UpdateCountByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Updating Counts via Upsert";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            var sw = new Stopwatch();
            sw.Start();

            var bin7 = 7;
            FasterKVProviderData<Key, TValue>[] GetCountDatas(int bin)
                => useMultiGroups
                    ? session.Query(store.CountBinPred, new CountBinKey(bin)).ToArray()
                    : session.Query(store.CombinedCountBinPred, new CombinedKey(bin)).ToArray();

            // First show we've nothing in the last bin, and get all in bin7.
            var lastBinDatas = GetCountDatas(CountBinKey.LastBin);
            var ok = WriteUpdateResult(isInitial: true, "LastBin", 0, lastBinDatas.Length, true);

            var bin7Datas = GetCountDatas(bin7);
            ok &= WriteUpdateResult(isInitial: true, "Bin7", bin7Count, bin7Datas.Length, AllMatch(bin7Datas, val => CountBinKey.GetBin(val.Count) == bin7));
            var expected = bin7Datas.Length;

            Console.WriteLine($"{indent2}Changing all Bin7 to LastBin");
            var context = new Context<TValue>();
            foreach (var providerData in bin7Datas)
            {
                // Get the old value and confirm it's as expected. We cannot have ref locals because this is an async function.
                Debug.Assert(CountBinKey.GetAndVerifyBin(providerData.GetValue().Count, out int tempBin) && tempBin == bin7);

                // Clone the old value with updated Count; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass the Index.
                var newValue = new TValue
                {
                    Id = providerData.GetValue().Id,
                    SizeInt = providerData.GetValue().SizeInt,
                    ColorArgb = providerData.GetValue().ColorArgb,
                    Count = providerData.GetValue().Count + (CountBinKey.LastBin - bin7) * CountBinKey.BinSize // updated
                };
                Debug.Assert(!CountBinKey.GetAndVerifyBin(newValue.Count, out tempBin) && tempBin == CountBinKey.LastBin);

                // Reuse the same key. Note: there is no UpsertAsync().
                session.Upsert(ref providerData.GetKey(), ref newValue, context);
            }

            // We updated all Bin7 to LastBin, so they should disappear because our Predicate returns null for LastBin.
            lastBinDatas = GetCountDatas(CountBinKey.LastBin);
            ok &= WriteUpdateResult(isInitial: false, "LastBin", 0, lastBinDatas.Length, true);

            bin7Datas = GetCountDatas(bin7);
            ok &= WriteUpdateResult(isInitial: false, "Bin7", 0, bin7Datas.Length, true);

            sw.Stop();
            return WriteStatus(activity, ok, expected, sw, isUpdate: true);
        }

        internal async static Task<bool> Delete<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Deleting Colors";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            var sw = new Stopwatch();
            sw.Start();

            async Task<FasterKVProviderData<Key, TValue>[]> GetColorDatas(Color color)
                => useMultiGroups
                    ? useAsync 
                        ? await session.QueryAsync(store.ColorPred, new ColorKey(color)).ToArrayAsync()
                        : session.Query(store.ColorPred, new ColorKey(color)).ToArray()
                    : useAsync
                        ? await session.QueryAsync(store.CombinedColorPred, new CombinedKey(color)).ToArrayAsync()
                        : session.Query(store.CombinedColorPred, new CombinedKey(color)).ToArray();

            var redDatas = await GetColorDatas(Color.Red);
            var ok = WriteUpdateResult(isInitial: true, "Red", redCount, redDatas.Length, AllMatch(redDatas, val => val.ColorArgb == Color.Red.ToArgb()));
            var expected = redDatas.Length;

            var context = new Context<TValue>();
            foreach (var providerData in redDatas)
            {
                // This will call Functions<>.InPlaceUpdater. Note: there is no DeleteAsync().
                session.Delete(ref providerData.GetKey(), context);

                // We query XXL count in final verification, so keep track of whether we've deleted one.
                if (providerData.GetValue().SizeInt == (int)Constants.Size.XXLarge)
                    ++xxlDeletedCount;
            }

            redDatas = await GetColorDatas(Color.Red);
            ok &= WriteUpdateResult(isInitial: false, "Red", 0, redDatas.Length, true, isDelete: true);

            sw.Stop();
            return WriteStatus(activity, ok, expected, sw, isUpdate: true);
        }

        internal async static Task<bool> QuerysForFinalVerification<TValue, TInput, TOutput, TFunctions, TSerializer>(Store<TValue, TInput, TOutput, TFunctions, TSerializer> store)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            const string activity = "Querying with no boolean ops";
            Console.WriteLine(activity);

            FlushAndEvictIfRequested(store);
            using var session = store.FasterKV.ForSI(new TFunctions()).NewSession<TFunctions>();

            var sw = new Stopwatch();
            sw.Start();

            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;
            long count = 0;

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery<TPKey>(IPredicate pred, TPKey key) where TPKey : struct
                => useAsync
                    ? await session.QueryAsync(pred, key).ToArrayAsync()
                    : session.Query(pred, key).ToArray();

            // Make sure operations were consistent across all values (and groups, if multiGroup).
            // These queries reflect the updated counts from prior operations, e.g. XXLarge count should now be what Medium count started as.

            providerDatas = useMultiGroups
                                ? await RunQuery(store.SizePred, new SizeKey(Constants.Size.Medium))
                                : await RunQuery(store.CombinedSizePred, new CombinedKey(Constants.Size.Medium));
            ok &= VerifyProviderDatas(providerDatas, "Medium", 0, ref count);
            providerDatas = useMultiGroups
                                ? await RunQuery(store.SizePred, new SizeKey(Constants.Size.XXLarge))
                                : await RunQuery(store.CombinedSizePred, new CombinedKey(Constants.Size.XXLarge));
            ok &= VerifyProviderDatas(providerDatas, "XXLarge", mediumCount - xxlDeletedCount, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.ColorPred, new ColorKey(Color.Blue))
                                : await RunQuery(store.CombinedColorPred, new CombinedKey(Color.Blue));
            ok &= VerifyProviderDatas(providerDatas, "Blue", 0, ref count);
            providerDatas = useMultiGroups
                                ? await RunQuery(store.ColorPred, new ColorKey(Color.Purple))
                                : await RunQuery(store.CombinedColorPred, new CombinedKey(Color.Purple));
            ok &= VerifyProviderDatas(providerDatas, "Purple", blueCount, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.ColorPred, new ColorKey(Color.Red))
                                : await RunQuery(store.CombinedColorPred, new CombinedKey(Color.Red));
            ok &= VerifyProviderDatas(providerDatas, "Red", 0, ref count);

            providerDatas = useMultiGroups
                                ? await RunQuery(store.CountBinPred, new CountBinKey(7))
                                : await RunQuery(store.CombinedCountBinPred, new CombinedKey(7));
            ok &= VerifyProviderDatas(providerDatas, "Bin7", 0, ref count);
            providerDatas = useMultiGroups
                                ? await RunQuery(store.CountBinPred, new CountBinKey(CountBinKey.LastBin))
                                : await RunQuery(store.CombinedCountBinPred, new CombinedKey(CountBinKey.LastBin));
            ok &= VerifyProviderDatas(providerDatas, "LastBin", 0, ref count);  // Insert skipped (returned null from the Predicate) all that fall into the last bin

            sw.Stop();
            return WriteStatus(activity, ok, count, sw, isUpdate: false);
        }
    }
}
