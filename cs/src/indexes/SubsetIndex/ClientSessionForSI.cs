// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetIndex;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Diagnostics;

namespace FASTER.indexes.SubsetIndex
{
    /// <summary>
    /// A SubsetIndex-enabled wrapper around a <see cref="ClientSession{Key, Value, Input, Output, TContext, Functions}"/>
    /// </summary>
    public sealed class ClientSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> : IDisposable
#if DEBUG
        , IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext>
#endif
        where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
    {
        private readonly AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions>> fkvSession;
        private readonly bool fkvCopyReadsToTail;
        private readonly bool fkvSessionSupportAsync;
        private readonly LogAccessor<TKVKey, TKVValue> fkvLogAccessor;
        private readonly RecordAccessor<TKVKey, TKVValue> fkvRecordAccessor;
        private readonly IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> indexingFunctions;

        private readonly AdvancedClientSession<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context,
                                          IAdvancedFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>> fkvLivenessSession;

        private readonly ClientSessionSI<FasterKVProviderData<TKVKey, TKVValue>, long> indexSession;

        internal const string NotAsyncSessionErr = ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int>>.NotAsyncSessionErr;

        internal ClientSessionForSI(FasterKV<TKVKey, TKVValue> fkv, IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> indexingFunctions,
                                  AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions>> fkvSession, bool fkvSessionSupportAsync,
                                  AdvancedClientSession<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context,
                                                   IAdvancedFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>> fkvLivenessSession,
                                  SubsetIndex<FasterKVProviderData<TKVKey, TKVValue>, long> subsetIndex)
        {
            this.fkvLogAccessor = fkv.Log;
            this.fkvRecordAccessor = fkv.RecordAccessor;
            this.fkvCopyReadsToTail = fkv.CopyReadsToTail;
            this.indexingFunctions = indexingFunctions;
            this.fkvSession = fkvSession;
            this.fkvSessionSupportAsync = fkvSessionSupportAsync;
            this.fkvLivenessSession = fkvLivenessSession;

            this.indexSession = subsetIndex.NewSession();
        }

        private void EnterIndexableOperation(IndexableOperation indexableOp)
        {
            if (this.indexingFunctions.IsSet)
                throw new InvalidOperationExceptionSI("Cannot execute concurrent actions on a session");
            this.indexingFunctions.IndexableOp = indexableOp;
        }

        #region SubsetIndex Queries
        internal FasterKVProviderData<TKVKey, TKVValue> CreateProviderData(long logicalAddress, LivenessFunctions<TKVKey, TKVValue>.Context context)
        {
            try
            {
                // Look up logicalAddress in the primary FasterKV by address only, and returns the key and value. The key is needed for
                // the liveness loop, and if the record is live, we'll return the key and value this call obtains.
                var input = new LivenessFunctions<TKVKey, TKVValue>.Input { logAccessor = this.fkvLogAccessor };

                Status status = this.fkvLivenessSession.ReadAtAddress(logicalAddress, ref input, ref context.output, ReadFlags.SkipReadCache, context);
                if (status == Status.PENDING)
                {
                    this.fkvLivenessSession.CompletePending(spinWait: true);
                    status = context.PendingResultStatus;
                }
                if (status != Status.OK)
                    return null;

                // Now prepare to confirm liveness: Look up the key and see if the address matches (it must be the highest non-readCache address for the key).
                // Setting input.LogAccessor to null switches Concurrent/SingleReader mode from "get the key and value at this address" to "traverse the liveness chain".
                input.logAccessor = null;
                RecordInfo recordInfo = default;

                while (true)
                {
                    status = this.fkvLivenessSession.Read(ref context.output.GetKey(), ref input, ref context.output, ref recordInfo, ReadFlags.SkipReadCache, context);
                    if (status == Status.PENDING)
                    {
                        this.fkvLivenessSession.CompletePending(spinWait: true);
                        status = context.PendingResultStatus;
                    }

                    if (status != Status.OK || context.output.currentAddress != logicalAddress)
                        return null;

                    context.output.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                    return new FasterKVProviderData<TKVKey, TKVValue>(keyContainer, valueContainer);
                }
            }
            finally
            {
                context.output.Dispose();
            }
        }

        internal IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> ReturnProviderDatas(IEnumerable<long> logicalAddresses)
        {
            var livenessContext = new LivenessFunctions<TKVKey, TKVValue>.Context();
            return logicalAddresses.Select(la => this.CreateProviderData(la, livenessContext)).Where(data => data is {});
        }

#if NETSTANDARD21
        internal async ValueTask<FasterKVProviderData<TKVKey, TKVValue>> CreateProviderDataAsync(long logicalAddress, QuerySettings querySettings)
        {
            // Look up logicalAddress in the primary FasterKV by address only, and returns the key.
            var input = new LivenessFunctions<TKVKey, TKVValue>.Input { logAccessor = this.fkvLogAccessor };
            LivenessFunctions<TKVKey, TKVValue>.Output initialOutput = default;

            try
            {
                //  We ignore the updated previousAddress here; we're just looking for the key.
                Status status;
                var readAsyncResult = await fkvLivenessSession.ReadAtAddressAsync(logicalAddress, ref input, ReadFlags.SkipReadCache, default, cancellationToken: querySettings.CancellationToken);
                if (querySettings.IsCanceled)
                    return null;
                (status, initialOutput) = readAsyncResult.Complete();
                if (status != Status.OK)
                    return null;

                // Now prepare to confirm liveness: Look up the key and see if the address matches (it must be the highest non-readCache address for the key).
                // Setting input.LogAccessor to null switches Concurrent/SingleReader mode from "get the key and value at this address" to "traverse the liveness chain".
                input.logAccessor = null;
                RecordInfo recordInfo = default;

                while (true)
                {
                    readAsyncResult = await this.fkvLivenessSession.ReadAsync(ref initialOutput.GetKey(), ref input, recordInfo.PreviousAddress, ReadFlags.SkipReadCache, default, cancellationToken:querySettings.CancellationToken);
                    if (querySettings.IsCanceled)
                        return null;
                    LivenessFunctions<TKVKey, TKVValue>.Output output = default;
                    (status, output) = readAsyncResult.Complete(out recordInfo);

                    if (status != Status.OK || output.currentAddress != logicalAddress)
                        return null;

                    if (status != Status.OK || output.currentAddress != logicalAddress)
                        return null;

                    initialOutput.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                    return new FasterKVProviderData<TKVKey, TKVValue>(keyContainer, valueContainer);
                }
            }
            finally
            {
                initialOutput.Dispose();
            }
        }

        internal async IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> ReturnProviderDatasAsync(IAsyncEnumerable<long> logicalAddresses, QuerySettings querySettings)
        {
            querySettings ??= QuerySettings.Default;

            // For the async form, we always read fully; there is no pending.
            await foreach (var logicalAddress in logicalAddresses)
            {
                var providerData = await this.CreateProviderDataAsync(logicalAddress, querySettings);
                if (providerData is {})
                    yield return providerData;
            }
        }
#endif

        /// <summary>
        /// Issue a query on a single <see cref="Predicate{TPKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.Query(sizePred, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPKey">The type of the key value to return results for</typeparam>
        /// <param name="pred">The Predicate object</param>
        /// <param name="key">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey>(
                IPredicate pred, TPKey key, QuerySettings querySettings = null)
            where TPKey : struct
            => this.ReturnProviderDatas(this.indexSession.Query(pred, key, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on a single <see cref="Predicate{TPKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(sizePred, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPKey">The type of the key value to return results for</typeparam>
        /// <param name="pred">The Predicate object</param>
        /// <param name="key">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey>(
                IPredicate pred, TPKey key, QuerySettings querySettings = null)
            where TPKey : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred, key, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on a single <see cref="Predicate{TPKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(sizePred, new TesTPKey[] { Size.Medium, Size.Large })) {...}
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey">The type of the key value to return results for</typeparam>
        /// <param name="pred">The Predicate object</param>
        /// <param name="keys">A vector of key values to return results for; for example, an OR query on
        ///     a single Predicate, or a range query for a Predicate that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey>(
                IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings = null)
            where TPKey : struct 
            => this.ReturnProviderDatas(this.indexSession.Query(pred, keys, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on a single <see cref="Predicate{TPKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(sizePred, new TesTPKey[] { Size.Medium, Size.Large })) {...}
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey">The type of the key value to return results for</typeparam>
        /// <param name="pred">The Predicate object</param>
        /// <param name="keys">A vector of key values to return results for; for example, an OR query on
        ///     a single Predicate, or a range query for a Predicate that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey>(
                IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings = null)
            where TPKey : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred, keys, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on two <see cref="Predicate{TPKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fkvSi.Query(sizePred, Size.Medium, colorPred, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="key1">The key value to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(pred1, key1, pred2, key2, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on two <see cref="Predicate{TPKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fkvSi.Query(sizePred, Size.Medium, colorPred, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="key1">The key value to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred1, key1, pred2, key2, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on two <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The secojnd Predicate object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2>(
                    IPredicate pred1, IEnumerable<TPKey1> keys1,
                    IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(pred1, keys1, pred2, keys2, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on two <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2>(
                    IPredicate pred1, IEnumerable<TPKey1> keys1,
                    IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred1, keys1, pred2, keys2, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on three <see cref="Predicate{TPKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fkvSi.Query(sizePred, Size.Medium, colorPred, Color.Red, countPred, 7, (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="pred3">The third Predicate object</param>
        /// <param name="key1">The key value to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key3">The key value to return results from the third <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, 3) whether the record matches the third Predicate, and returns
        /// a bool indicating whether the record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2, TPKey3>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(pred1, key1, pred2, key2, pred3, key3, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on three <see cref="Predicate{TPKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fkvSi.Query(sizePred, Size.Medium, colorPred, Color.Red, countPred, 7, (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="pred3">The third Predicate object</param>
        /// <param name="key1">The key value to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="key3">The key value to return results from the third <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, 3) whether the record matches the third Predicate,
        /// and returns a bool indicating whether the record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2, TPKey3>(
                    IPredicate pred1, TPKey1 key1,
                    IPredicate pred2, TPKey2 key2,
                    IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred1, key1, pred2, key2, pred3, key3, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on three <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         countPred, new [] { new CountKey(7), new CountKey(42) },
        ///         (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="pred3">The third Predicate object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys3">The key values to return results from the third <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, 3) whether the record matches the third Predicate,
        /// and returns a bool indicating whether the record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2, TPKey3>(
                    IPredicate pred1, IEnumerable<TPKey1> keys1,
                    IPredicate pred2, IEnumerable<TPKey2> keys2,
                    IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(pred1, keys1, pred2, keys2, pred3, keys3, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on three <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         countPred, new [] { new CountKey(7), new CountKey(42) },
        ///         (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate object</param>
        /// <param name="pred2">The second Predicate object</param>
        /// <param name="pred3">The third Predicate object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys3">The key values to return results from the third <see cref="Predicate{TPKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first Predicate, 2) whether the record matches the second Predicate, 3) whether the record matches the third Predicate,
        /// and returns a bool indicating whether the record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2, TPKey3>(
                    IPredicate pred1, IEnumerable<TPKey1> keys1,
                    IPredicate pred2, IEnumerable<TPKey2> keys2,
                    IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(pred1, keys1, pred2, keys2, pred3, keys3, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on one or more <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] {
        ///             (sizePred, new TesTPKey[] { Size.Medium, Size.Large }),
        ///             (colorPred, new TesTPKey[] { Color.Red, Color.Blue})},
        ///         ll => ll[0]))
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey">The type of the key value for the <see cref="Predicate{TPKey, TRecordId}"/> vector</typeparam>
        /// <param name="predsAndKeys">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="predsAndKeys"/> vector indicating whether a candidate record matches the corresponding
        /// <see cref="Predicate{TPKey, TRecordId}"/>, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of the input vector are true,
        /// else false; an OR query would return true if element of the input vector is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct
            => this.ReturnProviderDatas(this.indexSession.Query(predsAndKeys, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on one or more <see cref="Predicate{TPKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] {
        ///             (sizePred, new TesTPKey[] { Size.Medium, Size.Large }),
        ///             (colorPred, new TesTPKey[] { Color.Red, Color.Blue})},
        ///         ll => ll[0]))
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey">The type of the key value for the <see cref="Predicate{TPKey, TRecordId}"/> vector</typeparam>
        /// <param name="predsAndKeys">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="predsAndKeys"/> vector indicating whether a candidate record matches the corresponding
        /// <see cref="Predicate{TPKey, TRecordId}"/>, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of the input vector are true,
        /// else false; an OR query would return true if element of the input vector is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(predsAndKeys, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on multiple keys <see cref="Predicate{TPKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] {
        ///             (sizePred, new TesTPKey[] { Size.Medium, Size.Large }),
        ///             (colorPred, new TesTPKey[] { Color.Red, Color.Blue })},
        ///         new[] {
        ///             (countPred, new [] { new CountKey(7), new CountKey(9) })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predsAndKeys1">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey1"/> to be queried</param>
        /// <param name="predsAndKeys2">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey2"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="predsAndKeys1"/> vector and a second boolean vector in parallel with 
        /// the <paramref name="predsAndKeys2"/> vector, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of both input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific Predicates.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predsAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(predsAndKeys1, predsAndKeys2, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on multiple keys <see cref="Predicate{TPKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] {
        ///             (sizePred, new TesTPKey[] { Size.Medium, Size.Large }),
        ///             (colorPred, new TesTPKey[] { Color.Red, Color.Blue })},
        ///         new[] {
        ///             (countPred, new [] { new CountKey(7), new CountKey(9) })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// (Note that this example requires an implicit TesTPKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predsAndKeys1">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey1"/> to be queried</param>
        /// <param name="predsAndKeys2">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey2"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="predsAndKeys1"/> vector and a second boolean vector in parallel with 
        /// the <paramref name="predsAndKeys2"/> vector, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of both input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific Predicates.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predsAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(predsAndKeys1, predsAndKeys2, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on multiple keys <see cref="Predicate{TPKey, TRecordId}"/>s for three different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] { (sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) }) },
        ///         new[] { (colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) }) },
        ///         new[] { (countPred, new [] { new CountKey(4), new CountKey(7) }) },
        ///         (ll, mm, rr) => ll[0] || mm[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predsAndKeys1">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey1"/> to be queried</param>
        /// <param name="predsAndKeys2">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey2"/> to be queried</param>
        /// <param name="predsAndKeys3">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey3"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters three boolean vectors in parallel with 
        /// each other, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of all input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific Predicates.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> Query<TPKey1, TPKey2, TPKey3>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predsAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predsAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatas(this.indexSession.Query(predsAndKeys1, predsAndKeys2, predsAndKeys3, matchPredicate, querySettings));

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on multiple keys <see cref="Predicate{TPKey, TRecordId}"/>s for three different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fkvSi.Query(
        ///         new[] { (sizePred, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) }) },
        ///         new[] { (colorPred, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) }) },
        ///         new[] { (countPred, new [] { new CountKey(4), new CountKey(7) }) },
        ///         (ll, mm, rr) => ll[0] || mm[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPKey1">The type of the key value for the first vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key value for the second vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey3">The type of the key value for the third vector's <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predsAndKeys1">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey1"/> to be queried</param>
        /// <param name="predsAndKeys2">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey2"/> to be queried</param>
        /// <param name="predsAndKeys3">A vector of <see cref="Predicate{TPKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPKey3"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters three boolean vectors in parallel with 
        /// each other, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of all input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific Predicates.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryAsync<TPKey1, TPKey2, TPKey3>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predsAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predsAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.ReturnProviderDatasAsync(this.indexSession.QueryAsync(predsAndKeys1, predsAndKeys2, predsAndKeys3, matchPredicate, querySettings), querySettings);
#endif // NETSTANDARD21
        #endregion SubsetIndex Queries

        #region IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> implementation

        /// <inheritdoc/>
        public string ID => this.fkvSession.ID;

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TInput input, ref TOutput output, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        public Status Read(TKVKey key, TInput input, out TOutput output, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(key, input, out output, userContext, serialNo);
        }

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TOutput output, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(ref key, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        public Status Read(TKVKey key, out TOutput output, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(key, out output, userContext, serialNo);
        }

        /// <inheritdoc/>
        public (Status, TOutput) Read(TKVKey key, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(key, userContext, serialNo);
        }

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TInput input, ref TOutput output, ref RecordInfo recordInfo, ReadFlags readFlags = ReadFlags.None, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.Read(ref key, ref input, ref output, ref recordInfo, readFlags, userContext, serialNo);
        }

        /// <inheritdoc/>
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ReadFlags readFlags = ReadFlags.None, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAtAddress(address, ref input, ref output, readFlags, userContext, serialNo);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAsync(ref TKVKey key, ref TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAsync(ref key, ref input, context, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAsync(TKVKey key, TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAsync(ref key, ref input, context, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAsync(ref TKVKey key, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAsync(ref key, context, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAsync(TKVKey key, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAsync(ref key, context, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAsync(ref TKVKey key, ref TInput input, long startAddress, ReadFlags readFlags = ReadFlags.None,
                                                                                                          TContext userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.Read);
            return this.fkvSession.ReadAsync(ref key, ref input, startAddress, readFlags, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext>> ReadAtAddressAsync(long address, ref TInput input, ReadFlags readFlags = ReadFlags.None, 
                                                                                                                   TContext userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
            => this.fkvSession.ReadAtAddressAsync(address, ref input, readFlags, userContext, serialNo, cancellationToken);

        /// <inheritdoc/>
        public Status Upsert(ref TKVKey key, ref TKVValue desiredValue, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Upsert);
            try
            {
                var status = this.fkvSession.Upsert(ref key, ref desiredValue, userContext, serialNo);
                if (status == Status.OK)
                {
                    var providerData = this.indexingFunctions.ChangeTracker is null
                                        ? new FasterKVProviderData<TKVKey, TKVValue>(this.fkvLogAccessor.GetKeyContainer(ref key),
                                                                                     this.fkvLogAccessor.GetValueContainer(ref desiredValue))
                                        : this.indexingFunctions.ChangeTracker.AfterData;
                    status = this.indexSession.Upsert(providerData, this.indexingFunctions.LogicalAddress, this.indexingFunctions.ChangeTracker);
                }
                return status;
            }
            finally
            {
                this.indexingFunctions.Clear();
            }
        }

        /// <inheritdoc/>
        public Status RMW(ref TKVKey key, ref TInput input, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.RMW);
            try
            {
                var status = this.fkvSession.RMW(ref key, ref input, userContext, serialNo);
                if (status == Status.OK || status == Status.NOTFOUND)
                {
                    status = this.indexSession.Update(this.indexingFunctions.ChangeTracker);
                }
                return status;
            }
            finally
            {
                this.indexingFunctions.Clear();
            }
        }

        /// <inheritdoc/>
        public Status RMW(TKVKey key, TInput input, TContext userContext = default, long serialNo = 0) => RMW(ref key, ref input, userContext, serialNo);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext>> RMWAsync(ref TKVKey key, ref TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            EnterIndexableOperation(IndexableOperation.RMW);
            Debug.Assert(fkvSessionSupportAsync, NotAsyncSessionErr);
            return this.CompleteRMWAsync(this.fkvSession.RMWAsync(ref key, ref input, context, serialNo, cancellationToken), cancellationToken);
        }

        private async ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext>> CompleteRMWAsync(
                ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext>> primaryFkvValueTask, CancellationToken cancellationToken)
        {
            try
            {
                Status primaryFkvStatus = (await primaryFkvValueTask).Complete();

                // Either the operation completed synchronously (indexingFunctions.ChangeTracker is not null) or RMWCompletionCallback should have been called exactly once.
                Debug.Assert(indexingFunctions.ChangeTracker is { } || indexingFunctions.Queue.Count == 1);
                await this.indexSession.UpdateAsync(indexingFunctions.ChangeTracker ?? indexingFunctions.Queue.Dequeue(), cancellationToken);

                // Map to unwrapped TFunctions type.
                return new FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext>(primaryFkvStatus, default);
            }
            finally
            {
                this.indexingFunctions.Clear();
            }
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext>> RMWAsync(TKVKey key, TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
            => this.RMWAsync(ref key, ref input, context, serialNo, cancellationToken);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(ref TKVKey key, TContext userContext = default, long serialNo = 0)
        {
            EnterIndexableOperation(IndexableOperation.Delete);
            try
            {
                var status = this.fkvSession.Delete(ref key, userContext, serialNo);

                // If there is no changeTracker, the record was not in memory, so we could not get the old value; we will have to let the liveness check fail the dead record.
                if (status == Status.OK && this.indexingFunctions.ChangeTracker is { })
                {
                    status = this.indexSession.Delete(this.indexingFunctions.ChangeTracker);
                }
                return status;
            }
            finally
            {
                this.indexingFunctions.Clear();
            }
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(TKVKey key, TContext userContext = default, long serialNo = 0) => this.Delete(ref key, userContext, serialNo);

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests() => this.fkvSession.GetPendingRequests();

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh() => this.fkvSession.Refresh();

        private bool DetachTrackers(out List<ChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long>> trackers)
        {
            trackers = this.indexingFunctions.Queue.ToList();
            this.indexingFunctions.Queue.Clear();
            return trackers.Count > 0;
        }

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            var result = this.fkvSession.CompletePending(spinWait, spinWaitForCommit);

            // Execute any pending change operations on the session.
            if (this.DetachTrackers(out var trackers))
            {
                foreach (var tracker in trackers)
                {
                    Status status = tracker.UpdateOp switch
                    {
                        var op when op == UpdateOperation.IPU || op == UpdateOperation.RCU || op == UpdateOperation.Insert => this.indexSession.Update(tracker),
                        UpdateOperation.Delete => this.indexSession.Delete(tracker),
                        _ => throw new InternalErrorExceptionSI("Unexpected UpdateOperation"),
                    };
                    if (status == Status.ERROR)
                    {
                        // TODO handle error in CompletePending
                    }
                }
            }
            return this.indexSession.CompletePending(spinWait, spinWaitForCommit) && result; // TODO: Resolve issues with non-async operations in groups
        }

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken cancellationToken = default)
        {
            // Simple sequence to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            await this.fkvSession.CompletePendingAsync(waitForCommit, cancellationToken);

            // Execute any pending change operations on the session.
            if (this.DetachTrackers(out var trackers))
            {
                foreach (var tracker in trackers)
                {
                    ValueTask task = tracker.UpdateOp switch
                    {
                        var op when op == UpdateOperation.IPU || op == UpdateOperation.RCU || op == UpdateOperation.Insert => this.indexSession.UpdateAsync(tracker, cancellationToken),
                        UpdateOperation.Delete => this.indexSession.DeleteAsync(tracker, cancellationToken),
                        _ => throw new InternalErrorExceptionSI("Unexpected UpdateOperation"),
                    };
                    await task;
                }
            }
            await this.indexSession.CompletePendingAsync(waitForCommit, cancellationToken);    // TODO: Resolve issues with non-async operations in groups
        }

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken cancellationToken = default)
        {
            await this.fkvSession.ReadyToCompletePendingAsync(cancellationToken);
            await this.indexSession.ReadyToCompletePendingAsync(cancellationToken);
        }

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken cancellationToken = default)
        {
            await this.fkvSession.WaitForCommitAsync(cancellationToken);
            await this.indexSession.WaitForCommitAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public void Dispose() => this.fkvSession.Dispose();

        #endregion IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> implementation
    }
}
