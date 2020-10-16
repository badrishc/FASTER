// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using PSF.Index;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;

namespace FASTER.PSF
{
    /// <summary>
    /// A PSF-enabled wrapper around a <see cref="ClientSession{Key, Value, Input, Output, TContext, Functions}"/>
    /// </summary>
    public sealed class PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> : IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions>
        where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
    {
        private readonly ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> fkvSession;
        private readonly LogAccessor<TKVKey, TKVValue> fkvLogAccessor;
        private readonly WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> wrapperFunctions;

        private readonly LivenessFunctions<TKVKey, TKVValue> fkvLivenessFunctions;
        private readonly ClientSession<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context,
                                          IFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>> fkvLivenessSession;

        private readonly PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfManager;
        private readonly PSFIndexSession<FasterKVProviderData<TKVKey, TKVValue>, long> psfSession;

        internal PSFClientSession(LogAccessor<TKVKey, TKVValue> logAccessor,
                                  WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> wrapperFunctions,
                                  ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> session,
                                  LivenessFunctions<TKVKey, TKVValue> livenessFunctions,
                                  ClientSession<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context,
                                                   IFunctions<TKVKey, TKVValue, LivenessFunctions<TKVKey, TKVValue>.Input, LivenessFunctions<TKVKey, TKVValue>.Output, LivenessFunctions<TKVKey, TKVValue>.Context>> livenessSession,
                                  PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfManager)
        {
            this.fkvSession = session;
            this.fkvLogAccessor = logAccessor;
            this.wrapperFunctions = wrapperFunctions;
            this.psfManager = psfManager;
            this.psfSession = psfManager.NewSession();

            this.fkvLivenessFunctions = livenessFunctions;
            this.fkvLivenessSession = livenessSession;
        }

        private void ThrowIfActive()
        {
            if (this.wrapperFunctions.IsSet)
                throw new PSFInvalidOperationException("Cannot execute concurrent actions on a session");
        }

        #region PSF Queries
        internal FasterKVProviderData<TKVKey, TKVValue> CreateProviderData(long logicalAddress, LivenessFunctions<TKVKey, TKVValue>.Context context)
        {
            // Look up logicalAddress in the primary FasterKV by address only, and returns the key.
            var input = new LivenessFunctions<TKVKey, TKVValue>.Input { logAccessor = this.fkvLogAccessor };

            try
            {
                //  We ignore the updated previousAddress here; we're just looking for the key.
                Status status = this.fkvLivenessSession.GetKey(logicalAddress, ref input, ref context.output, context);
                if (status == Status.PENDING)
                    this.fkvLivenessSession.CompletePending(spinWait: true);
                if (status != Status.OK)
                    return null;

                // Now confirm liveness: Read all records in the key chain in a loop until we find logicalAddress or run out of records.
                // Start initially by reading the key; then previousAddress will be updated on each loop iteration and will be used for the following iteration.
                input.logAccessor = null;
                RecordInfo recordInfo = default;
                context.output.currentAddress = Constants.kInvalidAddress;
 
                while (true)
                {
                    status = this.fkvLivenessSession.Read(ref context.output.GetKey(), ref input, ref context.output, recordInfo.PreviousAddress, out recordInfo, context);
                    if (status == Status.PENDING)
                    {
                        this.fkvLivenessSession.CompletePending(spinWait: true);
                        recordInfo = context.output.recordInfo;
                        status = context.status;
                    }

                    // Invariant: address chains always move downward. Therefore if context.output.currentAddress < logicalAddress, logicalAddress is not live.
                    if (status != Status.OK || context.output.currentAddress < logicalAddress)
                        return null;
                    
                    if (context.output.currentAddress == logicalAddress)
                    {
                        context.output.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                        return new FasterKVProviderData<TKVKey, TKVValue>(keyContainer, valueContainer);
                    }
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
            return logicalAddresses.Select(la => this.CreateProviderData(la, livenessContext)).Where(data => !(data is null));
        }

#if NETSTANDARD21
        internal async ValueTask<FasterKVProviderData<TKVKey, TKVValue>> CreateProviderDataAsync(long logicalAddress, PSFQuerySettings querySettings)
        {
            // Look up logicalAddress in the primary FasterKV by address only, and returns the key.
            var input = new LivenessFunctions<TKVKey, TKVValue>.Input { logAccessor = this.fkvLogAccessor };
            LivenessFunctions<TKVKey, TKVValue>.Output initialOutput = default;

            try
            {
                //  We ignore the updated previousAddress here; we're just looking for the key.
                Status status;
                var readAsyncResult = await fkvLivenessSession.GetKeyAsync(logicalAddress, ref input, default, cancellationToken: querySettings.CancellationToken);
                if (querySettings.IsCanceled)
                    return null;
                (status, initialOutput) = readAsyncResult.Complete();
                if (status != Status.OK)    // TODOerr: check other status
                    return null;

                // Now confirm liveness: Read all records in the key chain in a loop until we find logicalAddress or run out of records.
                // Start initially by reading the key; then previousAddress will be updated on each loop iteration and will be used for the following iteration.
                input.logAccessor = null;
                RecordInfo recordInfo = default;

                while (true)
                {
                    readAsyncResult = await this.fkvLivenessSession.ReadAsync(ref initialOutput.GetKey(), ref input, recordInfo.PreviousAddress, default, cancellationToken:querySettings.CancellationToken);
                    if (querySettings.IsCanceled)
                        return null;
                    LivenessFunctions<TKVKey, TKVValue>.Output output = default;
                    (status, output) = readAsyncResult.Complete(out recordInfo);

                    // Invariant: address chains always move downward. Therefore if context.output.currentAddress < logicalAddress, logicalAddress is not live.
                    if (status != Status.OK || output.currentAddress < logicalAddress)
                        return null;

                    if (output.currentAddress == logicalAddress)
                    {
                        initialOutput.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                        return new FasterKVProviderData<TKVKey, TKVValue>(keyContainer, valueContainer);
                    }
                }
            }
            finally
            {
                initialOutput.Dispose();
            }
        }

        internal async IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> ReturnProviderDatasAsync(IAsyncEnumerable<long> logicalAddresses, PSFQuerySettings querySettings)
        {
            querySettings ??= PSFQuerySettings.Default;

            // For the async form, we always read fully; there is no pending.
            await foreach (var logicalAddress in logicalAddresses)
            {
                var providerData = await this.CreateProviderDataAsync(logicalAddress, querySettings);
                if (!(providerData is null))
                    yield return providerData;
            }
        }
#endif

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(sizePsf, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="key">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey>(
                IPSF psf, TPSFKey key, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf, key, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(sizePsf, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="key">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey>(
                IPSF psf, TPSFKey key, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf, key, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(sizePsf, new TestPSFKey[] { Size.Medium, Size.Large })) {...}
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="keys">A vector of key values to return results for; for example, an OR query on
        ///     a single PSF, or a range query for a PSF that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey>(
                IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf, keys, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(sizePsf, new TestPSFKey[] { Size.Medium, Size.Large })) {...}
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="keys">A vector of key values to return results for; for example, an OR query on
        ///     a single PSF, or a range query for a PSF that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey>(
                IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf, keys, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in psfKV.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf1, key1, psf2, key2, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in psfKV.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf1, key1, psf2, key2, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The secojnd Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2>(
                    IPSF psf1, IEnumerable<TPSFKey1> keys1,
                    IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf1, keys1, psf2, keys2, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The secojnd Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IPSF psf1, IEnumerable<TPSFKey1> keys1,
                    IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf1, keys1, psf2, keys2, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in psfKV.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, countPsf, 7, (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key3">The key value to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf1, key1, psf2, key2, psf3, key3, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in psfKV.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, countPsf, 7, (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key3">The key value to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf1, key1, psf2, key2, psf3, key3, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         countPsf, new [] { new CountKey(7), new CountKey(42) },
        ///         (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys3">The key values to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IPSF psf1, IEnumerable<TPSFKey1> keys1,
                    IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psf1, keys1, psf2, keys2, psf3, keys3, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         countPsf, new [] { new CountKey(7), new CountKey(42) },
        ///         (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys3">The key values to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IPSF psf1, IEnumerable<TPSFKey1> keys1,
                    IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psf1, keys1, psf2, keys2, psf3, keys3, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on one or more <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue})},
        ///         ll => ll[0]))
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value for the <see cref="PSF{TPSFKey, TRecordId}"/> vector</typeparam>
        /// <param name="psfsAndKeys">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys"/> vector indicating whether a candidate record matches the corresponding
        /// <see cref="PSF{TPSFKey, TRecordId}"/>, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of the input vector are true,
        /// else false; an OR query would return true if element of the input vector is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psfsAndKeys, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on one or more <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue})},
        ///         ll => ll[0]))
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value for the <see cref="PSF{TPSFKey, TRecordId}"/> vector</typeparam>
        /// <param name="psfsAndKeys">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys"/> vector indicating whether a candidate record matches the corresponding
        /// <see cref="PSF{TPSFKey, TRecordId}"/>, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of the input vector are true,
        /// else false; an OR query would return true if element of the input vector is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psfsAndKeys, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue })},
        ///         new[] {
        ///             (countPsf, new [] { new CountKey(7), new CountKey(9) })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys1"/> vector and a second boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys2"/> vector, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of both input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psfsAndKeys1, psfsAndKeys2, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue })},
        ///         new[] {
        ///             (countPsf, new [] { new CountKey(7), new CountKey(9) })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys1"/> vector and a second boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys2"/> vector, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of both input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psfsAndKeys1, psfsAndKeys2, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21

        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for three different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] { (sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) }) },
        ///         new[] { (colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) }) },
        ///         new[] { (countPsf, new [] { new CountKey(4), new CountKey(7) }) },
        ///         (ll, mm, rr) => ll[0] || mm[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="psfsAndKeys3">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey3"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters three boolean vectors in parallel with 
        /// each other, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of all input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.psfSession.QueryPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3, matchPredicate, querySettings));
        }

#if NETSTANDARD21
        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for three different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in psfKV.QueryPSF(
        ///         new[] { (sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) }) },
        ///         new[] { (colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) }) },
        ///         new[] { (countPsf, new [] { new CountKey(4), new CountKey(7) }) },
        ///         (ll, mm, rr) => ll[0] || mm[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="psfsAndKeys3">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey3"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters three boolean vectors in parallel with 
        /// each other, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of all input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IAsyncEnumerable<FasterKVProviderData<TKVKey, TKVValue>> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatasAsync(this.psfSession.QueryPSFAsync(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3, matchPredicate, querySettings), querySettings);
        }
#endif // NETSTANDARD21
        #endregion PSF Queries

        #region IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> implementation

        /// <inheritdoc/>
        public string ID => this.fkvSession.ID;

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TInput input, ref TOutput output, TContext userContext, long serialNo)
            => this.fkvSession.Read(ref key, ref input, ref output, userContext, serialNo);

        /// <inheritdoc/>
        public Status Read(TKVKey key, TInput input, out TOutput output, TContext userContext = default, long serialNo = 0)
            => this.fkvSession.Read(key, input, out output, userContext, serialNo);

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TOutput output, TContext userContext = default, long serialNo = 0)
            => this.fkvSession.Read(ref key, ref output, userContext, serialNo);

        /// <inheritdoc/>
        public Status Read(TKVKey key, out TOutput output, TContext userContext = default, long serialNo = 0)
            => this.fkvSession.Read(key, out output, userContext, serialNo);

        /// <inheritdoc/>
        public (Status, TOutput) Read(TKVKey key, TContext userContext = default, long serialNo = 0) 
            => this.fkvSession.Read(key, userContext, serialNo);

        /// <inheritdoc/>
        public Status Read(ref TKVKey key, ref TInput input, ref TOutput output, long startAddress, out RecordInfo recordInfo, TContext userContext = default, long serialNo = 0)
            => this.fkvSession.Read(ref key, ref input, ref output, startAddress, out recordInfo, userContext, serialNo);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ReadAsync(ref TKVKey key, ref TInput input, TContext context = default, long serialNo = 0, CancellationToken token = default)
            => this.fkvSession.ReadAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ReadAsync(TKVKey key, TInput input, TContext context = default, long serialNo = 0, CancellationToken token = default)
            => this.fkvSession.ReadAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ReadAsync(ref TKVKey key, TContext context = default, long serialNo = 0, CancellationToken token = default)
            => this.fkvSession.ReadAsync(ref key, context, serialNo, token);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ReadAsync(TKVKey key, TContext context = default, long serialNo = 0, CancellationToken token = default)
            => this.fkvSession.ReadAsync(ref key, context, serialNo, token);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ReadAsync(ref TKVKey key, ref TInput input, long startAddress, TContext userContext = default,
                                                                                                                         long serialNo = 0, CancellationToken cancellationToken = default)
            => this.fkvSession.ReadAsync(ref key, ref input, startAddress, userContext, serialNo, cancellationToken);

        /// <inheritdoc/>
        public Status GetKey(long logicalAddress, ref TInput input, ref TOutput output, TContext userContext = default, long serialNo = 0)
            => this.fkvSession.GetKey(logicalAddress, ref input, ref output, userContext, serialNo);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> GetKeyAsync(long logicalAddress, ref TInput input, TContext userContext = default,
                                                                                                                        long serialNo = 0, CancellationToken cancellationToken = default) 
            => this.fkvSession.GetKeyAsync(logicalAddress, ref input, userContext, serialNo, cancellationToken);

        /// <inheritdoc/>
        public Status Upsert(ref TKVKey key, ref TKVValue desiredValue, TContext userContext = default, long serialNo = 0)
        {
            ThrowIfActive();
            var status = this.fkvSession.Upsert(ref key, ref desiredValue, userContext, serialNo);
            if (status == Status.OK)
            {
                var providerData = this.wrapperFunctions.ChangeTracker is null
                                    ? new FasterKVProviderData<TKVKey, TKVValue>(this.fkvLogAccessor.GetKeyContainer(ref key),
                                                                                 this.fkvLogAccessor.GetValueContainer(ref desiredValue))
                                    : this.wrapperFunctions.ChangeTracker.AfterData;
                status = this.psfSession.Upsert(providerData, this.wrapperFunctions.LogicalAddress, this.wrapperFunctions.ChangeTracker);
            }
            return status;
        }

        /// <inheritdoc/>
        public Status RMW(ref TKVKey key, ref TInput input, TContext userContext = default, long serialNo = 0)
        {
            ThrowIfActive();
            var status = this.fkvSession.RMW(ref key, ref input, userContext, serialNo);
            if (status == Status.OK || status == Status.NOTFOUND)
            {
                status = this.psfSession.Update(this.wrapperFunctions.ChangeTracker);
            }
            return status;
        }

        /// <inheritdoc/>
        public Status RMW(TKVKey key, TInput input, TContext userContext = default, long serialNo = 0) => RMW(ref key, ref input, userContext, serialNo);

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext, TFunctions>> RMWAsync(ref TKVKey key, ref TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            ThrowIfActive();
            return this.CompleteRMWAsync(this.fkvSession.RMWAsync(ref key, ref input, context, serialNo, cancellationToken), cancellationToken);
        }

        private async ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext, TFunctions>> CompleteRMWAsync(ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext, TFunctions>> primaryFkvValueTask,
                                                                                                                                   CancellationToken cancellationToken)
        {
            var rmwAsyncResult = await primaryFkvValueTask;
            await this.psfSession.UpdateAsync(this.wrapperFunctions.ChangeTracker, cancellationToken);
            return rmwAsyncResult;
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<TKVKey, TKVValue>.RmwAsyncResult<TInput, TOutput, TContext, TFunctions>> RMWAsync(TKVKey key, TInput input, TContext context = default, long serialNo = 0, CancellationToken cancellationToken = default)
            => RMWAsync(ref key, ref input, context, serialNo, cancellationToken);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(ref TKVKey key, TContext userContext, long serialNo)
        {
            ThrowIfActive();
            var status = this.fkvSession.Delete(ref key, userContext, serialNo);
            if (status == Status.OK)
            {
                status = this.psfSession.Delete(this.wrapperFunctions.ChangeTracker);
            }
            return status;
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(TKVKey key, TContext userContext, long serialNo) => this.Delete(ref key, userContext, serialNo);

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

        private bool DetachTrackers(out List<PSFChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long>> trackers)
        {
            trackers = this.wrapperFunctions.Queue.ToList();
            this.wrapperFunctions.Queue.Clear();
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
            // TODO parallelize fkv and psfManager CompletePending
            var result = this.fkvSession.CompletePending(spinWait, spinWaitForCommit);
            if (this.DetachTrackers(out var trackers))
                this.psfSession.ProcessChanges(trackers);
            return this.psfSession.CompletePending(spinWait, spinWaitForCommit) && result; // TODO: Resolve issues with non-async operations in groups
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
            if (this.DetachTrackers(out var trackers))
                await this.psfSession.ProcessChangesAsync(trackers);
            await this.psfSession.CompletePendingAsync(waitForCommit, cancellationToken);    // TODO: Resolve issues with non-async operations in groups
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
            await this.psfSession.ReadyToCompletePendingAsync(cancellationToken);
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
            await this.psfSession.WaitForCommitAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public void Dispose() => this.fkvSession.Dispose();

        #endregion IClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> implementation
    }
}
