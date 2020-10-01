// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

namespace PSF.Index
{
    /// <summary>
    /// A session for PSF operations.
    /// </summary>
    public class PSFIndexSession<TProviderData, TRecordId> : IDisposable where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly PSFManager<TProviderData, TRecordId> psfManager;

        private Dictionary<IExecutePSF<TProviderData, TRecordId>, IDisposable> groupSessions = new Dictionary<IExecutePSF<TProviderData, TRecordId>, IDisposable>();

        internal PSFIndexSession(PSFManager<TProviderData, TRecordId> psfMgr, long id)
        {
            this.psfManager = psfMgr;
            this.Id = id;
        }

        internal long Id { get; }

        internal void AddGroup(IExecutePSF<TProviderData, TRecordId> group)
            => this.groupSessions[group] = group.NewSession();

        internal IDisposable GetGroupSession(IExecutePSF<TProviderData, TRecordId> group) => this.groupSessions[group];

        #region PSF Updates
        /// <summary>
        /// Inserts a new PSF key/RecordId, or adds the RecordId to an existing chain
        /// </summary>
        /// <param name="data">The provider's data; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <param name="changeTracker">Tracks changes if this is an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFStatus Upsert(TProviderData data, TRecordId recordId, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.psfManager.Upsert(this, data, recordId, changeTracker);

        /// <summary>
        /// Asynchronously Inserts a new PSF key/RecordId, or adds the RecordId to an existing chain
        /// </summary>
        /// <param name="data">The provider's data; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <param name="changeTracker">Tracks changes if this is an existing Key/RecordId entry</param>
        /// <param name="waitForCommit">True to wait for a checkpoint after the operation</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public ValueTask UpsertAsync(TProviderData data, TRecordId recordId, PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
            => this.psfManager.UpsertAsync(this, data, recordId, changeTracker, waitForCommit, cancellationToken);

        /// <summary>
        /// Updates a PSF key/RecordId entry, possibly by RCU (Read-Copy-Update)
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public PSFStatus Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.psfManager.Update(this, changeTracker);

        /// <summary>
        /// Asynchronously Updates a PSF key/RecordId entry, possibly by RCU (Read-Copy-Update)
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <param name="waitForCommit">True to wait for a checkpoint after the operation</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public ValueTask UpdateAsync(PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
            => this.psfManager.UpdateAsync(this, changeTracker, waitForCommit, cancellationToken);

        /// <summary>
        /// Deletes a PSF key/RecordId entry from the chain, possibly by insertion of a "marked deleted" record
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public PSFStatus Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.psfManager.Delete(this, changeTracker);

        /// <summary>
        /// Deletes a PSF key/RecordId entry from the chain, possibly by insertion of a "marked deleted" record
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <param name="waitForCommit">True to wait for a checkpoint after the operation</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public ValueTask DeleteAsync(PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
            => this.psfManager.DeleteAsync(this, changeTracker, waitForCommit, cancellationToken);
        
        #endregion PSF Updates

        #region Complete pending operations

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
            => this.psfManager.CompletePending(this, spinWait, spinWaitForCommit);

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken cancellationToken = default)
            => this.psfManager.CompletePendingAsync(this, waitForCommit, cancellationToken);

        #endregion Complete pending operations

        #region PSF Queries

        /// <summary>
        /// Does a synchronous scan of a single PSF for records matching a single key
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="key">The <typeparamref name="TPSFKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
            => this.psfManager.QueryPSF(this, psf, key, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of a single PSF for records matching a single key
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="key">The <typeparamref name="TPSFKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
            => this.psfManager.QueryPSFAsync(this, IPSF psf, TPSFKey key, PSFQuerySettings querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of a single PSF for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="keys">The <typeparamref name="TPSFKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
            => this.psfManager.QueryPSF(this, psf, keys, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of a single PSF for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="keys">The <typeparamref name="TPSFKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
            => this.psfManager.QueryPSFAsync(this, psf, keys, querySettings);

#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of one key on each of two PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            => this.psfManager.QueryPSF(this, psf1, key1, psf2, key2, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an synchronous scan of one key on each of two PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            => this.psfManager.QueryPSFAsync(this, psf1, key1, psf2, key2, matchPredicate, querySettings);

#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of two PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct 
            => this.psfManager.QueryPSF(this, psf1, keys1, psf2, keys2, matchPredicate,querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of two PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            => this.psfManager.QueryPSFAsync(this, psf1, keys1, psf2, keys2, matchPredicate,querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of one key on each of three PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="key3">The <typeparamref name="TPSFKey3"/> identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct 
            => this.psfManager.QueryPSF(this, psf1, key1, psf2, key2, psf3, key3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of one key on each of three PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="key3">The <typeparamref name="TPSFKey3"/> identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
            => this.psfManager.QueryPSFAsync(this, psf1, key1, psf2, key2, psf3, key3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of three PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="keys3">The <typeparamref name="TPSFKey3"/>s identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct 
            => this.psfManager.QueryPSF(this, psf1, keys1, psf2, keys2, psf3, keys3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of three PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="keys3">The <typeparamref name="TPSFKey3"/>s identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
            => this.psfManager.QueryPSFAsync(this, psf1, keys1, psf2, keys2, psf3, keys3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        // Power user versions. Anything more complicated than these can be post-processed with LINQ.

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs with the same <typeparamref name="TPSFKey"/> type, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys">An enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the <typeparamref name="TPSFKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct 
            => this.psfManager.QueryPSF(this, psfsAndKeys, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs with the same <typeparamref name="TPSFKey"/> type, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys">An enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the <typeparamref name="TPSFKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
            => this.psfManager.QueryPSFAsync(this, psfsAndKeys, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs on each of two TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct 
            => this.psfManager.QueryPSF(this, psfsAndKeys1, psfsAndKeys2, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs on each of two TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            => this.psfManager.QueryPSFAsync(this, psfsAndKeys1, psfsAndKeys2, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs on each of three TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys3">The third enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct 
            => this.psfManager.QueryPSF(this, psfsAndKeys1, psfsAndKeys2, psfsAndKeys3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs on each of three TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys3">The third enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
            => this.psfManager.QueryPSFAsync(this, psfsAndKeys1, psfsAndKeys2, psfsAndKeys3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        #endregion PSF Queries

        /// <inheritdoc/>
        public void Dispose()
        {
            var sessions = this.groupSessions;
            if (sessions.Count == 0)
                return;
            sessions = Interlocked.CompareExchange(ref this.groupSessions, null, new Dictionary<IExecutePSF<TProviderData, TRecordId>, IDisposable>());
            foreach (var session in sessions.Values)
                session.Dispose();
        }
    }
}
