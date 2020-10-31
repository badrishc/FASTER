// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// A session for SubsetHashIndex operations.
    /// </summary>
    public class ClientSessionSHI<TProviderData, TRecordId> : IDisposable where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly SubsetHashIndex<TProviderData, TRecordId> subsetHashIndex;

        private Dictionary<IExecutePredicate<TProviderData, TRecordId>, IDisposable> groupSessions = new Dictionary<IExecutePredicate<TProviderData, TRecordId>, IDisposable>();

        internal ClientSessionSHI(SubsetHashIndex<TProviderData, TRecordId> subsetHashIndex, long id)
        {
            this.subsetHashIndex = subsetHashIndex;
            this.Id = id;
        }

        internal long Id { get; }

        internal void AddGroup(IExecutePredicate<TProviderData, TRecordId> group)
            => this.groupSessions[group] = group.NewSession();

        internal IDisposable GetGroupSession(IExecutePredicate<TProviderData, TRecordId> group) => this.groupSessions[group];

        #region SubsetHashIndex Updates
        /// <summary>
        /// Inserts a new Predicate key/RecordId, or adds the RecordId to an existing chain
        /// </summary>
        /// <param name="data">The provider's data; will be passed to the Predicate execution</param>
        /// <param name="recordId">The record Id to be stored for any matching Predicates</param>
        /// <param name="changeTracker">Tracks changes if this is an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(TProviderData data, TRecordId recordId, ChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.subsetHashIndex.Upsert(this, data, recordId, changeTracker);

        /// <summary>
        /// Updates a Predicate key/RecordId entry, possibly by RCU (Read-Copy-Update)
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status Update(ChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.subsetHashIndex.Update(this, changeTracker);

        /// <summary>
        /// Asynchronously Updates a Predicate key/RecordId entry, possibly by RCU (Read-Copy-Update)
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns>An awaitable task</returns>
        public ValueTask UpdateAsync(ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken)
            => this.subsetHashIndex.UpdateAsync(this, changeTracker, cancellationToken);

        /// <summary>
        /// Deletes a Predicate key/RecordId entry from the chain, possibly by insertion of a "marked deleted" record
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status Delete(ChangeTracker<TProviderData, TRecordId> changeTracker)
            => this.subsetHashIndex.Delete(this, changeTracker);

        /// <summary>
        /// Deletes a Predicate key/RecordId entry from the chain, possibly by insertion of a "marked deleted" record
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns>An awaitable task</returns>
        public ValueTask DeleteAsync(ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken)
            => this.subsetHashIndex.DeleteAsync(this, changeTracker, cancellationToken);

        #endregion SubsetHashIndex Updates

        #region Complete pending operations

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
            => this.subsetHashIndex.CompletePending(this, spinWait, spinWaitForCommit);

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken cancellationToken = default)
            => this.subsetHashIndex.CompletePendingAsync(this, waitForCommit, cancellationToken);

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        public ValueTask ReadyToCompletePendingAsync(CancellationToken cancellationToken = default)
            => this.subsetHashIndex.ReadyToCompletePendingAsync(this, cancellationToken);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        public ValueTask WaitForCommitAsync(CancellationToken cancellationToken = default)
            => this.subsetHashIndex.WaitForCommitAsync(this, cancellationToken);

        #endregion Complete pending operations

        #region SubsetHashIndex Queries

        /// <summary>
        /// Does a synchronous scan of a single Predicate chain for records matching a single key
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred">The Predicate to be queried</param>
        /// <param name="key">The <typeparamref name="TPKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the Predicate query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public IEnumerable<TRecordId> Query<TPKey>(IPredicate pred, TPKey key, QuerySettings querySettings = null)
            where TPKey : struct
            => this.subsetHashIndex.Query(this, pred, key, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of a single Predicate chain for records matching a single key
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred">The Predicate to be queried</param>
        /// <param name="key">The <typeparamref name="TPKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(IPredicate pred, TPKey key, QuerySettings querySettings = null)
            where TPKey : struct
            => this.subsetHashIndex.QueryAsync(this, pred, key, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of a single Predicate chain for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred">The Predicate to be queried</param>
        /// <param name="keys">The <typeparamref name="TPKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public IEnumerable<TRecordId> Query<TPKey>(IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings = null)
            where TPKey : struct
            => this.subsetHashIndex.Query(this, pred, keys, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of a single Predicate chain for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred">The Predicate to be queried</param>
        /// <param name="keys">The <typeparamref name="TPKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings = null)
            where TPKey : struct
            => this.subsetHashIndex.QueryAsync(this, pred, keys, querySettings);

#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of one key on each of two Predicates, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="key1">The <typeparamref name="TPKey1"/> identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="key2">The <typeparamref name="TPKey2"/> identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2>(
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.subsetHashIndex.Query(this, pred1, key1, pred2, key2, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an synchronous scan of one key on each of two Predicates, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="key1">The <typeparamref name="TPKey1"/> identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="key2">The <typeparamref name="TPKey2"/> identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.subsetHashIndex.QueryAsync(this, pred1, key1, pred2, key2, matchPredicate, querySettings);

#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of two Predicates, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPKey1"/>s identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="keys2">The <typeparamref name="TPKey2"/>s identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2>(
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct 
            => this.subsetHashIndex.Query(this, pred1, keys1, pred2, keys2, matchPredicate,querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of two Predicates, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPKey1"/>s identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="keys2">The <typeparamref name="TPKey2"/>s identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.subsetHashIndex.QueryAsync(this, pred1, keys1, pred2, keys2, matchPredicate,querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of one key on each of three Predicates, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="pred3">The third Predicate to be queried</param>
        /// <param name="key1">The <typeparamref name="TPKey1"/> identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="key2">The <typeparamref name="TPKey2"/> identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="key3">The <typeparamref name="TPKey3"/> identifying the records to be retrieved from <paramref name="pred3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                     IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct 
            => this.subsetHashIndex.Query(this, pred1, key1, pred2, key2, pred3, key3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of one key on each of three Predicates, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="pred3">The third Predicate to be queried</param>
        /// <param name="key1">The <typeparamref name="TPKey1"/> identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="key2">The <typeparamref name="TPKey2"/> identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="key3">The <typeparamref name="TPKey3"/> identifying the records to be retrieved from <paramref name="pred3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                     IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.subsetHashIndex.QueryAsync(this, pred1, key1, pred2, key2, pred3, key3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of three Predicates, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="pred3">The third Predicate to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPKey1"/>s identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="keys2">The <typeparamref name="TPKey2"/>s identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="keys3">The <typeparamref name="TPKey3"/>s identifying the records to be retrieved from <paramref name="pred3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                     IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct 
            => this.subsetHashIndex.Query(this, pred1, keys1, pred2, keys2, pred3, keys3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of three Predicates, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="pred1">The first Predicate to be queried</param>
        /// <param name="pred2">The second Predicate to be queried</param>
        /// <param name="pred3">The third Predicate to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPKey1"/>s identifying the records to be retrieved from <paramref name="pred1"/></param>
        /// <param name="keys2">The <typeparamref name="TPKey2"/>s identifying the records to be retrieved from <paramref name="pred2"/></param>
        /// <param name="keys3">The <typeparamref name="TPKey3"/>s identifying the records to be retrieved from <paramref name="pred3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                     IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.subsetHashIndex.QueryAsync(this, pred1, keys1, pred2, keys2, pred3, keys3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        // Power user versions. Anything more complicated than these can be post-processed with LINQ.

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple Predicates with the same <typeparamref name="TPKey"/> type, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys">An enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the <typeparamref name="TPKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the Query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct 
            => this.subsetHashIndex.Query(this, predAndKeys, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple Predicates with the same <typeparamref name="TPKey"/> type, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys">An enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the <typeparamref name="TPKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct
            => this.subsetHashIndex.QueryAsync(this, predAndKeys, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple Predicates on each of two TPKey types, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys1">The first enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys2">The second enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct 
            => this.subsetHashIndex.Query(this, predAndKeys1, predAndKeys2, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple Predicates on each of two TPKey types, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys1">The first enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys2">The second enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the Predicate query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            => this.subsetHashIndex.QueryAsync(this, predAndKeys1, predAndKeys2, matchPredicate, querySettings);
#endif // NETSTANDARD21

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple Predicates on each of three TPKey types, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys1">The first enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys2">The second enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys3">The third enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct 
            => this.subsetHashIndex.Query(this, predAndKeys1, predAndKeys2, predAndKeys3, matchPredicate, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple Predicates on each of three TPKey types, returning records matching any of those keys,
        ///     with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPKey1">The type of the key returned from the first set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey2">The type of the key returned from the second set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPKey3">The type of the key returned from the third set of <see cref="Predicate{TPKey, TRecordId}"/>s</typeparam>
        /// <param name="predAndKeys1">The first enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys2">The second enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="predAndKeys3">The third enumeration of tuples containing a <see cref="Predicate{TPKey, TRecordId}"/> and the TPKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which Predicates are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the Predicate keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
            => this.subsetHashIndex.QueryAsync(this, predAndKeys1, predAndKeys2, predAndKeys3, matchPredicate, querySettings);
#endif // NETSTANDARD21

        #endregion SubsetHashIndex Queries

        /// <inheritdoc/>
        public void Dispose()
        {
            var sessions = this.groupSessions;
            if (sessions.Count == 0)
                return;
            sessions = Interlocked.CompareExchange(ref this.groupSessions, null, new Dictionary<IExecutePredicate<TProviderData, TRecordId>, IDisposable>());
            foreach (var session in sessions.Values)
                session.Dispose();
        }
    }
}
