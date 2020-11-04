// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

// TODO: Remove PackageId and PackageOutputPath from csproj when this is folded into master

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// The class that manages Groups and their Predicates.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the provider data communicated between the data provider and the Predicates; for the primary FasterKV, it is FasterKVProviderData{TKVKey, TKVValue}</typeparam>
    /// <typeparam name="TRecordId">The type of the Record identifier in the data provider; for the primary FasterKV it is the record's logical address</typeparam>
    public class SubsetIndex<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly Dictionary<long, IExecutePredicate<TProviderData, TRecordId>> groups = new Dictionary<long, IExecutePredicate<TProviderData, TRecordId>>();
        private readonly Dictionary<long, ClientSessionSI<TProviderData, TRecordId>> indexSessions = new Dictionary<long, ClientSessionSI<TProviderData, TRecordId>>();

        private readonly ConcurrentDictionary<string, Guid> predicateNames = new ConcurrentDictionary<string, Guid>();

        private static long NextGroupId = 0;
        private static long NextSessionId = 0;

        /// <summary>
        /// Create a new session for ShubsetIndex operations.
        /// </summary>
        public ClientSessionSI<TProviderData, TRecordId> NewSession()
        {
            var sId = Interlocked.Increment(ref NextSessionId) - 1;
            var session = new ClientSessionSI<TProviderData, TRecordId>(this, sId);
            foreach (var group in this.groups.Values)
                session.AddGroup(group);
            lock (this.indexSessions)
                this.indexSessions.Add(sId, session);
            return session;
        }

        internal void ReleaseSession(ClientSessionSI<TProviderData, TRecordId> session)
        {
            lock (this.indexSessions)
                this.indexSessions.Remove(session.Id);
        }

        internal Status Upsert(ClientSessionSI<TProviderData, TRecordId> indexSession, TProviderData data, TRecordId recordId,
                               ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: RecordId locking, to ensure consistency of multiple Predicate chains if the same record is updated
            // multiple times; possibly a single Array<CacheLine>[N] which is locked on TRecordId.GetHashCode % N.

            if (changeTracker is null || changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // This Upsert was an Insert: For the FasterKV Insert fast path, changeTracker is null.
                foreach (var group in this.groups.Values)
                {
                    // Fast Insert path: No IPUCache lookup is done for Inserts, so this is called directly here.
                    var status = group.ExecuteAndStore(indexSession.GetGroupSession(group), data, recordId, ExecutionPhase.Insert, changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                return Status.OK;
            }

            // This Upsert was an IPU or RCU
            return this.Update(indexSession, changeTracker);
        }

        private async ValueTask WhenAll(IEnumerable<ValueTask> tasks)
        {
            // Sequential to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            foreach (var task in tasks.Where(task => !task.IsCompletedSuccessfully))
                await task;
        }

        internal Status Update(ClientSessionSI<TProviderData, TRecordId> indexSession, ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.groups.Values)
            {
                var status = group.Update(indexSession.GetGroupSession(group), changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        internal ValueTask UpdateAsync(ClientSessionSI<TProviderData, TRecordId> indexSession, ChangeTracker<TProviderData, TRecordId> changeTracker, 
                                       CancellationToken cancellationToken) 
            => WhenAll(this.groups.Values.Select(group => group.UpdateAsync(indexSession.GetGroupSession(group), changeTracker, cancellationToken)));

        internal Status Delete(ClientSessionSI<TProviderData, TRecordId> indexSession, ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.groups.Values)
            {
                var status = group.Delete(indexSession.GetGroupSession(group), changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        internal ValueTask DeleteAsync(ClientSessionSI<TProviderData, TRecordId> indexSession, ChangeTracker<TProviderData, TRecordId> changeTracker,
                                       CancellationToken cancellationToken)
            => WhenAll(this.groups.Values.Select(group => group.DeleteAsync(indexSession.GetGroupSession(group), changeTracker, cancellationToken)));

        internal bool CompletePending(ClientSessionSI<TProviderData, TRecordId> indexSession, bool spinWait = false, bool spinWaitForCommit = false)
            // TODO parallelize group CompletePending
            => this.groups.Values.Aggregate(true, (result, group) => group.CompletePending(indexSession.GetGroupSession(group), spinWait, spinWaitForCommit) && result);

        internal ValueTask CompletePendingAsync(ClientSessionSI<TProviderData, TRecordId> indexSession, bool waitForCommit = false, CancellationToken cancellationToken = default) 
            => WhenAll(this.groups.Values.Select(group => group.CompletePendingAsync(indexSession.GetGroupSession(group), waitForCommit, cancellationToken)));

        internal ValueTask ReadyToCompletePendingAsync(ClientSessionSI<TProviderData, TRecordId> indexSession, CancellationToken cancellationToken = default) 
            => WhenAll(this.groups.Values.Select(group => group.ReadyToCompletePendingAsync(indexSession.GetGroupSession(group), cancellationToken)));

        internal ValueTask WaitForCommitAsync(ClientSessionSI<TProviderData, TRecordId> indexSession, CancellationToken cancellationToken = default) 
            => WhenAll(this.groups.Values.Select(group => group.WaitForCommitAsync(indexSession.GetGroupSession(group), cancellationToken)));

        /// <summary>
        /// Obtains a list of registered Predicate names organized by the groups defined in previous Register calls. TODO: Replace with GetMetadata()
        /// </summary>
        /// <returns>A list of registered Predicate names organized by the groups defined in previous Register calls.</returns>
        public string[][] GetRegisteredPredicateNames() => throw new NotImplementedException("TODO");

        /// <summary>
        /// Creates an instance of a <see cref="ChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.
        /// </summary>
        /// <returns>An instance of a <see cref="ChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.</returns>
        public ChangeTracker<TProviderData, TRecordId> CreateChangeTracker() 
            => new ChangeTracker<TProviderData, TRecordId>(this.groups.Values.Select(group => group.Id));

        /// <summary>
        /// Sets the data for the state of a provider's data record prior to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data prior to the update; will be passed to the Predicate execution</param>
        /// <param name="recordId">The record Id to be stored for any matching Predicates</param>
        /// <param name="executePredicatesNow">Whether Predicates should be executed now or deferred. Should be 'true' if the provider's value type is an Object,
        ///     because the update will likely change the object's internal values, and thus a deferred 'before' execution will pick up the updated values instead.</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status SetBeforeData(ChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId, bool executePredicatesNow)
        {
            changeTracker.SetBeforeData(data, recordId);
            if (executePredicatesNow)
            {
                foreach (var group in this.groups.Values)
                {
                    var status = group.GetBeforeKeys(changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                changeTracker.HasBeforeKeys = true;
            }
            return Status.OK;
        }

        /// <summary>
        /// Sets the data for the state of a provider's data record after to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data after to the update; will be passed to the Predicate execution</param>
        /// <param name="recordId">The record Id to be stored for any matching Predicates</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status SetAfterData(ChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId)
        {
            changeTracker.SetAfterData(data, recordId);
            return Status.OK;
        }

        private void AddGroup<TPKey>(Group<TProviderData, TPKey, TRecordId> group) where TPKey : struct
        {
            var gId = Interlocked.Increment(ref NextGroupId) - 1;
            lock (this.groups)
                this.groups.Add(gId, group);
        }

        private void VerifyIsBlittable<TPKey>()
        {
            if (!Utility.IsBlittable<TPKey>())
                throw new ArgumentExceptionSI("The Predicate Key type must be blittable.");
        }

        private Predicate<TPKey, TRecordId> GetImplementingPredicate<TPKey>(IPredicate iPred)
        {
            if (iPred is null)
                throw new ArgumentExceptionSI($"The Predicate cannot be null.");
            var pred = iPred as Predicate<TPKey, TRecordId>;
            Guid id = default;
            if (pred is null || !this.predicateNames.TryGetValue(pred.Name, out id) || id != pred.Id)
                throw new ArgumentExceptionSI($"The Predicate {pred.Name} with Id {(pred is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return pred;
        }

        private void VerifyIsOurPredicate(params IPredicate[] preds)
        {
            foreach (var pred in preds)
            {
                if (pred is null)
                    throw new ArgumentExceptionSI($"The Predicate cannot be null.");
                if (!this.predicateNames.ContainsKey(pred.Name))
                    throw new ArgumentExceptionSI($"The Predicate {pred.Name} is not registered with this Index.");
            }
        }

        private void VerifyIsOurPredicate<TPKey>(IEnumerable<(IPredicate, IEnumerable<TPKey>)> predsAndKeys)
        {
            if (predsAndKeys is null)
                throw new ArgumentExceptionSI($"The Predicate enumerable cannot be null.");
            foreach (var predAndKeys in predsAndKeys)
                this.VerifyIsOurPredicate(predAndKeys.Item1);
        }

        private void VerifyIsOurPredicate<TPKey1, TPKey2>(IEnumerable<(IPredicate, IEnumerable<TPKey1>)> predAndKeys1,
                                                        IEnumerable<(IPredicate, IEnumerable<TPKey2>)> predAndKeys2)
        {
            VerifyIsOurPredicate(predAndKeys1);
            VerifyIsOurPredicate(predAndKeys2);
        }

        private void VerifyIsOurPredicate<TPKey1, TPKey2, TPKey3>(IEnumerable<(IPredicate, IEnumerable<TPKey1>)> predAndKeys1,
                                                        IEnumerable<(IPredicate, IEnumerable<TPKey2>)> predAndKeys2,
                                                        IEnumerable<(IPredicate, IEnumerable<TPKey3>)> predAndKeys3)
        {
            VerifyIsOurPredicate(predAndKeys1);
            VerifyIsOurPredicate(predAndKeys2);
            VerifyIsOurPredicate(predAndKeys3);
        }

        private static void VerifyRegistrationSettings<TPKey>(RegistrationSettings<TPKey> registrationSettings) where TPKey : struct
        {
            if (registrationSettings is null)
                throw new ArgumentExceptionSI("RegistrationSettings is required");
            if (registrationSettings.LogSettings is null)
                throw new ArgumentExceptionSI("RegistrationSettings.LogSettings is required");
            if (registrationSettings.CheckpointSettings is null)
                throw new ArgumentExceptionSI("RegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for SubsetIndex
            if (registrationSettings.LogSettings.ReadCacheSettings is {} || registrationSettings.LogSettings.CopyReadsToTail)
                throw new ArgumentExceptionSI("SubsetIndex does not support ReadCache or CopyReadsToTail");
        }

        /// <summary>
        /// Register a <see cref="Predicate{TPKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="def">The Predicate definition</param>
        /// <returns>A Predicate implementation(</returns>
        public IPredicate Register<TPKey>(RegistrationSettings<TPKey> registrationSettings, IPredicateDefinition<TProviderData, TPKey> def)
            where TPKey : struct
        {
            this.VerifyIsBlittable<TPKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (def is null)
                throw new ArgumentExceptionSI("Predicate definition cannot be null");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.predicateNames)
            {
                if (predicateNames.ContainsKey(def.Name))
                    throw new ArgumentExceptionSI($"A Predicate named {def.Name} is already registered in another group");
                var group = new Group<TProviderData, TPKey, TRecordId>(registrationSettings, new[] { def }, this.groups.Count);
                AddGroup(group);
                var pred = group[def.Name];
                this.predicateNames.TryAdd(pred.Name, pred.Id);
                return pred;
            }
        }

        /// <summary>
        /// Register multiple <see cref="Predicate{TPKey, TRecordId}"/>s with a vector of definitions.
        /// </summary>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="Predicate{TPKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="defs">The Predicate definitions</param>
        /// <returns>A Predicate implementation(</returns>
        public IPredicate[] Register<TPKey>(RegistrationSettings<TPKey> registrationSettings, IPredicateDefinition<TProviderData, TPKey>[] defs)
            where TPKey : struct
        {
            this.VerifyIsBlittable<TPKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (defs is null || defs.Length == 0 || defs.Any(def => def is null) || defs.Length == 0)
                throw new ArgumentExceptionSI("Predicate definitions cannot be null or empty");

            // We use stackalloc for speed and can recurse in pending operations, so make sure we don't blow the stack.
            if (defs.Length > Constants.kInvalidPredicateOrdinal)
                throw new ArgumentExceptionSI($"There can be no more than {Constants.kInvalidPredicateOrdinal} Predicates in a single Group");
            const int maxKeySize = 256;
            if (Utility.GetSize(default(KeyPointer<TPKey>)) > maxKeySize)
                throw new ArgumentExceptionSI($"The size of the Predicate key can be no more than {maxKeySize} bytes");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.predicateNames)
            {
                for (var ii = 0; ii < defs.Length; ++ii)
                {
                    var def = defs[ii];
                    if (predicateNames.ContainsKey(def.Name))
                        throw new ArgumentExceptionSI($"A Predicate named {def.Name} is already registered in another group");
                    for (var jj = ii + 1; jj < defs.Length; ++jj)
                    {
                        if (defs[jj].Name == def.Name)
                            throw new ArgumentExceptionSI($"The Predicate name {def.Name} cannot be specfied twice");
                    }
                }

                var group = new Group<TProviderData, TPKey, TRecordId>(registrationSettings, defs, this.groups.Count);
                AddGroup(group);
                foreach (var pred in group.Predicates)
                    this.predicateNames.TryAdd(pred.Name, pred.Id);
                return group.Predicates;
            }
        }

        private IDisposable GetGroupSession<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, Predicate<TPKey, TRecordId> pred)
            => indexSession.GetGroupSession(this.groups[pred.GroupId]);

        internal IEnumerable<TRecordId> Query<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, IPredicate pred, TPKey key, QuerySettings querySettings)
            where TPKey : struct
        {
            var predImpl = this.GetImplementingPredicate<TPKey>(pred);
            querySettings ??= QuerySettings.Default;
            foreach (var recordId in predImpl.Query(this.GetGroupSession(indexSession, predImpl), key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#if NETSTANDARD21
        internal async IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, IPredicate pred, TPKey key, QuerySettings querySettings)
            where TPKey : struct
        {
            var predImpl = this.GetImplementingPredicate<TPKey>(pred);
            querySettings ??= QuerySettings.Default;
            await foreach (var recordId in predImpl.QueryAsync(this.GetGroupSession(indexSession, predImpl), key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings)
            where TPKey : struct
        {
            this.VerifyIsOurPredicate(pred);
            querySettings ??= QuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one Read(Key|Address) loop.
            foreach (var key in keys)
            {
                foreach (var recordId in Query(indexSession, pred, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#if NETSTANDARD21
        internal async IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, IPredicate pred, IEnumerable<TPKey> keys, QuerySettings querySettings)
            where TPKey : struct
        {
            this.VerifyIsOurPredicate(pred);
            querySettings ??= QuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one Read(Key|Address) loop.
            foreach (var key in keys)
            {
                await foreach (var recordId in QueryAsync(indexSession, pred, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(pred1, this.Query(indexSession, pred1, key1, querySettings),
                                                      pred2, this.Query(indexSession, pred2, key2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(pred1, this.QueryAsync(indexSession, pred1, key1, querySettings),
                                                           pred2, this.QueryAsync(indexSession, pred2, key2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(pred1, this.Query(indexSession, pred1, keys1, querySettings),
                                                      pred2, this.Query(indexSession, pred2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    QuerySettings querySettings)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(pred1, this.QueryAsync(indexSession, pred1, keys1, querySettings),
                                                           pred2, this.QueryAsync(indexSession, pred2, keys2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                     IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2, pred3);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(pred1, this.Query(indexSession, pred1, key1, querySettings),
                                                      pred2, this.Query(indexSession, pred2, key2, querySettings),
                                                      pred3, this.Query(indexSession, pred3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                     IPredicate pred1, TPKey1 key1,
                     IPredicate pred2, TPKey2 key2,
                     IPredicate pred3, TPKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2, pred3);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(pred1, this.QueryAsync(indexSession, pred1, key1, querySettings),
                                                           pred2, this.QueryAsync(indexSession, pred2, key2, querySettings),
                                                           pred3, this.QueryAsync(indexSession, pred3, key3, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                     IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2, pred3);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(pred1, this.Query(indexSession, pred1, keys1, querySettings),
                                                      pred2, this.Query(indexSession, pred2, keys2, querySettings),
                                                      pred3, this.Query(indexSession, pred3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                     IPredicate pred1, IEnumerable<TPKey1> keys1,
                     IPredicate pred2, IEnumerable<TPKey2> keys2,
                     IPredicate pred3, IEnumerable<TPKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(pred1, pred2, pred3);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(pred1, this.QueryAsync(indexSession, pred1, keys1, querySettings),
                                                           pred2, this.QueryAsync(indexSession, pred2, keys2, querySettings),
                                                           pred3, this.QueryAsync(indexSession, pred3, keys3, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        // Power user versions. Anything more complicated than these can be post-processed with LINQ.

        internal IEnumerable<TRecordId> Query<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct
        {
            this.VerifyIsOurPredicate(predAndKeys);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] { predAndKeys.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey> keys)> predAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey : struct
        {
            this.VerifyIsOurPredicate(predAndKeys);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] { predAndKeys.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(predAndKeys1, predAndKeys2);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {predAndKeys1.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings))),
                                                             predAndKeys2.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
        {
            this.VerifyIsOurPredicate(predAndKeys1, predAndKeys2);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {predAndKeys1.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings))),
                                                                  predAndKeys2.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings)))},
                                                           matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> Query<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(predAndKeys1, predAndKeys2, predAndKeys3);
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {predAndKeys1.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings))),
                                                             predAndKeys2.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings))),
                                                             predAndKeys3.Select(tup => ((IPredicate)tup.pred, this.Query(indexSession, tup.pred, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryAsync<TPKey1, TPKey2, TPKey3>(ClientSessionSI<TProviderData, TRecordId> indexSession, 
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey1> keys)> predAndKeys1,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey2> keys)> predAndKeys2,
                    IEnumerable<(IPredicate pred, IEnumerable<TPKey3> keys)> predAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
            where TPKey1 : struct
            where TPKey2 : struct
            where TPKey3 : struct
        {
            this.VerifyIsOurPredicate(predAndKeys1, predAndKeys2, predAndKeys3);
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {predAndKeys1.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings))),
                                                                  predAndKeys2.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings))),
                                                                  predAndKeys3.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(indexSession, tup.pred, tup.keys, querySettings)))},
                                                           matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }
#endif // NETSTANDARD21

        #region Checkpoint Operations

        private static async ValueTask<bool> AggregateResult(IEnumerable<ValueTask<bool>> valueTasks)
        {
            var success = true;
            foreach (var vt in valueTasks)
                success &= (await vt);
            return success;
        }

        /// <summary>
        /// Grow the hash index
        /// </summary>
        public bool GrowIndex()
            => this.groups.Values.Aggregate(true, (result, group) => group.GrowIndex() && result);

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, take a full checkpoint of the FasterKV implementing the Group's index.
        /// </summary>
        public bool TakeFullCheckpoint()
            => this.groups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint() && result);

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, take a full checkpoint of the FasterKV implementing the group's index.
        /// </summary>
        public bool TakeFullCheckpoint(CheckpointType checkpointType)
            => this.groups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint(checkpointType) && result);

        /// <summary>
        /// Takes a full (index + log) checkpoint of FASTER asynchronously
        /// </summary>
        public ValueTask<bool> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default) 
            => AggregateResult(this.groups.Values.Select(group => group.TakeFullCheckpointAsync(checkpointType, cancellationToken)));

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, take a checkpoint of the Index (hashtable) only
        /// </summary>
        public bool TakeIndexCheckpoint()
            => this.groups.Values.Aggregate(true, (result, group) => group.TakeIndexCheckpoint() && result);

        /// <summary>
        /// Take a checkpoint of the Index (hashtable) only
        /// </summary>
        public ValueTask<bool> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
            => AggregateResult(this.groups.Values.Select(group => group.TakeIndexCheckpointAsync(cancellationToken)));

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, take a checkpoint of the hybrid log only
        /// </summary>
        public bool TakeHybridLogCheckpoint() 
            => this.groups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint() && result);

        /// <summary>
        /// Take a checkpoint of the hybrid log only
        /// </summary>
        public bool TakeHybridLogCheckpoint(CheckpointType checkpointType)
            => this.groups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint(checkpointType) && result);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        public ValueTask<bool> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
            => AggregateResult(this.groups.Values.Select(group => group.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken)));

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, complete ongoing checkpoint (spin-wait)
        /// </summary>
        public Task CompleteCheckpointAsync(CancellationToken token = default)
        {
            var tasks = this.groups.Values.Select(group => group.CompleteCheckpointAsync(token).AsTask()).ToArray();
            return Task.WhenAll(tasks);
        }

        /// <summary>
        /// For each <see cref="Group{TProviderData, TPKey, TRecordId}"/>, recover from last successful checkpoints
        /// </summary>
        public void Recover()
        {
            foreach (var group in this.groups.Values)
                group.Recover();
        }

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        public void Recover(Guid fullcheckpointToken)
        {
            foreach (var group in this.groups.Values)
                group.Recover(fullcheckpointToken);
        }

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        public void Recover(Guid indexToken, Guid hybridLogToken)
        {
            foreach (var group in this.groups.Values)
                group.Recover(indexToken, hybridLogToken);
        }

        #endregion Checkpoint Operations

        #region Log Operations

        /// <summary>
        /// Flush logs for all <see cref="Group{TProviderData, TPKey, TRecordId}"/>s until their current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait)
        {
            foreach (var group in this.groups.Values)
                group.Flush(wait);
        }

        /// <summary>
        /// Flush logs for all <see cref="Group{TProviderData, TPKey, TRecordId}"/>s and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public void FlushAndEvict(bool wait)
        {
            foreach (var group in this.groups.Values)
            {
                group.FlushAndEvict(wait);
            }
        }

        /// <summary>
        /// Delete logs for all <see cref="Group{TProviderData, TPKey, TRecordId}"/>s entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeFromMemory()
        {
            foreach (var group in this.groups.Values)
                group.DisposeFromMemory();
        }
        #endregion Log Operations
    }
}
