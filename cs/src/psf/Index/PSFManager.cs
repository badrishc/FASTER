// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

// TODO: Remove PackageId and PackageOutputPath from csproj when this is folded into master
// TODO: Make a new FASTER.PSF.dll

namespace PSF.Index
{
    /// <summary>
    /// The class that manages PSFs. Called internally by the primary FasterKV.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the provider data returned by PSF queries; for the primary FasterKV, it is FasterKVProviderData{TKVKey, TKVValue}</typeparam>
    /// <typeparam name="TRecordId">The type of the Record identifier in the data provider; for the primary FasterKV it is the record's logical address</typeparam>
    public class PSFManager<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly Dictionary<long, IExecutePSF<TProviderData, TRecordId>> psfGroups = new Dictionary<long, IExecutePSF<TProviderData, TRecordId>>();
        private readonly Dictionary<long, PSFIndexSession<TProviderData, TRecordId>> psfSessions = new Dictionary<long, Index.PSFIndexSession<TProviderData, TRecordId>>();

        private readonly ConcurrentDictionary<string, Guid> psfNames = new ConcurrentDictionary<string, Guid>();

        internal bool HasPSFs => this.psfGroups.Count > 0;

        private static long NextGroupId = 0;
        private static long NextSessionId = 0;


        /// <summary>
        /// Create a new session for PSF operations.
        /// </summary>
        public PSFIndexSession<TProviderData, TRecordId> NewSession()
        {
            var sId = Interlocked.Increment(ref NextSessionId) - 1;
            var session = new PSFIndexSession<TProviderData, TRecordId>(this, sId);
            foreach (var group in this.psfGroups.Values)
                session.AddGroup(group);
            lock (this.psfSessions)
                this.psfSessions.Add(sId, session);
            return session;
        }

        internal void ReleaseSession(PSFIndexSession<TProviderData, TRecordId> session)
        {
            lock (this.psfSessions)
                this.psfSessions.Remove(session.Id);
        }

        internal PSFStatus Upsert(PSFIndexSession<TProviderData, TRecordId> psfSession, TProviderData data, TRecordId recordId,
                                  PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: RecordId locking, to ensure consistency of multiple PSFs if the same record is updated
            // multiple times; possibly a single Array<CacheLine>[N] which is locked on TRecordId.GetHashCode % N.

            if (changeTracker is null || changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // This Upsert was an Insert: For the FasterKV Insert fast path, changeTracker is null.
                foreach (var group in this.psfGroups.Values)
                {
                    // Fast Insert path: No IPUCache lookup is done for Inserts, so this is called directly here.
                    var status = group.ExecuteAndStore(psfSession.GetGroupSession(group), data, recordId, PSFExecutePhase.Insert, changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                return PSFStatus.OK;
            }

            // This Upsert was an IPU or RCU
            return this.Update(psfSession, changeTracker);
        }

        private async ValueTask WhenAll(IEnumerable<ValueTask> tasks)
        {
            // Sequential to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            foreach (var task in tasks.Where(task => !task.IsCompletedSuccessfully))
                await task;
        }

        internal async ValueTask UpsertAsync(PSFIndexSession<TProviderData, TRecordId> psfSession, TProviderData data, TRecordId recordId, 
                                             PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
        {
            if (changeTracker is null || changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // This Upsert was an Insert: For the FasterKV Insert fast path, changeTracker is null.
                // Fast Insert path: No IPUCache lookup is done for Inserts, so this is called directly here.
                await WhenAll(this.psfGroups.Values.Select(group => group.ExecuteAsync(psfSession.GetGroupSession(group), data, recordId, PSFExecutePhase.Insert,
                              changeTracker, waitForCommit, cancellationToken)));
                return;
            }

            // This Upsert was an IPU or RCU
            await this.UpdateAsync(psfSession, changeTracker, waitForCommit, cancellationToken);
        }

        internal PSFStatus Update(PSFIndexSession<TProviderData, TRecordId> psfSession, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Update(psfSession.GetGroupSession(group), changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return PSFStatus.OK;
        }

        internal ValueTask UpdateAsync(PSFIndexSession<TProviderData, TRecordId> psfSession, PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit,
                                       CancellationToken cancellationToken) 
            => WhenAll(this.psfGroups.Values.Select(group => group.UpdateAsync(psfSession.GetGroupSession(group), changeTracker, waitForCommit, cancellationToken)));

        internal PSFStatus Delete(PSFIndexSession<TProviderData, TRecordId> psfSession, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Delete(psfSession.GetGroupSession(group), changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return PSFStatus.OK;
        }

        internal ValueTask DeleteAsync(PSFIndexSession<TProviderData, TRecordId> psfSession, PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit,
                                       CancellationToken cancellationToken)
            => WhenAll(this.psfGroups.Values.Select(group => group.DeleteAsync(psfSession.GetGroupSession(group), changeTracker, waitForCommit, cancellationToken)));

        internal bool CompletePending(PSFIndexSession<TProviderData, TRecordId> psfSession, bool spinWait = false, bool spinWaitForCommit = false)
            // TODO parallelize group CompletePending
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.CompletePending(psfSession.GetGroupSession(group), spinWait, spinWaitForCommit) && result);

        internal async ValueTask CompletePendingAsync(PSFIndexSession<TProviderData, TRecordId> psfSession, bool waitForCommit = false, CancellationToken cancellationToken = default)
        {
            foreach (var task in this.psfGroups.Values.Select(group => group.CompletePendingAsync(psfSession.GetGroupSession(group), waitForCommit, cancellationToken))
                .Where(task => !task.IsCompletedSuccessfully))
            {
                await task;
            }
        }

        /// <summary>
        /// Obtains a list of registered PSF names organized by the groups defined in previous RegisterPSF calls.
        /// </summary>
        /// <returns>A list of registered PSF names organized by the groups defined in previous RegisterPSF calls.</returns>
        public string[][] GetRegisteredPSFNames() => throw new NotImplementedException("TODO");

        /// <summary>
        /// Creates an instance of a <see cref="PSFChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.
        /// </summary>
        /// <returns>An instance of a <see cref="PSFChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.</returns>
        public PSFChangeTracker<TProviderData, TRecordId> CreateChangeTracker() 
            => new PSFChangeTracker<TProviderData, TRecordId>(this.psfGroups.Values.Select(group => group.Id));

        /// <summary>
        /// Sets the data for the state of a provider's data record prior to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data prior to the update; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <param name="executePSFsNow">Whether PSFs should be executed now or deferred. Should be 'true' if the provider's value type is an Object,
        ///     because the update will likely change the object's internal values, and thus a deferred 'before' execution will pick up the updated values instead.</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public PSFStatus SetBeforeData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId, bool executePSFsNow)
        {
            changeTracker.SetBeforeData(data, recordId);
            if (executePSFsNow)
            {
                foreach (var group in this.psfGroups.Values)
                {
                    var status = group.GetBeforeKeys(changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                changeTracker.HasBeforeKeys = true;
            }
            return PSFStatus.OK;
        }

        /// <summary>
        /// Sets the data for the state of a provider's data record after to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data after to the update; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public PSFStatus SetAfterData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId)
        {
            changeTracker.SetAfterData(data, recordId);
            return PSFStatus.OK;
        }

        private void AddGroup<TPSFKey>(PSFGroup<TProviderData, TPSFKey, TRecordId> group) where TPSFKey : struct
        {
            var gId = Interlocked.Increment(ref NextGroupId) - 1;
            lock (this.psfGroups)
                this.psfGroups.Add(gId, group);
        }

        private void VerifyIsBlittable<TPSFKey>()
        {
            if (!Utility.IsBlittable<TPSFKey>())
                throw new PSFArgumentException("The PSF Key type must be blittable.");
        }

        private PSF<TPSFKey, TRecordId> GetImplementingPSF<TPSFKey>(IPSF ipsf)
        {
            if (ipsf is null)
                throw new PSFArgumentException($"The PSF cannot be null.");
            var psf = ipsf as PSF<TPSFKey, TRecordId>;
            Guid id = default;
            if (psf is null || !this.psfNames.TryGetValue(psf.Name, out id) || id != psf.Id)
                throw new PSFArgumentException($"The PSF {psf.Name} with Id {(psf is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return psf;
        }

        private void VerifyIsOurPSF(params IPSF[] psfs)
        {
            foreach (var psf in psfs)
            {
                if (psf is null)
                    throw new PSFArgumentException($"The PSF cannot be null.");
                if (!this.psfNames.ContainsKey(psf.Name))
                    throw new PSFArgumentException($"The PSF {psf.Name} is not registered with this FasterKV.");
            }
        }

        private void VerifyIsOurPSF<TPSFKey>(IEnumerable<(IPSF, IEnumerable<TPSFKey>)> psfsAndKeys)
        {
            if (psfsAndKeys is null)
                throw new PSFArgumentException($"The PSF enumerable cannot be null.");
            foreach (var psfAndKeys in psfsAndKeys)
                this.VerifyIsOurPSF(psfAndKeys.Item1);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2, TPSFKey3>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey3>)> psfsAndKeys3)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
            VerifyIsOurPSF(psfsAndKeys3);
        }

        private static void VerifyRegistrationSettings<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings) where TPSFKey : struct
        {
            if (registrationSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings is required");
            if (registrationSettings.LogSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.LogSettings is required");
            if (registrationSettings.CheckpointSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for PSFs
            if (!(registrationSettings.LogSettings.ReadCacheSettings is null) || registrationSettings.LogSettings.CopyReadsToTail)
                throw new PSFArgumentException("PSFs do not support ReadCache or CopyReadsToTail");
        }

        /// <summary>
        /// Register a <see cref="PSF{TPSFKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="def">The PSF definition</param>
        /// <returns>A PSF implementation(</returns>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey> def)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (def is null)
                throw new PSFArgumentException("PSF definition cannot be null");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                if (psfNames.ContainsKey(def.Name))
                    throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, new[] { def }, this.psfGroups.Count);
                AddGroup(group);
                var psf = group[def.Name];
                this.psfNames.TryAdd(psf.Name, psf.Id);
                return psf;
            }
        }

        /// <summary>
        /// Register multiple <see cref="PSF{TPSFKey, TRecordId}"/>s with a vector of definitions.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="defs">The PSF definitions</param>
        /// <returns>A PSF implementation(</returns>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey>[] defs)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (defs is null || defs.Length == 0 || defs.Any(def => def is null) || defs.Length == 0)
                throw new PSFArgumentException("PSF definitions cannot be null or empty");

            // We use stackalloc for speed and can recurse in pending operations, so make sure we don't blow the stack.
            if (defs.Length > PSFConstants.kInvalidPsfOrdinal)
                throw new PSFArgumentException($"There can be no more than {PSFConstants.kInvalidPsfOrdinal} PSFs in a single Group");
            const int maxKeySize = 256;
            if (Utility.GetSize(default(KeyPointer<TPSFKey>)) > maxKeySize)
                throw new PSFArgumentException($"The size of the PSF key can be no more than {maxKeySize} bytes");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                for (var ii = 0; ii < defs.Length; ++ii)
                {
                    var def = defs[ii];
                    if (psfNames.ContainsKey(def.Name))
                        throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                    for (var jj = ii + 1; jj < defs.Length; ++jj)
                    {
                        if (defs[jj].Name == def.Name)
                            throw new PSFArgumentException($"The PSF name {def.Name} cannot be specfied twice");
                    }
                }

                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, defs, this.psfGroups.Count);
                AddGroup(group);
                foreach (var psf in group.PSFs)
                    this.psfNames.TryAdd(psf.Name, psf.Id);
                return group.PSFs;
            }
        }

        private IDisposable GetGroupSession<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, PSF<TPSFKey, TRecordId> psf)
            => psfSession.GetGroupSession(this.psfGroups[psf.GroupId]);

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            foreach (var recordId in psfImpl.Query(this.GetGroupSession(psfSession, psfImpl), key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#if NETSTANDARD21
        internal async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            await foreach (var recordId in psfImpl.QueryAsync(this.GetGroupSession(psfSession, psfImpl), key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                foreach (var recordId in QueryPSF(psfSession, psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#if NETSTANDARD21
        internal async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                await foreach (var recordId in QueryPSFAsync(psfSession, psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psfSession, psf1, key1, querySettings),
                                                      psf2, this.QueryPSF(psfSession, psf2, key2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psfSession, psf1, key1, querySettings),
                                                           psf2, this.QueryPSFAsync(psfSession, psf2, key2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psfSession, psf1, keys1, querySettings), 
                                                      psf2, this.QueryPSF(psfSession, psf2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psfSession, psf1, keys1, querySettings),
                                                           psf2, this.QueryPSFAsync(psfSession, psf2, keys2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psfSession, psf1, key1, querySettings),
                                                      psf2, this.QueryPSF(psfSession, psf2, key2, querySettings),
                                                      psf3, this.QueryPSF(psfSession, psf3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psfSession, psf1, key1, querySettings),
                                                           psf2, this.QueryPSFAsync(psfSession, psf2, key2, querySettings),
                                                           psf3, this.QueryPSFAsync(psfSession, psf3, key3, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psfSession, psf1, keys1, querySettings),
                                                      psf2, this.QueryPSF(psfSession, psf2, keys2, querySettings),
                                                      psf3, this.QueryPSF(psfSession, psf3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if NETSTANDARD21
        inteernal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psfSession, psf1, keys1, querySettings),
                                                           psf2, this.QueryPSFAsync(psfSession, psf2, keys2, querySettings),
                                                           psf3, this.QueryPSFAsync(psfSession, psf3, keys3, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        // Power user versions. Anything more complicated than these can be post-processed with LINQ.

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }

#if NETSTANDARD21
        intenral IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings))),
                                                                  psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings)))},
                                                           matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }
#endif // NETSTANDARD21

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSF(psfSession, tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }

#if NETSTANDARD21
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(PSFIndexSession<TProviderData, TRecordId> psfSession, 
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings))),
                                                                  psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings))),
                                                                  psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(psfSession, tup.psf, tup.keys, querySettings)))},
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
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.GrowIndex() && result);

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a full checkpoint of the FasterKV implementing the group's PSFs.
        /// </summary>
        public bool TakeFullCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint() && result);

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a full checkpoint of the FasterKV implementing the group's PSFs.
        /// </summary>
        public bool TakeFullCheckpoint(CheckpointType checkpointType)
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint(checkpointType) && result);

        /// <summary>
        /// Takes a full (index + log) checkpoint of FASTER asynchronously
        /// </summary>
        public ValueTask<bool> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default) 
            => AggregateResult(this.psfGroups.Values.Select(group => group.TakeFullCheckpointAsync(checkpointType, cancellationToken)));

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a checkpoint of the Index (hashtable) only
        /// </summary>
        public bool TakeIndexCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeIndexCheckpoint() && result);

        /// <summary>
        /// Take a checkpoint of the Index (hashtable) only
        /// </summary>
        public ValueTask<bool> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
            => AggregateResult(this.psfGroups.Values.Select(group => group.TakeIndexCheckpointAsync(cancellationToken)));

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a checkpoint of the hybrid log only
        /// </summary>
        public bool TakeHybridLogCheckpoint() 
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint() && result);

        /// <summary>
        /// Take a checkpoint of the hybrid log only
        /// </summary>
        public bool TakeHybridLogCheckpoint(CheckpointType checkpointType)
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint(checkpointType) && result);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        public ValueTask<bool> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
            => AggregateResult(this.psfGroups.Values.Select(group => group.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken)));

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, complete ongoing checkpoint (spin-wait)
        /// </summary>
        public Task CompleteCheckpointAsync(CancellationToken token = default)
        {
            var tasks = this.psfGroups.Values.Select(group => group.CompleteCheckpointAsync(token).AsTask()).ToArray();
            return Task.WhenAll(tasks);
        }

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, recover from last successful checkpoints
        /// </summary>
        public void Recover()
        {
            foreach (var group in this.psfGroups.Values)
                group.Recover();
        }

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        public void Recover(Guid fullcheckpointToken)
        {
            foreach (var group in this.psfGroups.Values)
                group.Recover(fullcheckpointToken);
        }

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        public void Recover(Guid indexToken, Guid hybridLogToken)
        {
            foreach (var group in this.psfGroups.Values)
                group.Recover(indexToken, hybridLogToken);
        }

        #endregion Checkpoint Operations

        #region Log Operations

        /// <summary>
        /// Flush logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s until their current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void FlushLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
                group.FlushLog(wait);
        }

        /// <summary>
        /// Flush logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public void FlushAndEvictLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
            {
                group.FlushAndEvictLog(wait);
            }
        }

        /// <summary>
        /// Delete logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeLogsFromMemory()
        {
            foreach (var group in this.psfGroups.Values)
                group.DisposeLogFromMemory();
        }
        #endregion Log Operations
    }
}
