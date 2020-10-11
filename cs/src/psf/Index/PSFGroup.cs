﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

extern alias FasterCore;

using FC = FasterCore::FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace PSF.Index
{
    /// <summary>
    /// A group of <see cref="PSF{TPSFKey, TRecordId}"/>s. Ideally, most records in the group will either match all
    /// PSFs or none, for efficient use of log space.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the wrapper for the provider's data (obtained from TRecordId)</typeparam>
    /// <typeparam name="TPSFKey">The type of the key returned by the Predicate and stored in the secondary FasterKV instance</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    internal class PSFGroup<TProviderData, TPSFKey, TRecordId> : IExecutePSF<TProviderData, TRecordId>, IQueryPSF<TPSFKey, TRecordId>, IEquatable<PSFGroup<TProviderData, TPSFKey, TRecordId>>
        where TPSFKey : struct
        where TRecordId : struct, IComparable<TRecordId>
    {
        internal FC.FasterKV<TPSFKey, TRecordId> fht;
        internal IPSFDefinition<TProviderData, TPSFKey>[] psfDefinitions;
        private readonly PSFRegistrationSettings<TPSFKey> regSettings;

        /// <summary>
        /// ID of the group (used internally only)
        /// </summary>
        public long Id { get; }

        private readonly FC.CheckpointSettings checkpointSettings;
        private readonly int keyPointerSize = FC.Utility.GetSize(default(KeyPointer<TPSFKey>));
        private readonly int recordIdSize = (Utility.GetSize(default(TRecordId)) + sizeof(long) - 1) & ~(sizeof(long) - 1);

        /// <summary>
        /// The list of <see cref="PSF{TPSFKey, TRecordId}"/>s in this group
        /// </summary>
        public PSF<TPSFKey, TRecordId>[] PSFs { get; private set; }

        private int PSFCount => this.PSFs.Length;

        private readonly IFasterEqualityComparer<TPSFKey> userKeyComparer;
        private readonly KeyAccessor<TPSFKey> keyAccessor;

        private readonly SectorAlignedBufferPool bufferPool;

        // Override equivalence testing for set membership

        /// <inheritdoc/>
        public override int GetHashCode() => this.Id.GetHashCode();

        /// <inheritdoc/>
        public override bool Equals(object obj) => this.Equals(obj as PSFGroup<TProviderData, TPSFKey, TRecordId>);

        /// <inheritdoc/>
        public bool Equals(PSFGroup<TProviderData, TPSFKey, TRecordId> other) => !(other is null) && this.Id == other.Id;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="regSettings">Optional registration settings</param>
        /// <param name="defs">PSF definitions</param>
        /// <param name="id">The ordinal of this PSFGroup in the <see cref="PSFManager{TProviderData, TRecordId}"/>'s
        /// PSFGroup list.</param>
        public PSFGroup(PSFRegistrationSettings<TPSFKey> regSettings, IPSFDefinition<TProviderData, TPSFKey>[] defs, long id)
        {
            this.psfDefinitions = defs;
            this.Id = id;
            this.regSettings = regSettings;
            this.userKeyComparer = GetUserKeyComparer();

            this.PSFs = defs.Select((def, ord) => new PSF<TPSFKey, TRecordId>(this.Id, ord, def.Name, this)).ToArray();
            this.keyAccessor = new KeyAccessor<TPSFKey>(this.userKeyComparer, this.PSFCount, this.keyPointerSize);

            this.checkpointSettings = regSettings?.CheckpointSettings;
            this.fht = new FasterKV<TPSFKey, TRecordId>(
                    regSettings.HashTableSize, regSettings.LogSettings, this.checkpointSettings, null /*SerializerSettings*/,
                    new CompositeKey<TPSFKey>.UnusedKeyComparer(),
                    new VariableLengthStructSettings<TPSFKey, TRecordId>
                    {
                        keyLength = new CompositeKey<TPSFKey>.VarLenLength(this.keyPointerSize, this.PSFCount)
                    }
                )
            { PsfKeyAccessor = keyAccessor };

            this.bufferPool = this.fht.hlog.bufferPool;
        }

        /// <inheritdoc/>
        public IDisposable NewSession()
            => this.fht.NewSession<PSFInput<TPSFKey>, PSFOutput<TPSFKey, TRecordId>, PSFContext, PSFSecondaryFunctions<TPSFKey, TRecordId>>(
                    new PSFSecondaryFunctions<TPSFKey, TRecordId>(), threadAffinitized: this.regSettings.ThreadAffinitized);

        private ClientSession<TPSFKey, TRecordId, PSFInput<TPSFKey>, PSFOutput<TPSFKey, TRecordId>, PSFContext, PSFSecondaryFunctions<TPSFKey, TRecordId>> ToFkvSession(IDisposable sessionObj)
            => sessionObj is ClientSession<TPSFKey, TRecordId, PSFInput<TPSFKey>, PSFOutput<TPSFKey, TRecordId>, PSFContext, PSFSecondaryFunctions<TPSFKey, TRecordId>> session
                ? session
                : throw new PSFInvalidOperationException("Expected a FASTER ClientSession");

        private IFasterEqualityComparer<TPSFKey> GetUserKeyComparer()
        {
            if (!(this.regSettings.KeyComparer is null))
                return this.regSettings.KeyComparer;
            if (typeof(IFasterEqualityComparer<TPSFKey>).IsAssignableFrom(typeof(TPSFKey)))
                return new TPSFKey() as IFasterEqualityComparer<TPSFKey>;

            Console.WriteLine(
                $"***WARNING*** Creating default FASTER key equality comparer based on potentially slow {nameof(EqualityComparer<TPSFKey>)}." +
                $" To avoid this, provide a comparer in {nameof(PSFRegistrationSettings<TPSFKey>)}.{nameof(PSFRegistrationSettings<TPSFKey>.KeyComparer)}," +
                $" or make {typeof(TPSFKey).Name} implement the interface {nameof(IFasterEqualityComparer<TPSFKey>)}");
            return FasterEqualityComparer.Get<TPSFKey>();
        }

        /// <summary>
        /// Returns the named <see cref="PSF{TPSFKey, TRecordId}"/> from the PSFs list.
        /// </summary>
        /// <param name="name">The name of the <see cref="PSF{TPSFKey, TRecordId}"/>; unique among all groups</param>
        /// <returns></returns>
        public PSF<TPSFKey, TRecordId> this[string name]
            => Array.Find(this.PSFs, psf => psf.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
                ?? throw new PSFArgumentException("PSF not found");

        private unsafe void StoreKeys(ref GroupCompositeKey keys, byte* keysPtr, int keysLen)
        {
            var poolKeyMem = this.bufferPool.Get(keysLen);
            Buffer.MemoryCopy(keysPtr, poolKeyMem.GetValidPointer(), keysLen, keysLen);
            keys.Set(poolKeyMem);
        }

        internal unsafe void MarkChanges(ref GroupCompositeKeyPair keysPair)
        {
            ref GroupCompositeKey before = ref keysPair.Before;
            ref GroupCompositeKey after = ref keysPair.After;
            ref CompositeKey<TPSFKey> beforeCompositeKey = ref before.CastToKeyRef<CompositeKey<TPSFKey>>();
            ref CompositeKey<TPSFKey> afterCompositeKey = ref after.CastToKeyRef<CompositeKey<TPSFKey>>();
            for (var ii = 0; ii < this.PSFCount; ++ii)
            {
                ref KeyPointer<TPSFKey> beforeKeyPointer = ref beforeCompositeKey.GetKeyPointerRef(ii, this.keyPointerSize);
                ref KeyPointer<TPSFKey> afterKeyPointer = ref afterCompositeKey.GetKeyPointerRef(ii, this.keyPointerSize);

                var beforeIsNull = beforeKeyPointer.IsNull;
                var afterIsNull = afterKeyPointer.IsNull;
                var keysEqual = !beforeIsNull && !afterIsNull && beforeKeyPointer.Key.Equals(afterKeyPointer.Key);

                // IsNull is already set in PSFGroup.ExecuteAndStore.
                if ((beforeIsNull != afterIsNull) || !keysEqual)
                    keysPair.HasChanges = true;
                if (!beforeIsNull && (afterIsNull || !keysEqual))
                {
                    afterKeyPointer.IsUnlinkOld = true;
                    keysPair.HasChanges = true;
                }
                if (!afterIsNull && (beforeIsNull || !keysEqual))
                {
                    afterKeyPointer.IsLinkNew = true;
                    keysPair.HasChanges = true;
                }
            }
        }

        /// <inheritdoc/>
        internal unsafe Status ExecuteAndStore(IDisposable sessionObj, TProviderData providerData, TRecordId recordId, PSFExecutePhase phase,
                                             PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // Note: stackalloc is safe because PendingContext or PSFChangeTracker will copy it to the bufferPool
            // if needed. On the Insert fast path, we don't want any allocations otherwise; changeTracker is null.
            var keyMemLen = this.keyPointerSize * this.PSFCount;
            var keyBytes = stackalloc byte[keyMemLen];
            var anyMatch = false;

            for (var ii = 0; ii < this.PSFCount; ++ii)
            {
                ref KeyPointer<TPSFKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPSFKey>>(keyBytes + ii * this.keyPointerSize);
                keyPointer.PreviousAddress = Constants.kInvalidAddress;
                keyPointer.PsfOrdinal = (byte)ii;

                var key = this.psfDefinitions[ii].Execute(providerData);
                keyPointer.IsNull = !key.HasValue;
                if (key.HasValue)
                {
                    keyPointer.Key = key.Value;
                    anyMatch = true;
                }
            }

            if (!anyMatch && phase == PSFExecutePhase.Insert)
                return Status.OK;

            ref CompositeKey<TPSFKey> compositeKey = ref Unsafe.AsRef<CompositeKey<TPSFKey>>(keyBytes);
            var input = new PSFInput<TPSFKey>(this.Id, 0);
            var value = recordId;

            int groupOrdinal = -1;
            if (!(changeTracker is null))
            {
                value = changeTracker.BeforeRecordId;
                if (phase == PSFExecutePhase.PreUpdate)
                {
                    // Get a free group ref and store the "before" values.
                    ref GroupCompositeKeyPair groupKeysPair = ref changeTracker.FindGroupRef(this.Id);
                    StoreKeys(ref groupKeysPair.Before, keyBytes, keyMemLen);
                    return Status.OK;
                }

                if (phase == PSFExecutePhase.PostUpdate)
                {
                    // TODOtest: If not found, this is a new group added after the PreUpdate was done, so handle this as an insert.
                    if (!changeTracker.FindGroup(this.Id, out groupOrdinal))
                    {
                        phase = PSFExecutePhase.Insert;
                    }
                    else
                    {
                        ref GroupCompositeKeyPair groupKeysPair = ref changeTracker.GetGroupRef(groupOrdinal);
                        StoreKeys(ref groupKeysPair.After, keyBytes, keyMemLen);
                        this.MarkChanges(ref groupKeysPair);
                        // TODOtest: In debug, for initial dev, follow chains to assert the values match what is in the record's compositeKey
                        if (!groupKeysPair.HasChanges)
                            return Status.OK;
                    }
                }

                // We don't need to do anything here for Delete.
            }

            var session = this.ToFkvSession(sessionObj);
            var lsn = session.ctx.serialNum + 1;
            var context = new PSFContext { Functions = session.functions };
            return phase switch
            {
                PSFExecutePhase.Insert => session.PsfInsert(ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, ref context, lsn),
                PSFExecutePhase.PostUpdate => session.PsfUpdate(ref changeTracker.GetGroupRef(groupOrdinal),
                                                                ref value, ref input, ref context, lsn, changeTracker),
                PSFExecutePhase.Delete => session.PsfDelete(ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, ref context, lsn,
                                                                changeTracker),
                _ => throw new PSFInternalErrorException("Unknown PSF execution Phase {phase}")
            };
        }

        // Async version of ExecuteAndStore; called when we expect the operation to actually access the fkv, not just store the keys.
        internal async ValueTask ExecuteAsync(IDisposable sessionObj, TProviderData providerData, TRecordId recordId, PSFExecutePhase phase,
                                              PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
        {
            var session = this.ToFkvSession(sessionObj);
            var status = this.ExecuteAndStore(sessionObj, providerData, recordId, phase, changeTracker);
            if (status == Status.PENDING)
                await session.CompletePendingAsync(waitForCommit, cancellationToken);
        }

        /// <inheritdoc/>
        public Status GetBeforeKeys(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.HasBeforeKeys)
                return Status.OK;

            // Obtain the "before" values; this does no FKV operations so the session is not used. TODOcache: try to find TRecordId in the IPUCache first.
            return ExecuteAndStore(sessionObj: null, changeTracker.BeforeData, default, PSFExecutePhase.PreUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public Status Update(IDisposable sessionObj, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // RMW did not find the record so did an insert. Go through Insert logic here.
                return this.ExecuteAndStore(sessionObj, changeTracker.BeforeData, changeTracker.BeforeRecordId, PSFExecutePhase.Insert, changeTracker:null);
            }

            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                if (changeTracker.UpdateOp == UpdateOperation.RCU)
                {
                    // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                }
                else
                {
                    // TODOcache: Try to splice in-place; or just copy its keys into changeTracker.Before.
                }
            }
            else
            {
                if (this.GetBeforeKeys(changeTracker) != Status.OK)
                {
                    // TODOerr: handle errors from GetBeforeKeys
                }
            }
            return this.ExecuteAndStore(sessionObj, changeTracker.AfterData, default, PSFExecutePhase.PostUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public ValueTask UpdateAsync(IDisposable sessionObj, PSFChangeTracker<TProviderData, TRecordId> changeTracker, bool waitForCommit, CancellationToken cancellationToken)
        {
            if (changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // RMW did not find the record so did an insert. Go through Insert logic here.
                return this.ExecuteAsync(sessionObj, changeTracker.BeforeData, changeTracker.BeforeRecordId, PSFExecutePhase.Insert, changeTracker: null, waitForCommit, cancellationToken);
            }

            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                if (changeTracker.UpdateOp == UpdateOperation.RCU)
                {
                    // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                }
                else
                {
                    // TODOcache: Try to splice in-place; or just copy its keys into changeTracker.Before.
                }
            }
            else
            {
                if (this.GetBeforeKeys(changeTracker) != Status.OK)
                {
                    // TODOerr: handle errors from GetBeforeKeys
                }
            }
            return this.ExecuteAsync(sessionObj, changeTracker.AfterData, default, PSFExecutePhase.PostUpdate, changeTracker, waitForCommit, cancellationToken);
        }

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        public Status Delete(IDisposable sessionObj, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                // If the latter, we can bypass ExecuteAndStore's PSF-execute loop
            }
            return this.ExecuteAndStore(sessionObj, changeTracker.BeforeData, default, PSFExecutePhase.Delete, changeTracker);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe PSFInput<TPSFKey> MakeQueryInput(int psfOrdinal, ref TPSFKey key)
        {
            // Putting the query key in PSFInput is necessary because iterator functions cannot contain unsafe code or have
            // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var psfInput = new PSFInput<TPSFKey>(this.Id, psfOrdinal);
            psfInput.SetQueryKey(this.bufferPool, this.keyAccessor, ref key);
            return psfInput;
        }

        /// <inheritdoc/>
        public unsafe IEnumerable<TRecordId> Query(IDisposable sessionObj, int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings)
            => Query(sessionObj, MakeQueryInput(psfOrdinal, ref key), querySettings);

        private IEnumerable<TRecordId> Query(IDisposable sessionObj, PSFInput<TPSFKey> input, PSFQuerySettings querySettings)
        {
            // TODOperf: if there are multiple PSFs within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPSFKeys and their indexes in queryKeyPtr. Also consider
            // having TPSFKeys[] for a single PSF walk through in parallel, so the FHT log memory access is sequential.
            var output = new PSFOutput<TPSFKey, TRecordId>(this.keyAccessor);
            var session = this.ToFkvSession(sessionObj);
            var context = new PSFContext { Functions = session.functions };
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                // Because we traverse the chain, we must wait for any pending read operations to complete.
                // TODOperf: See if there is a better solution than spinWaiting in CompletePending.
                Status status = session.PsfReadKey(ref input.QueryKeyRef, ref input, ref output, ref context, session.ctx.serialNum + 1);
                if (querySettings.IsCanceled)
                    yield break;
                if (status == Status.PENDING)
                    session.CompletePending(spinWait: true);
                if (status != Status.OK)    // TODOerr: check other status
                    yield break;

                if (output.Tombstone)
                    deadRecs.Add(output.RecordId);
                else
                    yield return output.RecordId;

                do
                {
                    input.ReadLogicalAddress = output.PreviousLogicalAddress;
                    status = session.PsfReadAddress(ref input, ref output, ref context, session.ctx.serialNum + 1);
                    if (status == Status.PENDING)
                        session.CompletePending(spinWait: true);
                    if (querySettings.IsCanceled)
                        yield break;
                    if (status != Status.OK)    // TODOerr: check other status
                        yield break;

                    if (deadRecs.IsDead(output.RecordId, output.Tombstone))
                        continue;

                    yield return output.RecordId;
                } while (output.PreviousLogicalAddress != Constants.kInvalidAddress);
            }
            finally
            {
                input.Dispose();
            }
        }

#if NETSTANDARD21
        /// <inheritdoc/>
        public unsafe IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings)
            => QueryAsync(sessionObj, MakeQueryInput(psfOrdinal, ref key), querySettings);

        private async IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, PSFInput<TPSFKey> input, PSFQuerySettings querySettings)
        {
            // TODOperf: if there are multiple PSFs within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPSFKeys and their indexes in queryKeyPtr. Also consider
            // having TPSFKeys[] for a single PSF walk through in parallel, so the FHT log memory access is sequential.
            var output = new PSFOutput<TPSFKey, TRecordId>(this.keyAccessor);
            var session = this.ToFkvSession(sessionObj);
            var context = new PSFContext { Functions = session.functions };
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                // Because we traverse the chain, we must wait for any pending read operations to complete.
                var readAsyncResult = await session.PsfReadKeyAsync(ref input.QueryKeyRef, ref input, ref output, ref context, session.ctx.serialNum + 1, querySettings);
                if (querySettings.IsCanceled)
                    yield break;
                var (status, _) = readAsyncResult.Complete();
                if (status != Status.OK)    // TODOerr: check other status
                    yield break;

                if (output.Tombstone)
                    deadRecs.Add(output.RecordId);
                else
                    yield return output.RecordId;

                do
                {
                    input.ReadLogicalAddress = output.PreviousLogicalAddress;
                    readAsyncResult = await session.PsfReadAddressAsync(ref input, ref output, ref context, session.ctx.serialNum + 1, querySettings);
                    if (querySettings.IsCanceled)
                        yield break;
                    (status, _) = readAsyncResult.Complete();
                    if (status != Status.OK)    // TODOerr: check other status
                        yield break;

                    if (deadRecs.IsDead(output.RecordId, output.Tombstone))
                        continue;

                    yield return output.RecordId;
                } while (output.PreviousLogicalAddress != Constants.kInvalidAddress);
            }
            finally
            {
                this.SecondarySessions.ReleaseSession(session);
                input.Dispose();
            }
        }
#endif

        /// <inheritdoc/>
        public bool CompletePending(bool spinWait, bool spinWaitForCommit);     // TODO: Resolve issues with non-async operations in groups

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit, CancellationToken token);     // TODO: Resolve issues with non-async operations in groups

        internal ValueTask ReadyToCompletePendingAsync(CancellationToken cancellationToken);

        internal ValueTask WaitForCommitAsync(CancellationToken cancellationToken);

        #region Checkpoint Operations
        /// <inheritdoc/>
        public bool GrowIndex() => this.fht.GrowIndex();   // TODO fht ==> fkv

        /// <inheritdoc/>
        public bool TakeFullCheckpoint() => this.fht.TakeFullCheckpoint(out _);

        /// <inheritdoc/>
        public bool TakeFullCheckpoint(CheckpointType checkpointType) => this.fht.TakeFullCheckpoint(out _, checkpointType);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
            => (await this.fht.TakeFullCheckpointAsync(checkpointType, cancellationToken)).success;

        /// <inheritdoc/>
        public bool TakeIndexCheckpoint() => this.fht.TakeIndexCheckpoint(out _);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default) 
            => (await this.fht.TakeIndexCheckpointAsync(cancellationToken)).success;

        /// <inheritdoc/>
        public bool TakeHybridLogCheckpoint() => this.fht.TakeHybridLogCheckpoint(out _);

        /// <inheritdoc/>
        public bool TakeHybridLogCheckpoint(CheckpointType checkpointType) => this.fht.TakeHybridLogCheckpoint(out _, checkpointType);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default) 
            => (await this.fht.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken)).success;

        /// <inheritdoc/>
        public ValueTask CompleteCheckpointAsync(CancellationToken token = default) => this.fht.CompleteCheckpointAsync(token);

        /// <inheritdoc/>
        public void Recover() => this.fht.Recover();

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        public void Recover(Guid fullcheckpointToken) => this.fht.Recover(fullcheckpointToken);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        public void Recover(Guid indexToken, Guid hybridLogToken) => this.fht.Recover(indexToken, hybridLogToken);

#endregion Checkpoint Operations

#region Log Operations
        /// <inheritdoc/>
        public void FlushLog(bool wait) => this.fht.Log.Flush(wait);

        /// <inheritdoc/>
        public void FlushAndEvictLog(bool wait) => this.fht.Log.FlushAndEvict(wait);

        /// <inheritdoc/>
        public void DisposeLogFromMemory() => this.fht.Log.DisposeFromMemory();
#endregion Log Operations
    }
}
