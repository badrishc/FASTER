// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// A group of <see cref="Predicate{TPKey, TRecordId}"/>s. Ideally, most records in the group will either match all
    /// Predicates or none, for efficient use of log space.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the wrapper for the provider's data (obtained from TRecordId)</typeparam>
    /// <typeparam name="TPKey">The type of the key returned by the Predicate and stored in the secondary FasterKV instance</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    internal class Group<TProviderData, TPKey, TRecordId> : IExecutePredicate<TProviderData, TRecordId>, IQueryPredicate<TPKey, TRecordId>, IEquatable<Group<TProviderData, TPKey, TRecordId>>
        where TPKey : struct
        where TRecordId : struct, IComparable<TRecordId>
    {
        internal FasterKVSHI<TPKey, TRecordId> fkv;
        internal IPredicateDefinition<TProviderData, TPKey>[] predicateDefinitions;
        private readonly RegistrationSettings<TPKey> regSettings;

        /// <summary>
        /// ID of the group (used internally only)
        /// </summary>
        public long Id { get; }

        private readonly CheckpointSettings checkpointSettings;
        private readonly int keyPointerSize = Utility.GetSize(default(KeyPointer<TPKey>));
        private readonly int recordIdSize = (Utility.GetSize(default(TRecordId)) + sizeof(long) - 1) & ~(sizeof(long) - 1);

        /// <summary>
        /// The list of <see cref="Predicate{TPKey, TRecordId}"/>s in this group
        /// </summary>
        public Predicate<TPKey, TRecordId>[] Predicates { get; private set; }

        private int PredicateCount => this.Predicates.Length;

        private readonly IFasterEqualityComparer<TPKey> userKeyComparer;
        private readonly KeyAccessor<TPKey, TRecordId> keyAccessor;

        private readonly SectorAlignedBufferPool bufferPool;

        // Override equivalence testing for set membership

        /// <inheritdoc/>
        public override int GetHashCode() => this.Id.GetHashCode();

        /// <inheritdoc/>
        public override bool Equals(object obj) => this.Equals(obj as Group<TProviderData, TPKey, TRecordId>);

        /// <inheritdoc/>
        public bool Equals(Group<TProviderData, TPKey, TRecordId> other) => other is {} && this.Id == other.Id;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="regSettings">Optional registration settings</param>
        /// <param name="defs">Predicate definitions</param>
        /// <param name="id">The ordinal of this Group in the <see cref="SubsetHashIndex{TProviderData, TRecordId}"/>'s Group list.</param>
        public Group(RegistrationSettings<TPKey> regSettings, IPredicateDefinition<TProviderData, TPKey>[] defs, long id)
        {
            this.predicateDefinitions = defs;
            this.Id = id;
            this.regSettings = regSettings;
            this.userKeyComparer = GetUserKeyComparer();

            this.Predicates = defs.Select((def, ord) => new Predicate<TPKey, TRecordId>(this.Id, ord, def.Name, this)).ToArray();
            this.keyAccessor = new KeyAccessor<TPKey, TRecordId>(this.userKeyComparer, this.PredicateCount, this.keyPointerSize);

            this.checkpointSettings = regSettings?.CheckpointSettings;
            this.fkv = new FasterKVSHI<TPKey, TRecordId>(
                    regSettings.HashTableSize, regSettings.LogSettings, this.checkpointSettings, null /*SerializerSettings*/,
                    keyAccessor,
                    new VariableLengthStructSettings<TPKey, TRecordId>
                    {
                        keyLength = new CompositeKey<TPKey>.VarLenLength(this.keyPointerSize, this.PredicateCount)
                    }
                );

            // Now we have the log to use.
            this.keyAccessor.SetLog(this.fkv.hlog);
            this.bufferPool = this.fkv.hlog.bufferPool;
        }

        /// <inheritdoc/>
        public IDisposable NewSession()
            => this.fkv.For(new FasterKVSHI<TPKey, TRecordId>.Functions(this.keyAccessor, this.fkv.RecordAccessor))
                       .NewSession<FasterKVSHI<TPKey, TRecordId>.Functions>(threadAffinitized: this.regSettings.ThreadAffinitized);

        private AdvancedClientSession<TPKey, TRecordId, FasterKVSHI<TPKey, TRecordId>.Input, FasterKVSHI<TPKey, TRecordId>.Output,
                              FasterKVSHI<TPKey, TRecordId>.Context, FasterKVSHI<TPKey, TRecordId>.Functions> ToFkvSession(IDisposable sessionObj)
            => sessionObj is AdvancedClientSession<TPKey, TRecordId, FasterKVSHI<TPKey, TRecordId>.Input, FasterKVSHI<TPKey, TRecordId>.Output, 
                                           FasterKVSHI<TPKey, TRecordId>.Context, FasterKVSHI<TPKey, TRecordId>.Functions> session
                ? session
                : throw new InvalidOperationExceptionSHI("Expected a FASTER AdvancedClientSession");

        private IFasterEqualityComparer<TPKey> GetUserKeyComparer()
        {
            if (this.regSettings.KeyComparer is {})
                return this.regSettings.KeyComparer;
            if (typeof(IFasterEqualityComparer<TPKey>).IsAssignableFrom(typeof(TPKey)))
                return new TPKey() as IFasterEqualityComparer<TPKey>;

            Console.WriteLine(
                $"***WARNING*** Creating default FASTER key equality comparer based on potentially slow {nameof(EqualityComparer<TPKey>)}." +
                $" To avoid this, provide a comparer in {nameof(RegistrationSettings<TPKey>)}.{nameof(RegistrationSettings<TPKey>.KeyComparer)}," +
                $" or make {typeof(TPKey).Name} implement the interface {nameof(IFasterEqualityComparer<TPKey>)}");
            return FasterEqualityComparer.Get<TPKey>();
        }

        /// <summary>
        /// Returns the named <see cref="Predicate{TPKey, TRecordId}"/> from the Predicates list.
        /// </summary>
        /// <param name="name">The name of the <see cref="Predicate{TPKey, TRecordId}"/>; unique among all groups</param>
        /// <returns></returns>
        public Predicate<TPKey, TRecordId> this[string name]
            => Array.Find(this.Predicates, pred => pred.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
                ?? throw new ArgumentExceptionSHI("Predicate not found");

        private unsafe void StoreKeys(ref GroupCompositeKey keys, byte* keysPtr, int keysLen)
        {
            var poolKeyMem = this.bufferPool.Get(keysLen);
            Buffer.MemoryCopy(keysPtr, poolKeyMem.GetValidPointer(), keysLen, keysLen);
            keys.Set(poolKeyMem);
        }

        internal unsafe void MarkChanges(ref GroupCompositeKeyPair keysPair, bool recordIdChanged)
        {
            ref GroupCompositeKey before = ref keysPair.Before;
            ref GroupCompositeKey after = ref keysPair.After;
            ref CompositeKey<TPKey> beforeCompositeKey = ref before.CastToKeyRef<CompositeKey<TPKey>>();
            ref CompositeKey<TPKey> afterCompositeKey = ref after.CastToKeyRef<CompositeKey<TPKey>>();

            if (recordIdChanged)
                keysPair.HasChanges = true;

            for (var ii = 0; ii < this.PredicateCount; ++ii)
            {
                ref KeyPointer<TPKey> beforeKeyPointer = ref beforeCompositeKey.GetKeyPointerRef(ii, this.keyPointerSize);
                ref KeyPointer<TPKey> afterKeyPointer = ref afterCompositeKey.GetKeyPointerRef(ii, this.keyPointerSize);

                var beforeIsNull = beforeKeyPointer.IsNull;
                var afterIsNull = afterKeyPointer.IsNull;
                var keysEqual = !beforeIsNull && !afterIsNull && beforeKeyPointer.Key.Equals(afterKeyPointer.Key);

                // IsNull is already set in Group.ExecuteAndStore.
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
        public unsafe Status ExecuteAndStore(IDisposable sessionObj, TProviderData providerData, TRecordId recordId, ExecutionPhase phase,
                                             ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // Note: stackalloc is safe because PendingContext or ChangeTracker will copy it to the bufferPool
            // if needed. On the Insert fast path, we don't want any allocations otherwise; changeTracker is null.
            var keyMemLen = this.keyPointerSize * this.PredicateCount;
            var keyBytes = stackalloc byte[keyMemLen];
            var anyMatch = false;

            for (var ii = 0; ii < this.PredicateCount; ++ii)
            {
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyBytes + ii * this.keyPointerSize);
                keyPointer.PreviousAddress = core.Constants.kInvalidAddress;
                keyPointer.PredicateOrdinal = (byte)ii;

                var key = this.predicateDefinitions[ii].Execute(providerData);
                keyPointer.IsNull = !key.HasValue;
                if (key.HasValue)
                {
                    keyPointer.Key = key.Value;
                    anyMatch = true;
                }
            }

            if (!anyMatch && phase == ExecutionPhase.Insert)
                return Status.OK;

            ref CompositeKey<TPKey> compositeKey = ref Unsafe.AsRef<CompositeKey<TPKey>>(keyBytes);
            var input = new FasterKVSHI<TPKey, TRecordId>.Input(this.Id, 0);
            var value = recordId;

            int groupOrdinal = -1;
            if (changeTracker is {})
            {
                value = changeTracker.BeforeRecordId;
                if (phase == ExecutionPhase.PreUpdate)
                {
                    // Get a free group ref and store the "before" values.
                    ref GroupCompositeKeyPair groupKeysPair = ref changeTracker.FindGroupRef(this.Id);
                    StoreKeys(ref groupKeysPair.Before, keyBytes, keyMemLen);
                    return Status.OK;
                }

                if (phase == ExecutionPhase.PostUpdate)
                {
                    // TODOtest: If not found, this is a new group added after the PreUpdate was done, so handle this as an insert.
                    if (!changeTracker.FindGroup(this.Id, out groupOrdinal))
                    {
                        phase = ExecutionPhase.Insert;
                    }
                    else
                    {
                        ref GroupCompositeKeyPair groupKeysPair = ref changeTracker.GetGroupRef(groupOrdinal);
                        StoreKeys(ref groupKeysPair.After, keyBytes, keyMemLen);
                        this.MarkChanges(ref groupKeysPair, changeTracker.AfterRecordId.CompareTo(changeTracker.BeforeRecordId) != 0);

                        // TODOtest: In debug, for initial dev, follow chains to assert the values match what is in the record's compositeKey
                        if (!groupKeysPair.HasChanges)
                            return Status.OK;
                    }
                }

                // We don't need to do anything here for Delete.
            }

            var session = this.ToFkvSession(sessionObj);
            var lsn = session.ctx.serialNum + 1;
            var context = new FasterKVSHI<TPKey, TRecordId>.Context { Functions = session.functions };
            return phase switch
            {
                ExecutionPhase.Insert => session.IndexInsert(this.fkv, ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, ref context, lsn),
                ExecutionPhase.PostUpdate => session.IndexUpdate(this.fkv, ref changeTracker.GetGroupRef(groupOrdinal), ref value, ref input, ref context, lsn, changeTracker),
                ExecutionPhase.Delete => session.IndexDelete(this.fkv, ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, ref context, lsn),
                _ => throw new InternalErrorExceptionSHI("Unknown Predicate execution Phase {phase}")
            };
        }

        // Async version of ExecuteAndStore; called when we expect the operation to actually access the fkv, not just store the keys.
        public async ValueTask ExecuteAsync(IDisposable sessionObj, TProviderData providerData, TRecordId recordId, ExecutionPhase phase,
                                              ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken)
        {
            var session = this.ToFkvSession(sessionObj);
            var status = this.ExecuteAndStore(sessionObj, providerData, recordId, phase, changeTracker);
            if (status == Status.PENDING)
                await session.CompletePendingAsync(waitForCommit: false, cancellationToken);
            // TODO handle Status.ERROR in ExecuteAsync
        }

        /// <inheritdoc/>
        public Status GetBeforeKeys(ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.HasBeforeKeys)
                return Status.OK;

            // Obtain the "before" values; this does no FKV operations so the session is not used. TODOcache: try to find TRecordId in the IPUCache first.
            return ExecuteAndStore(sessionObj: null, changeTracker.BeforeData, default, ExecutionPhase.PreUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public Status Update(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // RMW did not find the record so did an insert. Go through Insert logic here.
                return this.ExecuteAndStore(sessionObj, changeTracker.BeforeData, changeTracker.BeforeRecordId, ExecutionPhase.Insert, changeTracker:null);
            }

            if (changeTracker.UpdateOp == UpdateOperation.Delete)
            {
                return this.Delete(sessionObj, changeTracker);
            }

            changeTracker.CachedBeforeLA = core.Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != core.Constants.kInvalidAddress)
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
            return this.ExecuteAndStore(sessionObj, changeTracker.AfterData, default, ExecutionPhase.PostUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public ValueTask UpdateAsync(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken)
        {
            if (changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // RMW did not find the record so did an insert. Go through Insert logic here.
                return this.ExecuteAsync(sessionObj, changeTracker.BeforeData, changeTracker.BeforeRecordId, ExecutionPhase.Insert, changeTracker: null, cancellationToken);
            }

            if (changeTracker.UpdateOp == UpdateOperation.Delete)
            {
                return this.DeleteAsync(sessionObj, changeTracker, cancellationToken);
            }

            changeTracker.CachedBeforeLA = core.Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != core.Constants.kInvalidAddress)
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
            return this.ExecuteAsync(sessionObj, changeTracker.AfterData, default, ExecutionPhase.PostUpdate, changeTracker, cancellationToken);
        }

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        public Status Delete(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            changeTracker.CachedBeforeLA = core.Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != core.Constants.kInvalidAddress)
            {
                // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                // If the latter, we can bypass ExecuteAndStore's Predicate-execute loop
            }
            return this.ExecuteAndStore(sessionObj, changeTracker.BeforeData, default, ExecutionPhase.Delete, changeTracker);
        }

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        public ValueTask DeleteAsync(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken)
        {
            changeTracker.CachedBeforeLA = core.Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != core.Constants.kInvalidAddress)
            {
                // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                // If the latter, we can bypass ExecuteAndStore's Predicate-execute loop
            }
            return this.ExecuteAsync(sessionObj, changeTracker.BeforeData, default, ExecutionPhase.Delete, changeTracker, cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe FasterKVSHI<TPKey, TRecordId>.Input MakeQueryInput(int predOrdinal, ref TPKey key)
        {
            // Putting the query key in Input is necessary because iterator functions cannot contain unsafe code or have
            // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var input = new FasterKVSHI<TPKey, TRecordId>.Input(this.Id, predOrdinal);
            input.SetQueryKey(this.bufferPool, this.keyAccessor, ref key);
            return input;
        }

        /// <inheritdoc/>
        public unsafe IEnumerable<TRecordId> Query(IDisposable sessionObj, int predOrdinal, TPKey key, QuerySettings querySettings)
            => Query(sessionObj, MakeQueryInput(predOrdinal, ref key), querySettings);

        private IEnumerable<TRecordId> Query(IDisposable sessionObj, FasterKVSHI<TPKey, TRecordId>.Input input, QuerySettings querySettings)
        {
            // TODOperf: if there are multiple Predicates within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPKeys and their indexes in queryKeyPtr. Also consider
            // having TPKeys[] for a single Predicate walk through in parallel, so the FHT log memory access is sequential.
            var session = this.ToFkvSession(sessionObj);
            var context = new FasterKVSHI<TPKey, TRecordId>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                do
                {
                    var output = new FasterKVSHI<TPKey, TRecordId>.Output();
                    Status status = session.IndexRead(this.fkv, ref input.QueryKeyRef, ref input, ref output, ref recordInfo, ref context, session.ctx.serialNum);
                    if (querySettings.IsCanceled)
                        yield break;
                    if (status == Status.PENDING)
                    {
                        // Because we traverse the chain, we must wait for any pending read operations to complete.
                        // TODOperf: extend the queue for multiple sync+pending operations rather than spinWaiting in CompletePending for each pending record.
                        session.CompletePending(spinWait: true);
                        if (context.Functions.Queue.Count == 0)
                        {
                            Debug.Fail("ReadCompletionCallback was not called");
                            yield break;
                        }
                        output = context.Functions.Queue.Dequeue();
                        status = output.PendingResultStatus;
                    }

                    // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                    // Thus, Status.NOTFOUND should only be returned if the key was not found.
                    if (status != Status.OK)
                        yield break;

                    if (!deadRecs.CheckIfDead(output.RecordId, output.IsDeleted))
                        yield return output.RecordId;

                    recordInfo.PreviousAddress = output.PreviousAddress;
                } while (recordInfo.PreviousAddress != core.Constants.kInvalidAddress);
            }
            finally
            {
                input.Dispose();
            }
        }

#if NETSTANDARD21
        /// <inheritdoc/>
        public unsafe IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, int predOrdinal, TPKey key, QuerySettings querySettings)
            => QueryAsync(sessionObj, MakeQueryInput(predOrdinal, ref key), querySettings);

        private async IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, FasterKVSHI<TPKey, TRecordId>.Input input, QuerySettings querySettings)
        {
            // TODOperf: if there are multiple Predicates within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPKeys and their indexes in queryKeyPtr. Also consider
            // having TPKeys[] for a single Predicate walk through in parallel, so the FHT log memory access is sequential.
            var session = this.ToFkvSession(sessionObj);
            var context = new FasterKVSHI<TPKey, TRecordId>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                do
                {
                    // Because we traverse the chain, we must wait for any pending read operations to complete.
                    var readAsyncResult = await session.IndexReadAsync(this.fkv, ref input.QueryKeyRef, ref input, recordInfo.PreviousAddress, ref context, session.ctx.serialNum, querySettings);
                    if (querySettings.IsCanceled)
                        yield break;
                    var (status, output) = readAsyncResult.Complete();

                    // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                    // Thus, Status.NOTFOUND should only be returned if the key was not found.
                    if (status != Status.OK)
                        yield break;

                    if (!deadRecs.CheckIfDead(output.RecordId, output.IsDeleted))
                        yield return output.RecordId;

                    recordInfo.PreviousAddress = output.PreviousAddress;
                } while (recordInfo.PreviousAddress != core.Constants.kInvalidAddress);
            }
            finally
            {
                input.Dispose();
            }
        }
#endif

        /// <inheritdoc/>
        public bool CompletePending(IDisposable sessionObj, bool spinWait, bool spinWaitForCommit)
            => this.ToFkvSession(sessionObj).CompletePending(spinWait, spinWaitForCommit);                  // TODO: Resolve issues with non-async operations

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(IDisposable sessionObj, bool waitForCommit, CancellationToken cancellationToken)
            => this.ToFkvSession(sessionObj).CompletePendingAsync(waitForCommit, cancellationToken);        // TODO: Resolve issues with non-async operations in groups

        public ValueTask ReadyToCompletePendingAsync(IDisposable sessionObj, CancellationToken cancellationToken)
            => this.ToFkvSession(sessionObj).ReadyToCompletePendingAsync(cancellationToken);

        public ValueTask WaitForCommitAsync(IDisposable sessionObj, CancellationToken cancellationToken)
            => this.ToFkvSession(sessionObj).WaitForCommitAsync(cancellationToken);

        #region Checkpoint Operations
        /// <inheritdoc/>
        public bool GrowIndex() => this.fkv.GrowIndex();   // TODO fht ==> fkv

        /// <inheritdoc/>
        public bool TakeFullCheckpoint() => this.fkv.TakeFullCheckpoint(out _);

        /// <inheritdoc/>
        public bool TakeFullCheckpoint(CheckpointType checkpointType) => this.fkv.TakeFullCheckpoint(out _, checkpointType);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
            => (await this.fkv.TakeFullCheckpointAsync(checkpointType, cancellationToken)).success;

        /// <inheritdoc/>
        public bool TakeIndexCheckpoint() => this.fkv.TakeIndexCheckpoint(out _);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default) 
            => (await this.fkv.TakeIndexCheckpointAsync(cancellationToken)).success;

        /// <inheritdoc/>
        public bool TakeHybridLogCheckpoint() => this.fkv.TakeHybridLogCheckpoint(out _);

        /// <inheritdoc/>
        public bool TakeHybridLogCheckpoint(CheckpointType checkpointType) => this.fkv.TakeHybridLogCheckpoint(out _, checkpointType);

        /// <inheritdoc/>
        public async ValueTask<bool> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default) 
            => (await this.fkv.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken)).success;

        /// <inheritdoc/>
        public ValueTask CompleteCheckpointAsync(CancellationToken token = default) => this.fkv.CompleteCheckpointAsync(token);

        /// <inheritdoc/>
        public void Recover() => this.fkv.Recover();

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        public void Recover(Guid fullcheckpointToken) => this.fkv.Recover(fullcheckpointToken);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        public void Recover(Guid indexToken, Guid hybridLogToken) => this.fkv.Recover(indexToken, hybridLogToken);

#endregion Checkpoint Operations

#region Log Operations
        /// <inheritdoc/>
        public void Flush(bool wait) => this.fkv.Log.Flush(wait);

        /// <inheritdoc/>
        public void FlushAndEvict(bool wait) => this.fkv.Log.FlushAndEvict(wait);

        /// <inheritdoc/>
        public void DisposeFromMemory() => this.fkv.Log.DisposeFromMemory();
#endregion Log Operations
    }
}
