// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// This interface is implemented on a <see cref="Group{TProviderData, TPKey, TRecordId}"/> and decouples
    /// the <see cref="Predicate{TPKey, TRecordId}"/> execution from the knowledge of the TKVKey and TKVValue of the
    /// primary FasterKV instance.
    /// </summary>
    /// <typeparam name="TProviderData"></typeparam>
    /// <typeparam name="TRecordId"></typeparam>
    public interface IExecutePredicate<TProviderData, TRecordId>
        where TRecordId : struct, IComparable<TRecordId>
    {
        /// <summary>
        /// Creates a new session on the underlying secondary FasterKV
        /// </summary>
        /// <remarks>IDispoable because IClientSession is internal and IClientSession{} type params would be troublesome</remarks>
        IDisposable NewSession();

        /// <summary>
        /// For each <see cref="Predicate{TPKey, TRecordId}"/> in the <see cref="Group{TProviderData, TPKey, TRecordId}"/>,
        /// and store the resultant TPKey in the secondary FasterKV instance.
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="data">The provider's data, e.g. FasterKVProviderData{TKVKey, TKVValue}</param>
        /// <param name="recordId">The provider's record ID, e.g. long (logicalAddress) for FasterKV</param>
        /// <param name="phase">The phase of Index operations in which this execution is being done</param>
        /// <param name="changeTracker">Tracks the <see cref="ExecutionPhase.PreUpdate"/> values for comparison
        ///     to the <see cref="ExecutionPhase.PostUpdate"/> values</param>
        Status ExecuteAndStore(IDisposable sessionObj, TProviderData data, TRecordId recordId, 
                               ExecutionPhase phase, ChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// For each <see cref="Predicate{TPKey, TRecordId}"/> in the <see cref="Group{TProviderData, TPKey, TRecordId}"/>,
        /// and asynchronously store the resultant TPKey in the secondary FasterKV instance.
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="data">The provider's data, e.g. FasterKVProviderData{TKVKey, TKVValue}</param>
        /// <param name="recordId">The provider's record ID, e.g. long (logicalAddress) for FasterKV</param>
        /// <param name="phase">The phase of Index operations in which this execution is being done</param>
        /// <param name="changeTracker">Tracks the <see cref="ExecutionPhase.PreUpdate"/> values for comparison
        ///     to the <see cref="ExecutionPhase.PostUpdate"/> values</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        ValueTask ExecuteAsync(IDisposable sessionObj, TProviderData data, TRecordId recordId, ExecutionPhase phase,
                               ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken);

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        bool CompletePending(IDisposable sessionObj, bool spinWait, bool spinWaitForCommit);

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="waitForCommit">True to wait for a checkpoint after the operation</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        ValueTask CompletePendingAsync(IDisposable sessionObj, bool waitForCommit, CancellationToken cancellationToken);

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        ValueTask ReadyToCompletePendingAsync(IDisposable sessionObj, CancellationToken cancellationToken = default);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// <returns></returns>
        ValueTask WaitForCommitAsync(IDisposable sessionObj, CancellationToken cancellationToken = default);

        /// <summary>
        /// The identifier of this <see cref="Group{TProviderData, TPKey, TRecordId}"/>.
        /// </summary>
        long Id { get; }

        /// <summary>
        /// Get the TPKeys for the current (before updating) state of the RecordId
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// </summary>
        Status GetBeforeKeys(ChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Update the RecordId
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        Status Update(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Update the RecordId
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        /// </summary>
        ValueTask UpdateAsync(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken);

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        Status Delete(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// <param name="cancellationToken">Token to check for cancellation of the operation</param>
        ValueTask DeleteAsync(IDisposable sessionObj, ChangeTracker<TProviderData, TRecordId> changeTracker, CancellationToken cancellationToken);

        /// <summary>
        /// Grow the hash index
        /// </summary>
        bool GrowIndex();

        /// <summary>
        /// Take a full checkpoint of the FasterKV implementing the group's Index storage.
        /// </summary>
        bool TakeFullCheckpoint();

        /// <summary>
        /// Initiate full (index + log) checkpoint of the FasterKV implementing the group's Index storage.
        /// </summary>
        bool TakeFullCheckpoint(CheckpointType checkpointType);

        /// <summary>
        /// Takes a full (index + log) checkpoint of FASTER asynchronously
        /// </summary>
        ValueTask<bool> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Take a checkpoint of the Index (hashtable) only
        /// </summary>
        bool TakeIndexCheckpoint();

        /// <summary>
        /// Take a checkpoint of the Index (hashtable) only
        /// </summary>
        ValueTask<bool> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Take a checkpoint of the hybrid log only
        /// </summary>
        bool TakeHybridLogCheckpoint();

        /// <summary>
        /// Take a checkpoint of the hybrid log only
        /// </summary>
        bool TakeHybridLogCheckpoint(CheckpointType checkpointType);

        /// <summary>
        /// Initiate checkpoint of FASTER log only (not index)
        /// </summary>
        ValueTask<bool> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Complete ongoing checkpoint (spin-wait)
        /// </summary>
        ValueTask CompleteCheckpointAsync(CancellationToken token = default);

        /// <summary>
        /// Recover from last successful checkpoints
        /// </summary>
        void Recover();

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        void Recover(Guid fullcheckpointToken);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        void Recover(Guid indexToken, Guid hybridLogToken);

        /// <summary>
        /// Flush Index logs until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        void Flush(bool wait);

        /// <summary>
        /// Flush Index logs and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        void FlushAndEvict(bool wait);

        /// <summary>
        /// Delete Index logs entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        void DisposeFromMemory();
    }
}
