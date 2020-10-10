// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Thread-independent session interface to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public sealed partial class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IClientSession<Key, Value, Input, Output, Context, Functions>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value> fht;

        internal readonly bool SupportAsync = false;
        internal readonly FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx;
        internal CommitPoint LatestCommitPoint;

        internal readonly Functions functions;
        internal readonly IVariableLengthStruct<Value, Input> variableLengthStruct;

        internal readonly AsyncFasterSession FasterSession;

        internal ClientSession(
            FasterKV<Key, Value> fht,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            bool supportAsync,
            IVariableLengthStruct<Value, Input> variableLengthStruct)
        {
            this.fht = fht;
            this.ctx = ctx;
            this.functions = functions;
            SupportAsync = supportAsync;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            FasterSession = new AsyncFasterSession(this);

            this.variableLengthStruct = variableLengthStruct;
            if (this.variableLengthStruct == default)
            {
                if (fht.hlog is VariableLengthBlittableAllocator<Key, Value> allocator)
                {
                    Debug.WriteLine("Warning: Session did not specify Input-specific functions for variable-length values via IVariableLengthStruct<Value, Input>");
                    this.variableLengthStruct = new DefaultVariableLengthStruct<Value, Input>(allocator.ValueLength);
                }
            }
            else
            {
                if (!(fht.hlog is VariableLengthBlittableAllocator<Key, Value>))
                    Debug.WriteLine("Warning: Session param of variableLengthStruct provided for non-varlen allocator");
            }

            // Session runs on a single thread
            if (!supportAsync)
                UnsafeResumeThread();
        }

        /// <inheritdoc/>
        public string ID { get { return ctx.guid; } }

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            CompletePending(true);

            // Session runs on a single thread
            if (!SupportAsync)
                UnsafeSuspendThread();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return this.Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, Output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, long startAddress, out RecordInfo recordInfo, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, startAddress, out recordInfo, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            Input input = default;
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, long startAddress, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, startAddress, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        public Status GetKey(long logicalAddress, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            throw new FasterException("GetKey NYI");
        }

        /// <inheritdoc/>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> GetKeyAsync(long logicalAddress, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            throw new FasterException("GetKeyAsync NYI");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref desiredValue, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask UpsertAsync(ref Key key, ref Value desiredValue, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = Upsert(ref key, ref desiredValue, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowUpsertAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowUpsertAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {
            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRMW(ref key, ref input, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input) => this.RMW(ref key, ref input, default, 0);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RMWAsync(ref Key key, ref Input input, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = RMW(ref key, ref input, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowRMWAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowRMWAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {

            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext, long serialNo)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextDelete(ref key, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key)
        {
                return this.Delete(ref key, default, 0);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DeleteAsync(ref Key key, Context context = default, bool waitForCommit = false, CancellationToken token = default)
        {
            var status = Delete(ref key, context, ctx.serialNum + 1);

            if (status == Status.OK && !waitForCommit)
                return default;

            return SlowDeleteAsync(this, waitForCommit, status, token);
        }

        private static async ValueTask SlowDeleteAsync(ClientSession<Key, Value, Input, Output, Context, Functions> @this, bool waitForCommit, Status status, CancellationToken token)
        {

            if (status == Status.PENDING)
                await @this.CompletePendingAsync(waitForCommit, token);
            else if (waitForCommit)
                await @this.WaitForCommitAsync(token);
        }

        /// <summary>
        /// Experimental feature
        /// Checks whether specified record is present in memory
        /// (between HeadAddress and tail, or between fromAddress
        /// and tail)
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="fromAddress">Look until this address</param>
        /// <returns>Status</returns>
        internal Status ContainsKeyInMemory(ref Key key, long fromAddress = -1)
        {
            return fht.InternalContainsKeyInMemory(ref key, ctx, FasterSession, fromAddress);
        }

        /// <inheritdoc/>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.prevCtx?.retryRequests)
                yield return val.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.retryRequests)
                yield return val.serialNum;
        }

        /// <inheritdoc/>
        public void Refresh()
        {
            if (SupportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx, FasterSession);
            if (SupportAsync) UnsafeSuspendThread();
        }

        /// <inheritdoc/>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                var result = fht.InternalCompletePending(ctx, FasterSession, spinWait);
                if (spinWaitForCommit)
                {
                    if (spinWait != true)
                    {
                        throw new FasterException("Can spin-wait for checkpoint completion only if spinWait is true");
                    }
                    do
                    {
                        fht.InternalCompletePending(ctx, FasterSession, spinWait);
                        if (fht.InRestPhase())
                        {
                            fht.InternalCompletePending(ctx, FasterSession, spinWait);
                            return true;
                        }
                    } while (spinWait);
                }
                return result;
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await fht.CompletePendingAsync(this, token);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token);
        }

        /// <inheritdoc/>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await fht.ReadyToCompletePendingAsync(this, token);
        }

        /// <inheritdoc/>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Complete all pending operations on session
            await CompletePendingAsync();

            var task = fht.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token);
                Refresh();

                task = fht.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            fht.epoch.Resume();
            fht.InternalRefresh(ctx, FasterSession);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            fht.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(int version)
        {
            fht.AtomicSwitch(ctx, ctx.prevCtx, version, fht._hybridLogCheckpoint.info.checkpointTokens);
        }

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal struct AsyncFasterSession : IFasterSession<Key, Value, Input, Output, Context>, IFunctions<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public AsyncFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(guid, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }

            public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, long logicalAddress) 
                => _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, logicalAddress);

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long logicalAddress) 
                => _clientSession.functions.ConcurrentWriter(ref key, ref src, ref dst, logicalAddress);

            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, long oldLogicalAddress, long newLogicalAddress) 
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, oldLogicalAddress, newLogicalAddress);

            public void DeleteCompletionCallback(ref Key key, Context ctx) 
                => _clientSession.functions.DeleteCompletionCallback(ref key, ctx);

            public int GetInitialLength(ref Input input)
                => _clientSession.variableLengthStruct.GetInitialLength(ref input);

            public int GetLength(ref Value t, ref Input input) 
                => _clientSession.variableLengthStruct.GetLength(ref t, ref input);

            public void InitialUpdater(ref Key key, ref Input input, ref Value value, long logicalAddress) 
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, logicalAddress);

            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, long logicalAddress) 
                => _clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, logicalAddress);

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo) 
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordInfo);

            public void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status) 
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ctx, status);

            public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, long logicalAddress) 
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, logicalAddress);

            public void SingleWriter(ref Key key, ref Value src, ref Value dst, long logicalAddress) 
                => _clientSession.functions.SingleWriter(ref key, ref src, ref dst, logicalAddress);

            public void UnsafeResumeThread() 
                => _clientSession.UnsafeResumeThread();

            public void UnsafeSuspendThread() 
                => _clientSession.UnsafeSuspendThread();

            public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) 
                => _clientSession.functions.UpsertCompletionCallback(ref key, ref value, ctx);
        }
    }
}
