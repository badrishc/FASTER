// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal interface IClientSession
    {
        void AtomicSwitch(int version);
    }

    /// <summary>
    /// Public interface for a client session.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public interface IClientSession<Key, Value, Input, Output, Context, Functions> : IDisposable
            where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Get session ID
        /// </summary>
        string ID { get; }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns>Value is placed into <paramref name="output"/></returns>
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long serialNo);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns>Value is placed into <paramref name="output"/></returns>
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation that starts at an address rather than obtaining the address from the key's hash table slot.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="previousAddress">If this is != <see cref="Constants.kInvalidAddress"/>, the hash table is bypassed and the lookup starts at this address.
        ///     This is an in/out parameter that receives the previous address in the hash chain for this key when the matching record, if any, is found.</param>
        /// <param name="skipKeyVerification">If true, the caller does not have the key for previousAddress; Read returns whatever is at that address rather
        ///     than testing for possible collisions</param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns>Value is placed into <paramref name="output"/></returns>
        public Status Read(ref Key key, ref Input input, ref Output output, ref long previousAddress, bool skipKeyVerification = false, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call CompleteRead on the return value to complete the read operation</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default);

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="startAddress"></param>
        /// <param name="skipKeyVerification"></param>
        /// <param name="context"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call CompleteRead on the return value to complete the read operation</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, long startAddress, bool skipKeyVerification = false,
                                                                                                            Context context = default, CancellationToken token = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="context"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask UpsertAsync(ref Key key, ref Value desiredValue, Context context = default, bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status RMW(ref Key key, ref Input input, Context userContext, long serialNo);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public Status RMW(ref Key key, ref Input input);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask RMWAsync(ref Key key, ref Input input, Context context = default, bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(ref Key key, Context userContext, long serialNo);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Status Delete(ref Key key);

        /// <summary>
        /// Async delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="waitForCommit"></param>
        /// <param name="context"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask DeleteAsync(ref Key key, Context context = default, bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests();

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh();

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask ReadyToCompletePendingAsync(CancellationToken token = default);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public ValueTask WaitForCommitAsync(CancellationToken token = default);
    }
}
