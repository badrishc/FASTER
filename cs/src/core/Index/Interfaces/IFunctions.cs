using System;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Callback functions to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        /// <param name="previousAddress">The previous address in this record's hash chain</param>
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, long previousAddress);

        /// <summary>
        /// Upsert completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="ctx"></param>
        void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx);

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status);

        /// <summary>
        /// Delete completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="ctx"></param>
        void DeleteCompletionCallback(ref Key key, Context ctx);

        /// <summary>
        /// Checkpoint completion callback (called per client session)
        /// </summary>
        /// <param name="sessionId">Session ID reporting persistence</param>
        /// <param name="commitPoint">Commit point descriptor</param>
        void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint);

        /// <summary>
        /// Initial update for RMW (essentially an insert operation).
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="address">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        void InitialUpdater(ref Key key, ref Input input, ref Value value, long address);

        /// <summary>
        /// Copy-update for RMW (copy/update the value to a new location)
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="oldAddress">The logical address of the record being copied from; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        /// <param name="newAddress">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, long oldAddress, long newAddress);

        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an in-place update, there is a previous value there.</param>
        /// <param name="address">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, long address);

        /// <summary>
        /// Non-concurrent reader. 
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="address">The logical address of the record being read from; can be used as a RecordId by indexing and for liveness checking. Note that this address may
        ///     be in the immutable region, or may not be in memory because this method is called for a read that has gone pending. Use <see cref="RecordAccessor{Key, Value}.IsInMemory(long)"/>
        ///     to test before dereferencing.</param>
        void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, long address);

        /// <summary>
        /// Conncurrent reader
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="address">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, long address);

        /// <summary>
        /// Non-concurrent writer; called on Upsert that does not find the key so does an insert or finds the key in the immutable region so does a read/copy/update (RCU),
        /// or when copying reads fetched from disk to either read cache or tail of log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="oldValue">The previous value to be copied/updated</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="address">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        void SingleWriter(ref Key key, ref Value oldValue, ref Value newValue, long address);

        /// <summary>
        /// Concurrent writer
        /// </summary>
        /// <param name="key">The key for the record to be written</param>
        /// <param name="src">The value to be copied to <paramref name="dst"/></param>
        /// <param name="dst">The location where <paramref name="src"/> is to be copied; because this method is called only for in-place updates, there is a previous value there.</param>
        /// <param name="address">The logical address of the record being copied to; can be used as a RecordId by indexing or passed to <see cref="RecordAccessor{Key, Value}"/></param>
        bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long address);
    }

    /// <summary>
    /// Callback functions to FASTER (two-param version)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface IFunctions<Key, Value> : IFunctions<Key, Value, Value, Value, Empty>
    {
    }

    /// <summary>
    /// Callback functions to FASTER (two-param version with context)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Context> : IFunctions<Key, Value, Value, Value, Context>
    {
    }
}