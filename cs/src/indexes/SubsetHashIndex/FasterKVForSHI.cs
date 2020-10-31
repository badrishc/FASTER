// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetHashIndex;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.SubsetHashIndex
{
    /// <summary>
    /// SubsetHashIndex-enabled wrapper for FasterKV
    /// </summary>
    public partial class FasterKVForSHI<TKVKey, TKVValue> : FasterKV<TKVKey, TKVValue>
    {
        private readonly SubsetHashIndex<FasterKVProviderData<TKVKey, TKVValue>, long> subsetHashIndex;

        /// <summary>
        /// Constructs a <see cref="FasterKVForSHI{Key, Value}"/> subclass of <see cref="FasterKV{Key, Value}"/>.
        /// </summary>
        /// <param name="size"></param>
        /// <param name="logSettings"></param>
        /// <param name="checkpointSettings"></param>
        /// <param name="serializerSettings"></param>
        /// <param name="comparer"></param>
        /// <param name="variableLengthStructSettings"></param>
        public FasterKVForSHI(long size, LogSettings logSettings,
                           CheckpointSettings checkpointSettings = null, SerializerSettings<TKVKey, TKVValue> serializerSettings = null,
                           IFasterEqualityComparer<TKVKey> comparer = null,
                           VariableLengthStructSettings<TKVKey, TKVValue> variableLengthStructSettings = null)
            : base(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
            this.subsetHashIndex = new SubsetHashIndex<FasterKVProviderData<TKVKey, TKVValue>, long>();
        }

        #region Predicate Registration API
        /// <inheritdoc/>
        public IPredicate Register<TPKey>(RegistrationSettings<TPKey> registrationSettings,
                                         FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey> def)
            where TPKey : struct
            => this.subsetHashIndex.Register(registrationSettings, def);

        /// <inheritdoc/>
        public IPredicate[] Register<TPKey>(RegistrationSettings<TPKey> registrationSettings,
                                           params FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>[] defs)
            where TPKey : struct
            => this.subsetHashIndex.Register(registrationSettings, defs);

        /// <inheritdoc/>
        public IPredicate Register<TPKey>(RegistrationSettings<TPKey> registrationSettings,
                                         string predName, Func<TKVKey, TKVValue, TPKey?> predFunc)
            where TPKey : struct
            => this.subsetHashIndex.Register(registrationSettings, new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(predName, predFunc));

        /// <inheritdoc/>
        public IPredicate[] Register<TPKey>(RegistrationSettings<TPKey> registrationSettings,
                                           params (string, Func<TKVKey, TKVValue, TPKey?>)[] predFuncs)
            where TPKey : struct
            => this.subsetHashIndex.Register(registrationSettings, predFuncs.Select(e => new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(e.Item1, e.Item2)).ToArray());
        #endregion Predicate Registration API

        #region IFasterKV implementations

        #region New Session Operations

        #region Unavailable New Session Operations
#pragma warning disable CS0809 // Obsolete member overrides non-obsolete member

        const string MustUseSubsetHashIndexSessionErr = "SubsetHashIndex sessions must use ForSHI instead of For and NewSessionForSHI instead of NewSession";

        /// <inheritdoc/>
        [Obsolete(MustUseSubsetHashIndexSessionErr, true)]

        public override ClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        public override AdvancedClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        public override ClientSession<TKVKey, TKVValue, Input, Output, Context, IFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUseSubsetHashIndexSessionErr, true)]
        public override AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUseSubsetHashIndexSessionErr, true)]
        public override ClientSession<TKVKey, TKVValue, Input, Output, Context, IFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUseSubsetHashIndexSessionErr, true)]
        public override AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

#pragma warning restore CS0809 // Obsolete member overrides non-obsolete member
        #endregion Unavailable New Session Operations

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public ClientSessionBuilderForSHI<Input, Output, Context> ForSHI<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
        {
            return new ClientSessionBuilderForSHI<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types for advanced client sessions
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public AdvancedClientSessionBuilderForSHI<Input, Output, Context> ForSHI<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
        {
            return new AdvancedClientSessionBuilderForSHI<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> NewSessionForSHI<TInput, TOutput, TContext>(
                IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => NewSessionForSHI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSessionForSHI<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSessionForSHI<Input, Output, Context>(
                IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
        {
            return InternalNewSessionForSHI<Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
        }

        internal ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalNewSessionForSHI<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.Log, base.RecordAccessor, this.subsetHashIndex);
            var session = base.For(indexingFunctions).NewSession(indexingFunctions, sessionId, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>(this);
            var livenessSession = base.NewSession(livenessFunctions);
            return new ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this, indexingFunctions, session, session.SupportAsync, livenessSession, this.subsetHashIndex);
        }

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).ResumeSessionForSHI&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSHI<TInput, TOutput, TContext>(
                IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => ResumeSessionForSHI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSHI<TInput, TOutput, TContext>(
                IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
        {
            return InternalResumeSessionForSHI<TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
        }

        internal ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalResumeSessionForSHI<TInput, TOutput, TContext, Functions>(Functions functions,
                                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.Log, base.RecordAccessor, this.subsetHashIndex);
            var session = base.For(indexingFunctions).ResumeSession(indexingFunctions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>(this);
            var livenessSession = base.NewSession(livenessFunctions);
            return new ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this, indexingFunctions, session, session.SupportAsync, livenessSession, this.subsetHashIndex);
        }

        #endregion New Session Operations

        #region Growth and Recovery

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool GrowIndex()
            => base.GrowIndex() && this.subsetHashIndex.GrowIndex();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeFullCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            => base.TakeFullCheckpoint(out token) && this.subsetHashIndex.TakeFullCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            => base.TakeFullCheckpoint(out token, checkpointType) && this.subsetHashIndex.TakeFullCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            return (success && await this.subsetHashIndex.TakeFullCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeIndexCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeIndexCheckpoint
            => base.TakeIndexCheckpoint(out token) && this.subsetHashIndex.TakeIndexCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeIndexCheckpointAsync(cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeIndexCheckpoint
            return (success && await this.subsetHashIndex.TakeIndexCheckpointAsync(cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeHybridLogCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            => base.TakeHybridLogCheckpoint(out token) && this.subsetHashIndex.TakeHybridLogCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            => base.TakeHybridLogCheckpoint(out token, checkpointType) && this.subsetHashIndex.TakeHybridLogCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            return (success && await this.subsetHashIndex.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            // TODO: RecoverAsync with separate Tasks for primary fkv and each Group
            base.Recover(numPagesToPreload, undoFutureVersions);
            this.subsetHashIndex.Recover();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            base.Recover(fullcheckpointToken, numPagesToPreload, undoFutureVersions);
            this.subsetHashIndex.Recover(fullcheckpointToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            base.Recover(indexToken, hybridLogToken, numPagesToPreload, undoFutureVersions);
            this.subsetHashIndex.Recover(indexToken, hybridLogToken);
        }

        /// <inheritdoc/>
        public override async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            // Simple sequence to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            var vt1 = base.CompleteCheckpointAsync(token);
            var vt2 = this.subsetHashIndex.CompleteCheckpointAsync(token);
            await vt1;
            await vt2;
        }

        #endregion Growth and Recovery

        /// <inheritdoc/>
        public override void Dispose() => base.Dispose();

        #endregion IFasterKV implementations

        #region Flush implementations
        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait)
        {
            this.Log.Flush(wait);
            this.subsetHashIndex.Flush(wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Wait for operation to complete</param>
        public void FlushAndEvict(bool wait)
        {
            this.Log.FlushAndEvict(wait);
            this.subsetHashIndex.FlushAndEvict(wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeFromMemory()
        {
            this.Log.DisposeFromMemory();
            this.subsetHashIndex.DisposeFromMemory();
        }
        #endregion Flush implementations
    }
}
