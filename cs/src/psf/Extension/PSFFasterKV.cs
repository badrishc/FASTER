// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using PSF.Index;
using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

namespace FASTER.PSF
{
    /// <summary>
    /// PSF-enabled wrapper FasterKV
    /// </summary>
    public partial class PSFFasterKV<TKVKey, TKVValue> : IFasterKV<TKVKey, TKVValue>
    {
        private static readonly ConcurrentDictionary<IFasterKV<TKVKey, TKVValue>, PSFFasterKV<TKVKey, TKVValue>> fkvDictionary 
            = new ConcurrentDictionary<IFasterKV<TKVKey, TKVValue>, PSFFasterKV<TKVKey, TKVValue>>();

        private readonly FasterKV<TKVKey, TKVValue> fkv;
        private readonly PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfManager;

        private PSFFasterKV(FasterKV<TKVKey, TKVValue> fkv)
        {
            this.fkv = fkv;
            this.psfManager = new PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long>();
        }

        /// <summary>
        /// Constructs a PSF wrapper for a <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        internal static PSFFasterKV<TKVKey, TKVValue> GetOrCreateWrapper(FasterKV<TKVKey, TKVValue> fkv) 
            => fkvDictionary.TryGetValue(fkv, out var psfKV)
                ? psfKV
                : fkvDictionary.GetOrAdd(fkv, new PSFFasterKV<TKVKey, TKVValue>(fkv));

        /// <summary>
        /// Constructs an internal <see cref="FasterKV{Key, Value}"/> instance and a PSF wrapper for it.
        /// </summary>
        /// <param name="size"></param>
        /// <param name="logSettings"></param>
        /// <param name="checkpointSettings"></param>
        /// <param name="serializerSettings"></param>
        /// <param name="comparer"></param>
        /// <param name="variableLengthStructSettings"></param>
        public PSFFasterKV(long size, LogSettings logSettings,
                           CheckpointSettings checkpointSettings = null, SerializerSettings<TKVKey, TKVValue> serializerSettings = null,
                           IFasterEqualityComparer<TKVKey> comparer = null,
                           VariableLengthStructSettings<TKVKey, TKVValue> variableLengthStructSettings = null)
            : this(new FasterKV<TKVKey, TKVValue>(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings))
            => fkvDictionary.GetOrAdd(fkv, this);


        #region PSF Registration API
        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey> def)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, def);

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>[] defs)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, defs);

        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         string psfName, Func<TKVKey, TKVValue, TPSFKey?> psfFunc)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, new FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>(psfName, psfFunc));

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params (string, Func<TKVKey, TKVValue, TPSFKey?>)[] psfFuncs)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, psfFuncs.Select(e => new FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>(e.Item1, e.Item2)).ToArray());
        #endregion PSF Registration API

        #region IFasterKV implementations

        #region New Session Operations

        #region Unavailable New Session Operations

        const string MustUsePSFSessionErr = "PSF sessions must use NewPSFSession";

        /// <inheritdoc/>
        [Obsolete(MustUsePSFSessionErr, true)]
        public ClientSession<TKVKey, TKVValue, Input, Output, Context, IFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new PSFInvalidOperationException(MustUsePSFSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUsePSFSessionErr, true)]
        public AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new PSFInvalidOperationException(MustUsePSFSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUsePSFSessionErr, true)]
        public ClientSession<TKVKey, TKVValue, Input, Output, Context, IFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new PSFInvalidOperationException(MustUsePSFSessionErr);

        /// <inheritdoc/>
        [Obsolete(MustUsePSFSessionErr, true)]
        public AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new PSFInvalidOperationException(MustUsePSFSessionErr);

        #endregion Unavailable New Session Operations

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public PSFClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
        {
            return new PSFClientSessionBuilder<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types for advanced client sessions
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public PSFAdvancedClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
        {
            return new PSFAdvancedClientSessionBuilder<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Start a new PSF client session wrapper around a FASTER client session.
        /// For performance reasons, please use PSFFasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> NewPSFSession<TInput, TOutput, TContext>(
                IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => NewPSFSession(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new PSF client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use PSFFasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public PSFClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewPSFSession<Input, Output, Context>(
                IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
        {
            return InternalNewPSFSession<Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
        }

        internal PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalNewPSFSession<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.For(indexingFunctions).NewSession(indexingFunctions, sessionId, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this.Log, indexingFunctions, session, session.SupportAsync, livenessSession, this.psfManager);
        }

        /// <summary>
        /// Start a new PSF client session wrapper around a FASTER client session.
        /// For performance reasons, please use PSFFasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumePSFSession<TInput, TOutput, TContext>(
                IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => ResumePSFSession(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new PSF client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use PSFFasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumePSFSession<TInput, TOutput, TContext>(
                IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
        {
            return InternalResumePSFSession<TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
        }

        internal PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalResumePSFSession<TInput, TOutput, TContext, Functions>(Functions functions,
                                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.For(indexingFunctions).ResumeSession(indexingFunctions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this.Log, indexingFunctions, session, session.SupportAsync, livenessSession, this.psfManager);
        }

        #endregion New Session Operations

        #region Growth and Recovery

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GrowIndex()
            => this.fkv.GrowIndex() && this.psfManager.GrowIndex();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token) && this.psfManager.TakeFullCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token, checkpointType) && this.psfManager.TakeFullCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            return (success && await this.psfManager.TakeFullCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeIndexCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            => this.fkv.TakeIndexCheckpoint(out token) && this.psfManager.TakeIndexCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeIndexCheckpointAsync(cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            return (success && await this.psfManager.TakeIndexCheckpointAsync(cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token) && this.psfManager.TakeHybridLogCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token, checkpointType) && this.psfManager.TakeHybridLogCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            return (success && await this.psfManager.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            // TODO: RecoverAsync with separate Tasks for primary fkv and each psfGroup
            this.fkv.Recover(numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            this.fkv.Recover(fullcheckpointToken, numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover(fullcheckpointToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            this.fkv.Recover(indexToken, hybridLogToken, numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover(indexToken, hybridLogToken);
        }

        /// <inheritdoc/>
        public async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            // Simple sequence to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            var vt1 = this.fkv.CompleteCheckpointAsync(token);
            var vt2 = this.psfManager.CompleteCheckpointAsync(token);
            await vt1;
            await vt2;
        }

        #endregion Growth and Recovery

        #region Other Operations

        /// <inheritdoc/>
        public long EntryCount => this.fkv.EntryCount;

        /// <inheritdoc/>
        public long IndexSize => this.fkv.IndexSize;

        /// <inheritdoc/>
        public IFasterEqualityComparer<TKVKey> Comparer => this.fkv.Comparer;

        /// <inheritdoc/>
        public string DumpDistribution() => this.fkv.DumpDistribution();

        /// <inheritdoc/>
        public LogAccessor<TKVKey, TKVValue> Log => this.fkv.Log;

        /// <inheritdoc/>
        public LogAccessor<TKVKey, TKVValue> ReadCache => this.fkv.ReadCache;

        #endregion Other Operations

        /// <inheritdoc/>
        public void Dispose() => this.fkv.Dispose();

        #endregion IFasterKV implementations

        #region Flush implementations (LogAccessor is not available on PSFFasterKV)
        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait)
        {
            this.fkv.Log.Flush(wait);
            this.psfManager.Flush(wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Wait for operation to complete</param>
        public void FlushAndEvict(bool wait)
        {
            this.fkv.Log.FlushAndEvict(wait);
            this.psfManager.FlushAndEvict(wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeFromMemory()
        {
            this.fkv.Log.DisposeFromMemory();
            this.psfManager.DisposeFromMemory();
        }
        #endregion Flush implementations (LogAccessor is not available on PSFFasterKV)
    }
}
