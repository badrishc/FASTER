// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetHashIndex;
using System;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.SubsetHashIndex
{
    /// <summary>
    /// SubsetHashIndex-enabled internal wrapper for <see cref="FasterKV{TKVKey, TKVValue}"/>. This overrides base methods that 
    /// must be handled differently for SHI-enabled FKVs; the implementation of those different methods, as well as the additional
    /// methods for SHI, is in <see cref="SubsetHashIndexExtensions"/>.
    /// </summary>
    internal class FasterKVForSHI<TKVKey, TKVValue> : FasterKV<TKVKey, TKVValue>
    {
        internal SubsetHashIndex<FasterKVProviderData<TKVKey, TKVValue>, long> SubsetHashIndex { get; }

        /// <summary>
        /// Constructs a <see cref="FasterKVForSHI{Key, Value}"/> subclass of <see cref="FasterKV{Key, Value}"/>.
        /// </summary>
        public FasterKVForSHI(long size, LogSettings logSettings,
                           CheckpointSettings checkpointSettings = null, SerializerSettings<TKVKey, TKVValue> serializerSettings = null,
                           IFasterEqualityComparer<TKVKey> comparer = null,
                           VariableLengthStructSettings<TKVKey, TKVValue> variableLengthStructSettings = null)
            : base(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
            this.SubsetHashIndex = new SubsetHashIndex<FasterKVProviderData<TKVKey, TKVValue>, long>();
        }

        #region New Session Operations

        #region Unavailable New Session Operations

        const string MustUseSubsetHashIndexSessionErr = "SubsetHashIndex sessions must use ForSHI instead of For and NewSessionForSHI instead of NewSession";

        /// <inheritdoc/>

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
        public override AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        public override ClientSession<TKVKey, TKVValue, Input, Output, Context, IFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        /// <inheritdoc/>
        public override AdvancedClientSession<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> ResumeSession<Input, Output, Context>(IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => throw new InvalidOperationExceptionSHI(MustUseSubsetHashIndexSessionErr);

        #endregion Unavailable New Session Operations

        internal ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalNewSessionForSHI<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.Log, base.RecordAccessor, this.SubsetHashIndex);
            var session = base.For(indexingFunctions).NewSession(indexingFunctions, sessionId, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>(this);
            var livenessSession = base.NewSession(livenessFunctions);
            return new ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this, indexingFunctions, session, session.SupportAsync, livenessSession, this.SubsetHashIndex);
        }

        internal ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> InternalResumeSessionForSHI<TInput, TOutput, TContext, Functions>(Functions functions,
                                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            where Functions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var indexingFunctions = new IndexingFunctions<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(functions, this.Log, base.RecordAccessor, this.SubsetHashIndex);
            var session = base.For(indexingFunctions).ResumeSession(indexingFunctions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>(this);
            var livenessSession = base.NewSession(livenessFunctions);
            return new ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this, indexingFunctions, session, session.SupportAsync, livenessSession, this.SubsetHashIndex);
        }

        #endregion New Session Operations

        #region Growth and Recovery

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool GrowIndex()
            => base.GrowIndex() && this.SubsetHashIndex.GrowIndex();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeFullCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            => base.TakeFullCheckpoint(out token) && this.SubsetHashIndex.TakeFullCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            => base.TakeFullCheckpoint(out token, checkpointType) && this.SubsetHashIndex.TakeFullCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeFullCheckpoint
            return (success && await this.SubsetHashIndex.TakeFullCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeIndexCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeIndexCheckpoint
            => base.TakeIndexCheckpoint(out token) && this.SubsetHashIndex.TakeIndexCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeIndexCheckpointAsync(cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeIndexCheckpoint
            return (success && await this.SubsetHashIndex.TakeIndexCheckpointAsync(cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeHybridLogCheckpoint(out Guid token)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            => base.TakeHybridLogCheckpoint(out token) && this.SubsetHashIndex.TakeHybridLogCheckpoint();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            => base.TakeHybridLogCheckpoint(out token, checkpointType) && this.SubsetHashIndex.TakeHybridLogCheckpoint(checkpointType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await base.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the Index token here. TODO: Handle failure of SubsetHashIndex.TakeHybridLogCheckpoint
            return (success && await this.SubsetHashIndex.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken), token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            // TODO: RecoverAsync with separate Tasks for primary fkv and each Group
            base.Recover(numPagesToPreload, undoFutureVersions);
            this.SubsetHashIndex.Recover();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            base.Recover(fullcheckpointToken, numPagesToPreload, undoFutureVersions);
            this.SubsetHashIndex.Recover(fullcheckpointToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            base.Recover(indexToken, hybridLogToken, numPagesToPreload, undoFutureVersions);
            this.SubsetHashIndex.Recover(indexToken, hybridLogToken);
        }

        /// <inheritdoc/>
        public override async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            // Simple sequence to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            var vt1 = base.CompleteCheckpointAsync(token);
            var vt2 = this.SubsetHashIndex.CompleteCheckpointAsync(token);
            await vt1;
            await vt2;
        }

        #endregion Growth and Recovery

        /// <inheritdoc/>
        public override void Dispose() => base.Dispose();
    }
}
