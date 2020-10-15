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

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.PSF
{
    // PSF-enabled wrapper FasterKV
    public class PSFFasterKV<TKVKey, TKVValue> : IFasterKV<TKVKey, TKVValue>
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
        /// Provides a PSF wrapper for a <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        public static PSFFasterKV<TKVKey, TKVValue> GetOrCreateWrapper(FasterKV<TKVKey, TKVValue> fkv) 
            => fkvDictionary.TryGetValue(fkv, out var psfKV)
                ? psfKV
                : fkvDictionary.GetOrAdd(fkv, new PSFFasterKV<TKVKey, TKVValue>(fkv));

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

        public ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> NewSession<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => throw new PSFInvalidOperationException("Must use NewPSFSession");

        public ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> ResumeSession<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId, 
                                out CommitPoint commitPoint, bool threadAffinitized = false, IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => throw new PSFInvalidOperationException("Must use ResumePSFSession");

        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> NewPSFSession<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var wrapperFunctions = new WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.For(wrapperFunctions).NewSession<Functions>(sessionId, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this.Log, wrapperFunctions, session, livenessFunctions, livenessSession, this.psfManager);
        }

        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions> ResumePSFSession<TInput, TOutput, TContext, Functions>(Functions functions, string sessionId, 
                                out CommitPoint commitPoint, bool threadAffinitized = false, IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
        {
            var wrapperFunctions = new WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.For(wrapperFunctions).ResumeSession<Functions>(sessionId, out commitPoint, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, Functions>(this.Log, wrapperFunctions, session, livenessFunctions, livenessSession, this.psfManager);
        }

        #endregion New Session Operations

        #region Growth and Recovery

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GrowIndex()
            => this.fkv.GrowIndex() && this.psfManager.GrowIndex();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token) && this.psfManager.TakeFullCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token, checkpointType) && this.psfManager.TakeFullCheckpoint(checkpointType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            return (success && await this.psfManager.TakeFullCheckpointAsync(checkpointType, cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeIndexCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            => this.fkv.TakeIndexCheckpoint(out token) && this.psfManager.TakeIndexCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeIndexCheckpointAsync(cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            return (success && await this.psfManager.TakeIndexCheckpointAsync(cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token) && this.psfManager.TakeHybridLogCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token, checkpointType) && this.psfManager.TakeHybridLogCheckpoint(checkpointType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            return (success && await this.psfManager.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            // TODO: RecoverAsync with separate Tasks for primary fkv and each psfGroup
            this.fkv.Recover(numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid fullcheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            this.fkv.Recover(fullcheckpointToken, numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover(fullcheckpointToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            this.fkv.Recover(indexToken, hybridLogToken, numPagesToPreload, undoFutureVersions);
            this.psfManager.Recover(indexToken, hybridLogToken);
        }

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

        public long EntryCount => this.fkv.EntryCount;

        public long IndexSize => this.fkv.IndexSize;

        public IFasterEqualityComparer<TKVKey> Comparer => this.fkv.Comparer;

        public string DumpDistribution() => this.fkv.DumpDistribution();

        public LogAccessor<TKVKey, TKVValue> Log => this.fkv.Log;

        public LogAccessor<TKVKey, TKVValue> ReadCache => this.fkv.ReadCache;

        #endregion Other Operations

        public void Dispose() => this.fkv.Dispose();

        #endregion IFasterKV implementations
    }
}
