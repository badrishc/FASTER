﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace PSF.Index
{
    internal partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        internal PSFSecondaryFasterKV(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<TPSFKey, TRecordId> serializerSettings = null,
            IFasterEqualityComparer<TPSFKey> comparer = null,
            VariableLengthStructSettings<TPSFKey, TRecordId> variableLengthStructSettings = null)
            : base (size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfRead<TInput, TOutput, TContext, FasterSession>(ref TPSFKey key, ref TInput input, ref TOutput output, long startAddress, ref TContext context,
                                        FasterSession fasterSession, long serialNo, FasterExecutionContext<TInput, TOutput, TContext> sessionCtx)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var internalStatus = this.PsfInternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        internal override OperationStatus RetryOperation<Input, Output, Context, FasterSession>(FasterExecutionContext<Input, Output, Context> currentCtx,
                                                                        ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
        {
            OperationStatus internalStatus;
            switch (pendingContext.type)
            {
                case OperationType.READ:
                    internalStatus = this.PsfInternalRead(ref pendingContext.key.Get(),
                                         ref pendingContext.input,
                                         ref pendingContext.output,
                                         pendingContext.recordInfo.PreviousAddress,
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.INSERT:
                    internalStatus = this.PsfInternalInsert(ref pendingContext.key.Get(),
                                         ref pendingContext.value.Get(),
                                         ref pendingContext.input,
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "PSF insertion should not go pending");
                    break;
                default:
                    throw new PSFInternalErrorException($"PSF implementation should not be retrying operation {pendingContext.type}");
            };

            return internalStatus;
        }

        internal ValueTask<ReadAsyncResult<TInput, TOutput, TContext, TFunctions>> ContextPsfReadAsync<TInput, TOutput, TContext, TFunctions>(
                                        ClientSession<TPSFKey, TRecordId, TInput, TOutput, TContext, TFunctions> clientSession,
                                        ref TPSFKey key, ref TInput input, long startAddress, ref TContext context, long serialNo, PSFQuerySettings querySettings)
            where TFunctions : IFunctions<TPSFKey, TRecordId, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var diskRequest = default(AsyncIOContext<TPSFKey, TRecordId>);
            var output = default(TOutput);

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
                var internalStatus = this.PsfInternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, clientSession.FasterSession, clientSession.ctx, serialNo);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext, TFunctions>>(new ReadAsyncResult<TInput, TOutput, TContext, TFunctions>((Status)internalStatus, output, pcontext.recordInfo));
                }

                else
                {
                    var status = HandleOperationStatus(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext, TFunctions>>(new ReadAsyncResult<TInput, TOutput, TContext, TFunctions>(status, output, pcontext.recordInfo));
                }
            }
            finally
            {
                Debug.Assert(serialNo >= clientSession.ctx.serialNum, "Operation serial numbers must be non-decreasing");
                clientSession.ctx.serialNum = serialNo;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, clientSession, pcontext, diskRequest, querySettings.CancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfInsert<Input, Output, Context, FasterSession>(ref TPSFKey key, ref TRecordId value, 
                                         ref Input input, ref Context context,
                                         FasterSession fasterSession, long serialNo,
                                         FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input, ref context,
                                                      ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfUpdate<Input, Output, Context, FasterSession, TProviderData>(ref GroupCompositeKeyPair groupKeysPair, ref TRecordId value, 
                                                                   ref Input input, ref Context context,
                                                                   FasterSession fasterSession, long serialNo,
                                                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                   PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var groupKeys = groupKeysPair.Before;

            (context as IInputAccessor<Input>).SetDelete(ref input, true);

            var internalStatus = this.PsfInternalInsert(ref groupKeys.CastToKeyRef<TPSFKey>(), ref value, ref input, ref context,
                                                        ref pcontext, fasterSession, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;

            if (status == Status.OK)
            {
                value = changeTracker.AfterRecordId;
                return PsfRcuInsert(groupKeysPair.After, ref value, ref input, ref context, ref pcontext, fasterSession, sessionCtx, serialNo + 1);
            }
            return status;
        }

        private Status PsfRcuInsert<Input, Output, Context, FasterSession>(GroupCompositeKey groupKeys, ref TRecordId value, ref Input input,
                                    ref Context context, ref PendingContext<Input, Output, Context> pcontext, FasterSession fasterSession, 
                                    FasterExecutionContext<Input, Output, Context> sessionCtx, long serialNo)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, Input, Output, Context>
        {
            (context as IInputAccessor<Input>).SetDelete(ref input, false);
            var internalStatus = this.PsfInternalInsert(ref groupKeys.CastToKeyRef<TPSFKey>(), ref value, ref input, ref context,
                                                        ref pcontext, fasterSession, sessionCtx, serialNo);
            return internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfDelete<Input, Output, Context, FasterSession>(ref TPSFKey key, ref TRecordId value, ref Input input, 
                                                                   ref Context context, FasterSession fasterSession,
                                                                   FasterExecutionContext<Input, Output, Context> sessionCtx, long serialNo)
            where FasterSession : IFasterSession<TPSFKey, TRecordId, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);

            (context as IInputAccessor<Input>).SetDelete(ref input, true);

            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;
            return status;
        }
    }
}
