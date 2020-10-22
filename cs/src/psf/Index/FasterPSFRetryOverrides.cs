// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace PSF.Index
{
    internal partial class PSFSecondaryFasterKV<TPSFKey, TRecordId> : FasterKV<TPSFKey, TRecordId>
    {
        internal override OperationStatus RetryOperationStatus<Input, Output, Context, FasterSession>(FasterExecutionContext<Input, Output, Context> currentCtx,
                                                                        ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
        {
            OperationStatus internalStatus;
            switch (pendingContext.type)
            {
                case OperationType.READ:
                    internalStatus = this.PsfInternalRead(ref pendingContext.key.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.output,
                                         pendingContext.recordInfo.PreviousAddress,
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.INSERT:
                    internalStatus = this.PsfInternalInsert(ref pendingContext.key.Get(),
                                         ref pendingContext.value.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    // If this assert fires, we'll have to virtualize the retry and callback switches in InternalCompleteRetryRequest.
                    Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "PSF insertion should not go pending");
                    break;
                default:
                    throw new PSFInternalErrorException($"PSF implementation should not be retrying operation {pendingContext.type}");
            };

            return internalStatus;
        }
    }
}
