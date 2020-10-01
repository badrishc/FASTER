// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using PSF.Index;
using System;
using System.Threading.Tasks;

namespace FASTER.core
{
    public sealed partial class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        // This value is created within the Primary FKV session.
        Lazy<ClientSession<Key, Value, PSFInputPrimaryReadAddress, PSFOutputPrimaryReadAddress<Key, Value>, PSFContext, PSFPrimaryFunctions<Key, Value>>> psfLookupRecordIdSession;

        internal void CreateLazyPsfSessionWrapper() {
            this.psfLookupRecordIdSession = new Lazy<ClientSession<Key, Value, PSFInputPrimaryReadAddress, PSFOutputPrimaryReadAddress<Key, Value>, PSFContext, PSFPrimaryFunctions<Key, Value>>>(
                () => this.fht.NewSession<PSFInputPrimaryReadAddress, PSFOutputPrimaryReadAddress<Key, Value>, PSFContext, PSFPrimaryFunctions<Key, Value>>(
                                                                new PSFPrimaryFunctions<Key, Value>()));
        }

        internal void DisposeLazyPsfSessionWrapper()
        {
            if (!(this.psfLookupRecordIdSession is null) && this.psfLookupRecordIdSession.IsValueCreated)
                this.psfLookupRecordIdSession.Value.Dispose();
        }

        private ClientSession<Key, Value, PSFInputPrimaryReadAddress, PSFOutputPrimaryReadAddress<Key, Value>, PSFContext, PSFPrimaryFunctions<Key, Value>> GetPsfLookupRecordSession() 
            => this.psfLookupRecordIdSession.Value;

        internal Status PsfInsert(ref Key key, ref Value value, ref Input input, ref Context context, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfInsert(ref key, ref value, ref input, ref context, this.FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfReadKey(ref Key key, ref Input input, ref Output output, ref Context context, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfReadKey(ref key, ref input, ref output, ref context, this.FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> PsfReadKeyAsync(
                ref Key key, ref Input input, ref Output output, ref Context context, long serialNo, PSFQuerySettings querySettings)
        {
            // Called on the secondary FasterKV
            return fht.ContextPsfReadKeyAsync(this, ref key, ref input, ref output, ref context, serialNo, ctx, querySettings);
        }

        internal Status PsfReadAddress(ref Input input, ref Output output, ref Context context, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfReadAddress(ref input, ref output, ref context, this.FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> PsfReadAddressAsync(
                ref Input input, ref Output output, ref Context context, long serialNo, PSFQuerySettings querySettings)
        {
            // Called on the secondary FasterKV
            return fht.ContextPsfReadAddressAsync(this, ref input, ref output, ref context, serialNo, ctx, querySettings);
        }

        internal Status PsfUpdate<TProviderData>(ref GroupCompositeKeyPair groupKeysPair, ref Value value, ref Input input, 
                                                 ref Context context, long serialNo,
                                                 PSFChangeTracker<TProviderData, Value> changeTracker)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfUpdate(ref groupKeysPair, ref value, ref input, ref context, this.FasterSession, serialNo, ctx, changeTracker);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfDelete<TProviderData>(ref Key key, ref Value value, ref Input input, ref Context context, long serialNo,
                                                 PSFChangeTracker<TProviderData, Value> changeTracker)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfDelete(ref key, ref value, ref input, ref context, this.FasterSession, serialNo, ctx, changeTracker);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }
    }
}
