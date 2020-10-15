// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading.Tasks;

namespace PSF.Index
{
    internal static class SessionExtensions
    {

        internal static Status PsfRead<Key, Value, Input, Output, Context, Functions>(this ClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    PSFSecondaryFasterKV<Key, Value> fkv, ref Key key,
                                    ref Input input, ref Output output, long startAddress, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextPsfRead(ref key, ref input, ref output, startAddress, ref context, session.FasterSession, serialNo, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> PsfReadAsync<Key, Value, Input, Output, Context, Functions>(
                                    this ClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    PSFSecondaryFasterKV<Key, Value> fkv, ref Key key, ref Input input, ref Output output, long startAddress,
                                    ref Context context, long serialNo, PSFQuerySettings querySettings)
            where Key : struct
            where Value : struct
            where Functions : IFunctions<Key, Value, Input, Output, Context> 
            => fkv.ContextPsfReadAsync(session, ref key, ref input, startAddress, ref context, serialNo, querySettings);

        internal static Status PsfInsert<Key, Value, Input, Output, Context, Functions>(this ClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    PSFSecondaryFasterKV<Key, Value> fkv,
                                    ref Key key, ref Value value, ref Input input, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            // Called on the secondary FasterKV
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextPsfInsert(ref key, ref value, ref input, ref context, session.FasterSession, serialNo, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static Status PsfUpdate<Key, Value, Input, Output, Context, Functions, TProviderData>(this ClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    PSFSecondaryFasterKV<Key, Value> fkv, ref GroupCompositeKeyPair groupKeysPair, ref Value value, ref Input input,
                                    ref Context context, long serialNo, PSFChangeTracker<TProviderData, Value> changeTracker)
            where Key : struct
            where Value : struct
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextPsfUpdate(ref groupKeysPair, ref value, ref input, ref context, session.FasterSession, serialNo, session.ctx, changeTracker);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static Status PsfDelete<Key, Value, Input, Output, Context, Functions>(this ClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    PSFSecondaryFasterKV<Key, Value> fkv, ref Key key, ref Value value, ref Input input, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextPsfDelete(ref key, ref value, ref input, ref context, session.FasterSession, session.ctx, serialNo);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }
    }
}
