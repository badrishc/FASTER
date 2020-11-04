// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading.Tasks;

namespace FASTER.libraries.SubsetIndex
{
    internal static class SessionExtensions
    {
        internal static Status IndexRead<Key, Value, Input, Output, Context, Functions>(this AdvancedClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    FasterKVSI<Key, Value> fkv, ref Key key,
                                    ref Input input, ref Output output, ref RecordInfo recordInfo, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexRead(ref key, ref input, ref output, ref recordInfo, ref context, session.FasterSession, serialNo, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> IndexReadAsync<Key, Value, Input, Output, Context, Functions>(
                                    this AdvancedClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    FasterKVSI<Key, Value> fkv, ref Key key, ref Input input, long startAddress,
                                    ref Context context, long serialNo, QuerySettings querySettings)
            where Key : struct
            where Value : struct
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            return fkv.ContextIndexReadAsync(session.FasterSession, session.ctx, ref key, ref input, startAddress, ref context, serialNo, querySettings);
        }

        internal static Status IndexInsert<Key, Value, Input, Output, Context, Functions>(this AdvancedClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    FasterKVSI<Key, Value> fkv,
                                    ref Key key, ref Value value, ref Input input, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            // Called on the secondary FasterKV
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexInsert(ref key, ref value, ref input, ref context, session.FasterSession, serialNo, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static Status IndexUpdate<Key, Value, Input, Output, Context, Functions, TProviderData>(this AdvancedClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    FasterKVSI<Key, Value> fkv, ref GroupCompositeKeyPair groupKeysPair, ref Value value, ref Input input,
                                    ref Context context, long serialNo, ChangeTracker<TProviderData, Value> changeTracker)
            where Key : struct
            where Value : struct
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexUpdate(ref groupKeysPair, ref value, ref input, ref context, session.FasterSession, serialNo, session.ctx, changeTracker);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static Status IndexDelete<Key, Value, Input, Output, Context, Functions>(this AdvancedClientSession<Key, Value, Input, Output, Context, Functions> session,
                                    FasterKVSI<Key, Value> fkv, ref Key key, ref Value value, ref Input input, ref Context context, long serialNo)
            where Key : struct
            where Value : struct
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexDelete(ref key, ref value, ref input, ref context, session.FasterSession, session.ctx, serialNo);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }
    }
}
