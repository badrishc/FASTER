// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.SubsetHashIndex
{
    internal struct BasicFunctionsWrapper<Key, Value, Input, Output, Context> : IAdvancedFunctions<Key, Value, Input, Output, Context>
    {
        private readonly IFunctions<Key, Value, Input, Output, Context> _functions;

        public BasicFunctionsWrapper(IFunctions<Key, Value, Input, Output, Context> functions) => _functions = functions;

        public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint) 
            => _functions.CheckpointCompletionCallback(guid, commitPoint);

        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, long logicalAddress)
            => _functions.ConcurrentReader(ref key, ref input, ref value, ref dst);

        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long logicalAddress)
            => _functions.ConcurrentWriter(ref key, ref src, ref dst);

        public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue)
            => _functions.NeedCopyUpdate(ref key, ref input, ref oldValue);

        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, long oldLogicalAddress, long newLogicalAddress)
            => _functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue);

        public void DeleteCompletionCallback(ref Key key, Context ctx)
            => _functions.DeleteCompletionCallback(ref key, ctx);

        public void InitialUpdater(ref Key key, ref Input input, ref Value value, long logicalAddress)
            => _functions.InitialUpdater(ref key, ref input, ref value);

        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, long logicalAddress)
            => _functions.InPlaceUpdater(ref key, ref input, ref value);

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
            => _functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status);

        public void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status)
            => _functions.RMWCompletionCallback(ref key, ref input, ctx, status);

        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, long logicalAddress)
            => _functions.SingleReader(ref key, ref input, ref value, ref dst);

        public void SingleWriter(ref Key key, ref Value src, ref Value dst, long logicalAddress)
            => _functions.SingleWriter(ref key, ref src, ref dst);

        public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx)
            => _functions.UpsertCompletionCallback(ref key, ref value, ctx);
    }
}
