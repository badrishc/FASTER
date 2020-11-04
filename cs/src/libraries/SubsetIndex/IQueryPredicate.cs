// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// Provides an interface on the <see cref="Group{TProviderData, TPKey, TRecordId}"/> that decouples the
    /// Group from the primary FasterKV's TKVKey and TKVValue.
    /// </summary>
    /// <typeparam name="TPKey"></typeparam>
    /// <typeparam name="TRecordId"></typeparam>
    public interface IQueryPredicate<TPKey, TRecordId>
    {
        /// <summary>
        /// Issues a query on the specified <see cref="Predicate{TPKey, TRecordId}"/> to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="predOrdinal">The ordinal of the <see cref="Predicate{TPKey, TRecordId}"/> in this group</param>
        /// <param name="key">The key to query on to rertrieve the <typeparamref name="TRecordId"/>s.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns></returns>
        IEnumerable<TRecordId> Query(IDisposable sessionObj, int predOrdinal, TPKey key, QuerySettings querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Issues a query on the specified <see cref="Predicate{TPKey, TRecordId}"/> to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <param name="sessionObj">The FKV session for this group, held by the <see cref="ClientSessionSI{TProviderData, TRecordId}"/></param>
        /// <param name="predOrdinal">The ordinal of the <see cref="Predicate{TPKey, TRecordId}"/> in this group</param>
        /// <param name="key">The key to query on to rertrieve the <typeparamref name="TRecordId"/>s.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns></returns>
        IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, int predOrdinal, TPKey key, QuerySettings querySettings);
#endif
    }
}
