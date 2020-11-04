// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// The implementation of the Predicate Subset Function.
    /// </summary>
    /// <typeparam name="TPKey">The type of the key returned by the Predicate and store in the secondary
    ///     FasterKV instance</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    internal class Predicate<TPKey, TRecordId> : IPredicate
    {
        private readonly IQueryPredicate<TPKey, TRecordId> group;

        internal long GroupId { get; }          // unique in the SubsetIndex.Groups list

        internal int Ordinal { get; }           // index of the Predicate in the Group

        // Predicates are passed by the caller to the session Query functions, so make sure they don't send
        // a Predicate from a different SubsetIndex.
        internal Guid Id { get; }

        /// <inheritdoc/>
        public string Name { get; }

        internal Predicate(long groupId, int predOrdinal, string name, IQueryPredicate<TPKey, TRecordId> iqp)
        {
            this.GroupId = groupId;
            this.Ordinal = predOrdinal;
            this.Name = name;
            this.group = iqp;
            this.Id = Guid.NewGuid();
        }

        /// <summary>
        /// Issues a query on this Predicate for a given key, to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IEnumerable<TRecordId> Query(IDisposable sessionObj, TPKey key, QuerySettings querySettings) 
            => this.group.Query(sessionObj, this.Ordinal, key, querySettings);

#if NETSTANDARD21
        /// <summary>
        /// Issues a query on this Predicate for a given key, to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IAsyncEnumerable<TRecordId> QueryAsync(IDisposable sessionObj, TPKey key, QuerySettings querySettings) 
            => this.group.QueryAsync(sessionObj, this.Ordinal, key, querySettings);
#endif
    }
}
