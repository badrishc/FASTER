// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// The definition of a single Predicate in the SubsetHashIndex
    /// </summary>
    /// <typeparam name="TProviderData">The data from the provider that the Predicate should be run over</typeparam>
    /// <typeparam name="TPKey">The key returned by the Predicate</typeparam>
    public interface IPredicateDefinition<TProviderData, TPKey>
        where TPKey : struct
    {
        /// <summary>
        /// The callback used to obtain a new TPKey for the ProviderData record for this Predicate definition.
        /// </summary>
        /// <param name="record">The representation of the data that was written to the primary store
        ///     (e.g. Upsert in FasterKV).</param>
        /// <returns>Null if the record does not match the Predicate, else the indexing key for the record</returns>
        public TPKey? Execute(TProviderData record);

        /// <summary>
        /// The Name of the Predicate, assigned by the caller. Must be unique among Predicates in the group. It is
        /// used by the caller to index Predicates in the group in a friendly way.
        /// </summary>
        public string Name { get; }

    }
}
