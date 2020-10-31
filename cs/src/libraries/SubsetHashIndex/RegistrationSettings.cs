// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// Options for Predicate registration.
    /// </summary>
    public class RegistrationSettings<TPKey>
    {
        /// <summary>
        /// When registring new Indexes over an existing store, this is the logicalAddress in the primary
        /// FasterKV at which indexing will be started. TODO: LogicalAddress is FasterKV-specific; revisit when indexing existing records.
        /// </summary>
        public long IndexFromAddress = core.Constants.kInvalidAddress;

        /// <summary>
        /// The hash table size to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public long HashTableSize = 0;

        /// <summary>
        /// The log settings to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public LogSettings LogSettings;

        /// <summary>
        /// The log settings to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public CheckpointSettings CheckpointSettings;

        /// <summary>
        /// Optional key comparer; if null, <typeparamref name="TPKey"/> should implement
        ///     <see cref="IFasterEqualityComparer{TPKey}"/>; otherwise a slower EqualityComparer will be used.
        /// </summary>
        public IFasterEqualityComparer<TPKey> KeyComparer;

        /// <summary>
        /// Indicates whether Group Sessions are thread-affinitized.
        /// </summary>
        public bool ThreadAffinitized;

        /// <summary>
        /// The size of the first IPU Cache; inserts are done into this cache only. If zero, no caching is done. // TODOCache
        /// </summary>
        public long IPU1CacheSize = 0;

        /// <summary>
        /// The size of the second IPU Cache; inserts are not done into this cache, so more distant records
        /// are likelier to remain. If this is nonzero, <see cref="IPU1CacheSize"/> must also be nonzero.   // TODOCache
        /// </summary>
        public long IPU2CacheSize = 0;
    }
}
