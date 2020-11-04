// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.libraries.SubsetIndex
{
    internal static class Constants
    {
        public const long kInvalidPredicateGroupId = -1;
        public const int kInvalidPredicateOrdinal = 255; // 0-based ordinals; this is also the max count
    }

    /// <summary>
    /// The phase of Index operations in which Predicates are being executed
    /// </summary>
    public enum ExecutionPhase
    {
        /// <summary>
        /// Executing the Predicates to obtain new TPKeys to be inserted into the secondary FKV.
        /// </summary>
        Insert,

        /// <summary>
        /// Lookup in IPUCache or execute the Predicates to obtain TPKeys prior to update, to be compared to those 
        /// obtained after the update to modify the record's Predicate membership in the secondary FKV.
        /// </summary>
        PreUpdate,

        /// <summary>
        /// Executing the Predicates to obtain TPKeys following an update, to be compared to those obtained before
        /// the update to modify the record's Predicate membership in the secondary FKV.
        /// </summary>
        PostUpdate,

        /// <summary>
        /// Lookup in IPUCache to tombstone a record, or execute the Predicates to obtain TPKeys to place a new
        /// tombstoned record.
        /// </summary>
        Delete
    }
}
