// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// Defines settings that control some behaviors of the Query execution.
    /// </summary>
    public class QuerySettings
    {
        /// <summary>One or more streams has ended. Inputs are the Predicate whose stream ended and the index of that Predicate in the parameters,
        ///     identified by the 0-based ordinal of the TPKey type (TPKey1, TPKey2, or TPKey3 in the Query overloads) and
        ///     the 0-based ordinal of the Predicate within the TPKey type.</summary>
        /// <returns>true to continue the enumeration, else false</returns>
        public Func<IPredicate, (int keyTypeOrdinal, int predOrdinal), bool> OnStreamEnded;

        /// <summary>Cancel the enumeration if set. Can be set by another thread, e.g. one presenting results to a UI, or by StreamEnded.</summary>
        public CancellationToken CancellationToken { get; set; }

        /// <summary> When cancellation is reqested, simply terminate the enumeration without throwing a CancellationException.</summary>
        public bool ThrowOnCancellation { get; set; }

        /// <summary>Checks for cancellation and throws if requested</summary>        
        public bool IsCanceled
        { 
            get
            {
                if (this.CancellationToken.IsCancellationRequested)
                {
                    if (this.ThrowOnCancellation)
                        CancellationToken.ThrowIfCancellationRequested();
                    return true;
                }
                return false;
            }
        }

        internal bool CancelOnEOS(IPredicate pred, (int, int) location) => this.OnStreamEnded is {} && !this.OnStreamEnded(pred, location);

        /// <summary>
        /// Default query settings; let all streams continue to completion.
        /// </summary>
        public static readonly QuerySettings Default = new QuerySettings { OnStreamEnded = (unusedPred, unusedIndex) => true };
    }
}
