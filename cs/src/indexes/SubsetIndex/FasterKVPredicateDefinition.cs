// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.libraries.SubsetIndex;
using System;

namespace FASTER.indexes.SubsetIndex
{
    /// <summary>
    /// The definition of a single Predicate in the index
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the primary FasterKV instance</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the primary FasterKV instance</typeparam>
    /// <typeparam name="TPKey">The type of the key returned by the Predicate and store in the secondary
    ///     (Index-implementing) FasterKV instances</typeparam>
    public class FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey> : IPredicateDefinition<FasterKVProviderData<TKVKey, TKVValue>, TPKey>
        where TPKey : struct
    {
        /// <summary>
        /// The definition of the delegate used to obtain a new key matching the Value for this Predicate definition.
        /// </summary>
        /// <param name="kvKey">The key sent to FasterKV on Upsert or RMW</param>
        /// <param name="kvValue">The value sent to FasterKV on Upsert or RMW</param>
        /// <remarks>This must be a delegate instead of a lambda to allow ref parameters</remarks>
        /// <returns>Null if the value does not match the predicate, else a key for the value in the Index hash table</returns>
        public delegate TPKey? PredicateFunc(ref TKVKey kvKey, ref TKVValue kvValue);

        /// <summary>
        /// The predicate function that will be called by FasterKV on Upsert or RMW.
        /// </summary>
        public PredicateFunc Predicate;

        /// <summary>
        /// Executes the Predicate
        /// </summary>
        /// <param name="record">The record obtained from the primary FasterKV instance</param>
        /// <returns></returns>
        /// <returns>Null if the value does not match the predicate, else a key for the value in the Index hash table</returns>
        public TPKey? Execute(FasterKVProviderData<TKVKey, TKVValue> record) 
            => Predicate(ref record.GetKey(), ref record.GetValue());

        /// <summary>
        /// The Name of the Predicate, assigned by the caller. Must be unique among all Predicates.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Instantiates the instance with the name and predicate delegate
        /// </summary>
        /// <param name="name"></param>
        /// <param name="predicate"></param>
        public FasterKVPredicateDefinition(string name, PredicateFunc predicate)
        {
            this.Name = name;
            this.Predicate = predicate;
        }

        /// <summary>
        /// Instantiates the instance with the name and predicate Func{}, which we wrap in a delegate.
        /// This allows a streamlined API call.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="predicate"></param>
        public FasterKVPredicateDefinition(string name, Func<TKVKey, TKVValue, TPKey?> predicate)
        {
            TPKey? wrappedPredicate(ref TKVKey key, ref TKVValue value) => predicate(key, value);

            this.Name = name;
            this.Predicate = wrappedPredicate;
        }
    }
}
 