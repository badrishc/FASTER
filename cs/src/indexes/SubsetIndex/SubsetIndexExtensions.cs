// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetIndex;
using System;
using System.Linq;

namespace FASTER.indexes.SubsetIndex
{
    /// <summary>
    /// Extension methods on <see cref="FasterKVForSI{TKVKey, TKVValue}"/> to support SubsetIndex.
    /// </summary>
    public static class SubsetIndexExtensions
    {
        /// <summary>
        /// Obtain an instance of <see cref="FasterKV{Key, Value}"/> that supports a SubsetIndex.
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
        public static FasterKV<TKVKey, TKVValue> NewFasterKV<TKVKey, TKVValue>(long size, LogSettings logSettings,
                CheckpointSettings checkpointSettings = null, SerializerSettings<TKVKey, TKVValue> serializerSettings = null,
                IFasterEqualityComparer<TKVKey> comparer = null,
                VariableLengthStructSettings<TKVKey, TKVValue> variableLengthStructSettings = null) 
            => new FasterKVForSI<TKVKey, TKVValue>(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);

        private static FasterKVForSI<TKVKey, TKVValue> GetForSI<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv)
            => fkv is FasterKVForSI<TKVKey, TKVValue> fkvSi ? fkvSi : throw new FasterException("Instance is not a FasterKVForSI<TKVKey, TKVValue>");

        #region Predicate Registration API

        /// <summary>
        /// Register an <see cref="IPredicate"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TKVKey">The type of the key in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TKVValue">The type of the value in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="IPredicate"/></typeparam>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="def">The Predicate definition</param>
        /// <returns>A Predicate implementation(</returns>
        public static IPredicate Register<TKVKey, TKVValue, TPKey>(this FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings,
                                         FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey> def)
            where TPKey : struct
            => GetForSI(fkv).SubsetIndex.Register(registrationSettings, def);

        /// <summary>
        /// Register multiple <see cref="Predicate{TPKey, TRecordId}"/>s with a vector of definitions.
        /// </summary>
        /// <typeparam name="TKVKey">The type of the key in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TKVValue">The type of the value in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="IPredicate"/></typeparam>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="defs">The Predicate definitions</param>
        /// <returns>A Predicate implementation(</returns>
        public static IPredicate[] Register<TKVKey, TKVValue, TPKey>(this FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings,
                                           params FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>[] defs)
            where TPKey : struct
            => GetForSI(fkv).SubsetIndex.Register(registrationSettings, defs);

        /// <summary>
        /// Register a <see cref="Predicate{TPKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TKVKey">The type of the key in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TKVValue">The type of the value in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="IPredicate"/></typeparam>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="predName">The name of the Predicate</param>
        /// <param name="predFunc">The function implementing the Predicate</param>
        /// <returns>A Predicate implementation(</returns>
        public static IPredicate Register<TKVKey, TKVValue, TPKey>(this FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings,
                                         string predName, Func<TKVKey, TKVValue, TPKey?> predFunc)
            where TPKey : struct
            => GetForSI(fkv).SubsetIndex.Register(registrationSettings, new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(predName, predFunc));

        /// <summary>
        /// Register a <see cref="Predicate{TPKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TKVKey">The type of the key in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TKVValue">The type of the value in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
        /// <typeparam name="TPKey">The type of the key returned from the <see cref="IPredicate"/></typeparam>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="predFuncs">A vector of tuples of predicate name and the function implementing the Predicate</param>
        /// <returns>A Predicate implementation(</returns>
        public static IPredicate[] Register<TKVKey, TKVValue, TPKey>(this FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings,
                                           params (string name, Func<TKVKey, TKVValue, TPKey?> func)[] predFuncs)
            where TPKey : struct
            => GetForSI(fkv).SubsetIndex.Register(registrationSettings, predFuncs.Select(e => new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(e.name, e.func)).ToArray());

        #endregion Predicate Registration API

        #region New Session operations
        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <returns></returns>
        public static ClientSessionBuilderForSI<TKVKey, TKVValue, Input, Output, Context> ForSI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
            => new ClientSessionBuilderForSI<TKVKey, TKVValue, Input, Output, Context>(GetForSI(fkv), functions);

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types for advanced client sessions
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <returns></returns>
        public static AdvancedClientSessionBuilderForSI<TKVKey, TKVValue, Input, Output, Context> ForSI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions) 
            => new AdvancedClientSessionBuilderForSI<TKVKey, TKVValue, Input, Output, Context>(GetForSI(fkv), functions);

        /// <summary>
        /// Start a new SubsetIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> NewSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSI(fkv).NewSessionForSI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSI<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSessionForSI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => GetForSI(fkv).InternalNewSessionForSI<Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSI&lt;Key, Value&gt;.For(functions).ResumeSessionForSI&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSI(fkv).ResumeSessionForSI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSI(fkv).InternalResumeSessionForSI<TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

        #endregion New Session operations

        #region Flush implementations

        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public static void Flush<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv, bool wait)
        {
            fkv.Log.Flush(wait);
            GetForSI(fkv).SubsetIndex.Flush(wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="wait">Wait for operation to complete</param>
        public static void FlushAndEvict<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv, bool wait)
        {
            fkv.Log.FlushAndEvict(wait);
            GetForSI(fkv).SubsetIndex.FlushAndEvict(wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        public static void DisposeFromMemory<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv)
        {
            fkv.Log.DisposeFromMemory();
            GetForSI(fkv).SubsetIndex.DisposeFromMemory();
        }

        #endregion Flush implementations
    }
}
