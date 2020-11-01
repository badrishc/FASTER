// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.libraries.SubsetHashIndex;
using System;
using System.Linq;

namespace FASTER.indexes.SubsetHashIndex
{
    /// <summary>
    /// Extension methods on <see cref="FasterKVForSHI{TKVKey, TKVValue}"/> to support SHI.
    /// </summary>
    public static class ExtensionsForSHI
    {
        /// <summary>
        /// Obtain an instance of <see cref="FasterKV{Key, Value}"/> that supports a SubsetHashIndex.
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
        public static FasterKV<TKVKey, TKVValue> NewFasterKVForSHI<TKVKey, TKVValue>(long size, LogSettings logSettings,
                CheckpointSettings checkpointSettings = null, SerializerSettings<TKVKey, TKVValue> serializerSettings = null,
                IFasterEqualityComparer<TKVKey> comparer = null,
                VariableLengthStructSettings<TKVKey, TKVValue> variableLengthStructSettings = null) 
            => new FasterKVForSHI<TKVKey, TKVValue>(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);

        private static FasterKVForSHI<TKVKey, TKVValue> GetForSHI<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv)
            => fkv is FasterKVForSHI<TKVKey, TKVValue> fkvShi ? fkvShi : throw new FasterException("Instance is not a FasterKVForSHI<TKVKey, TKVValue>");

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
            => GetForSHI(fkv).SubsetHashIndex.Register(registrationSettings, def);

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
            => GetForSHI(fkv).SubsetHashIndex.Register(registrationSettings, defs);

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
            => GetForSHI(fkv).SubsetHashIndex.Register(registrationSettings, new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(predName, predFunc));

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
            => GetForSHI(fkv).SubsetHashIndex.Register(registrationSettings, predFuncs.Select(e => new FasterKVPredicateDefinition<TKVKey, TKVValue, TPKey>(e.name, e.func)).ToArray());

        #endregion Predicate Registration API

        #region New Session operations
        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <returns></returns>
        public static ClientSessionBuilderForSHI<TKVKey, TKVValue, Input, Output, Context> ForSHI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
            => new ClientSessionBuilderForSHI<TKVKey, TKVValue, Input, Output, Context>(GetForSHI(fkv), functions);

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types for advanced client sessions
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <returns></returns>
        public static AdvancedClientSessionBuilderForSHI<TKVKey, TKVValue, Input, Output, Context> ForSHI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions) 
            => new AdvancedClientSessionBuilderForSHI<TKVKey, TKVValue, Input, Output, Context>(GetForSHI(fkv), functions);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> NewSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSHI(fkv).NewSessionForSHI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSHI<TKVKey, TKVValue, Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>> NewSessionForSHI<TKVKey, TKVValue, Input, Output, Context>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
            => GetForSHI(fkv).InternalNewSessionForSHI<Input, Output, Context, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).ResumeSessionForSHI&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSHI(fkv).ResumeSessionForSHI(new BasicFunctionsWrapper<TKVKey, TKVValue, TInput, TOutput, TContext>(functions), sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

        /// <summary>
        /// Start a new SubsetHashIndex-enabled client session wrapper around a FASTER client session with advanced functions.
        /// For performance reasons, please use FasterKVForSHI&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public static ClientSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>> ResumeSessionForSHI<TKVKey, TKVValue, TInput, TOutput, TContext>(
                this FasterKV<TKVKey, TKVValue> fkv, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext> functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<TKVValue, TInput> sessionVariableLengthStructSettings = null)
            => GetForSHI(fkv).InternalResumeSessionForSHI<TInput, TOutput, TContext, IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);

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
            GetForSHI(fkv).SubsetHashIndex.Flush(wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        /// <param name="wait">Wait for operation to complete</param>
        public static void FlushAndEvict<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv, bool wait)
        {
            fkv.Log.FlushAndEvict(wait);
            GetForSHI(fkv).SubsetHashIndex.FlushAndEvict(wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        /// <param name="fkv">The <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</param>
        public static void DisposeFromMemory<TKVKey, TKVValue>(this FasterKV<TKVKey, TKVValue> fkv)
        {
            fkv.Log.DisposeFromMemory();
            GetForSHI(fkv).SubsetHashIndex.DisposeFromMemory();
        }

        #endregion Flush implementations
    }
}
