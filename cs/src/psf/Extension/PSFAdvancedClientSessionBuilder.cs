﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.PSF
{
    public partial class PSFFasterKV<TKVKey, TKVValue>
    {
        /// <summary>
        /// PSF Client session type helper
        /// </summary>
        public struct PSFAdvancedClientSessionBuilder<Input, Output, Context>
        {
            private readonly PSFFasterKV<TKVKey, TKVValue> _psfFasterKV;
            private readonly IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> _functions;

            internal PSFAdvancedClientSessionBuilder(PSFFasterKV<TKVKey, TKVValue> psfFasterKV, IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context> functions)
            {
                _psfFasterKV = psfFasterKV;
                _functions = functions;
            }

            /// <summary>
            /// Start a new PSFClientSession.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public PSFClientSession<TKVKey, TKVValue, Input, Output, Context, Functions> NewSession<Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
            {
                return _psfFasterKV.InternalNewPSFSession<Input, Output, Context, Functions>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER; used during recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public PSFClientSession<TKVKey, TKVValue, Input, Output, Context, Functions> ResumeSession<Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
            {
                return _psfFasterKV.InternalResumePSFSession<Input, Output, Context, Functions>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Start a new advanced client session with FASTER.
            /// </summary>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public PSFClientSession<TKVKey, TKVValue, Input, Output, Context, Functions> NewSession<Functions>(string sessionId = null, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
            {
                if (_functions is null)
                    throw new FasterException("Functions not provided for session");

                return _psfFasterKV.InternalNewPSFSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER with advanced functions; used during recovery from failure.
            /// </summary>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public PSFClientSession<TKVKey, TKVValue, Input, Output, Context, Functions> ResumeSession<Functions>(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<TKVValue, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<TKVKey, TKVValue, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _psfFasterKV.InternalResumePSFSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }
        }
    }
}
