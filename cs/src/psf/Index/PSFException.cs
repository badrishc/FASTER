// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace PSF.Index
{
    /// <summary>
    /// FASTER PSF exception base type
    /// </summary>
    public class PSFException : FasterException
    {
        public PSFException() { }

        public PSFException(string message) : base(message) { }

        public PSFException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFArgumentException : PSFException
    {
        public PSFArgumentException() { }

        public PSFArgumentException(string message) : base(message) { }

        public PSFArgumentException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFInvalidOperationException : PSFException
    {
        public PSFInvalidOperationException() { }

        public PSFInvalidOperationException(string message) : base(message) { }

        public PSFInvalidOperationException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFInternalErrorException : PSFException
    {
        public PSFInternalErrorException() { }

        public PSFInternalErrorException(string message) : base($"Internal Error: {message}") { }

        public PSFInternalErrorException(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
