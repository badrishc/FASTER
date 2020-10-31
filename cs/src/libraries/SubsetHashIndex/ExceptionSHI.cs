// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// SubsetHashIndex exception base type
    /// </summary>
    public class ExceptionSHI : FasterException
    {
        public ExceptionSHI() { }

        public ExceptionSHI(string message) : base(message) { }

        public ExceptionSHI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetHashIndex argument exception type
    /// </summary>
    public class ArgumentExceptionSHI : ExceptionSHI
    {
        public ArgumentExceptionSHI() { }

        public ArgumentExceptionSHI(string message) : base(message) { }

        public ArgumentExceptionSHI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetHashIndex argument exception type
    /// </summary>
    public class InvalidOperationExceptionSHI : ExceptionSHI
    {
        public InvalidOperationExceptionSHI() { }

        public InvalidOperationExceptionSHI(string message) : base(message) { }

        public InvalidOperationExceptionSHI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetHashIndex argument exception type
    /// </summary>
    public class InternalErrorExceptionSHI : ExceptionSHI
    {
        public InternalErrorExceptionSHI() { }

        public InternalErrorExceptionSHI(string message) : base($"Internal Error: {message}") { }

        public InternalErrorExceptionSHI(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
