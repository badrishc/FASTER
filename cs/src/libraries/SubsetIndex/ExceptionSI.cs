// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.libraries.SubsetIndex
{
    /// <summary>
    /// SubsetIndex exception base type
    /// </summary>
    public class ExceptionSI : FasterException
    {
        public ExceptionSI() { }

        public ExceptionSI(string message) : base(message) { }

        public ExceptionSI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class ArgumentExceptionSI : ExceptionSI
    {
        public ArgumentExceptionSI() { }

        public ArgumentExceptionSI(string message) : base(message) { }

        public ArgumentExceptionSI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class InvalidOperationExceptionSI : ExceptionSI
    {
        public InvalidOperationExceptionSI() { }

        public InvalidOperationExceptionSI(string message) : base(message) { }

        public InvalidOperationExceptionSI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class InternalErrorExceptionSI : ExceptionSI
    {
        public InternalErrorExceptionSI() { }

        public InternalErrorExceptionSI(string message) : base($"Internal Error: {message}") { }

        public InternalErrorExceptionSI(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
