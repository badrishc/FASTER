// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace SubsetHashIndexSample
{
    /// <summary>
    /// FASTER exception base type
    /// </summary>
    public class SampleException : Exception
    {
        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        public SampleException()
        {
        }

        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="message"></param>
        public SampleException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public SampleException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
