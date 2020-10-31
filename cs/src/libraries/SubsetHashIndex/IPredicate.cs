// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.libraries.SubsetHashIndex
{
    /// <summary>
    /// A base interface for <see cref="Predicate{TPKey, TRecordId}"/> to decouple the generic type parameters.
    /// </summary>
    public interface IPredicate
    {
        /// <summary>
        /// The name of the <see cref="Predicate{TPKey, TRecordId}"/>; must be unique among all
        ///     <see cref="Group{TProviderData, TPKey, TRecordId}"/>s.
        /// </summary>
        string Name { get; }
    }
}
