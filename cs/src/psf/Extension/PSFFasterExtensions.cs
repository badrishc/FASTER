// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

extern alias FasterCore;

using FC = FasterCore::FASTER.core;

namespace FASTER.PSF
{
    /// <summary>
    /// Extensions to wrap a <see cref="FC.FasterKV{Key, Value}"/> instance with PSF functionality.
    /// </summary>    
    public static class PSFFasterExtensions
    {
        /// <summary>
        /// Provides a PSF-enabled wrapper for a <see cref="FC.FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey"></typeparam>
        /// <typeparam name="TKVValue"></typeparam>
        public static PSFFasterKV<TKVKey, TKVValue> EnablePSFs<TKVKey, TKVValue>(this FC.FasterKV<TKVKey, TKVValue> fkv)
            => PSFFasterKV<TKVKey, TKVValue>.GetOrCreateWrapper(fkv);
    }
}
