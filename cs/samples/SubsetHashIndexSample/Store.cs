// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.SubsetHashIndex;
using FASTER.libraries.SubsetHashIndex;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SubsetHashIndexSample
{
    class Store<TValue, TInput, TOutput, TFunctions, TSerializer>
        where TValue : IOrders
        where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal FasterKV<Key, TValue> FasterKV { get; set; }

        private LogFiles logFiles;

        // MultiGroup Predicates -- different key types, one per group.
        internal IPredicate SizePred, ColorPred, CountBinPred;
        internal IPredicate CombinedSizePred, CombinedColorPred, CombinedCountBinPred;

        internal Store()
        {
            this.logFiles = new LogFiles(SubsetHashIndexSampleApp.useMultiGroups ? 3 : 1);

            this.FasterKV = SubsetHashIndexExtensions.NewFasterKV(
                                1L << 20, this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                SubsetHashIndexSampleApp.useObjectValues ? new SerializerSettings<Key, TValue> { valueSerializer = () => new TSerializer() } : null,
                                new Key.Comparer());

            if (SubsetHashIndexSampleApp.useMultiGroups)
            {
                var groupOrdinal = 0;
                this.SizePred = FasterKV.Register(CreateRegistrationSettings<SizeKey>(groupOrdinal++), nameof(this.SizePred),
                                                    (k, v) => new SizeKey((Constants.Size)v.SizeInt));
                this.ColorPred = FasterKV.Register(CreateRegistrationSettings<ColorKey>(groupOrdinal++), nameof(this.ColorPred),
                                                    (k, v) => new ColorKey(Constants.ColorDict[v.ColorArgb]));
                this.CountBinPred = FasterKV.Register(CreateRegistrationSettings<CountBinKey>(groupOrdinal++), nameof(this.CountBinPred),
                                                    (k, v) => CountBinKey.GetAndVerifyBin(v.Count, out int bin) ? new CountBinKey(bin) : (CountBinKey?)null);
            }
            else
            {
                var preds = FasterKV.Register(CreateRegistrationSettings<CombinedKey>(0),
                                                new (string, Func<Key, TValue, CombinedKey?>)[]
                                                {
                                                    (nameof(this.SizePred), (k, v) => new CombinedKey((Constants.Size)v.SizeInt)),
                                                    (nameof(this.ColorPred), (k, v) => new CombinedKey(Constants.ColorDict[v.ColorArgb])),
                                                    (nameof(this.CountBinPred), (k, v) => CountBinKey.GetAndVerifyBin(v.Count, out int bin)
                                                                                                    ? new CombinedKey(bin) : (CombinedKey?)null)
                                                });
                this.CombinedSizePred = preds[0];
                this.CombinedColorPred = preds[1];
                this.CombinedCountBinPred = preds[2];
            }
        }

        RegistrationSettings<TKey> CreateRegistrationSettings<TKey>(int groupOrdinal)
        {
            var regSettings = new RegistrationSettings<TKey>
            {
                HashTableSize = 1L << LogFiles.HashSizeBits,
                LogSettings = this.logFiles.GroupLogSettings[groupOrdinal],
                CheckpointSettings = new CheckpointSettings(),  // TODO checkpoints
                IPU1CacheSize = 0,          // TODO IPUCache
                IPU2CacheSize = 0
            };
            
            // Override some things.
            var regLogSettings = regSettings.LogSettings;
            if (!SubsetHashIndexSampleApp.useMultiGroups)
            {
                regLogSettings.PageSizeBits += 1;
                regLogSettings.SegmentSizeBits += 1;
                regLogSettings.MemorySizeBits += 2;
            }
            if (!(regLogSettings.ReadCacheSettings is null))
            {
                regLogSettings.ReadCacheSettings.PageSizeBits = regLogSettings.PageSizeBits;
                regLogSettings.ReadCacheSettings.MemorySizeBits = regLogSettings.MemorySizeBits;
            }
            return regSettings;
        }

    internal void Close()
        {
            if (!(this.FasterKV is null))
            {
                this.FasterKV.Dispose();
                this.FasterKV = null;
            }
            if (!(this.logFiles is null))
            {
                this.logFiles.Close();
                this.logFiles = null;
            }
        }
    }
}
