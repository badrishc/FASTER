// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.PSF;
using PSF.Index;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FasterPSFSample
{
    class FPSF<TValue, TInput, TOutput, TFunctions, TSerializer>
        where TValue : IOrders
        where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal PSFFasterKV<Key, TValue> PSFFasterKV { get; set; }

        private LogFiles logFiles;

        // MultiGroup PSFs -- different key types, one per group.
        internal IPSF SizePsf, ColorPsf, CountBinPsf;
        internal IPSF CombinedSizePsf, CombinedColorPsf, CombinedCountBinPsf;

        internal FPSF(bool useObjectValues, bool useMultiGroup, bool useReadCache)
        {
            this.logFiles = new LogFiles(useObjectValues, useReadCache, useMultiGroup ? 3 : 1);

            this.PSFFasterKV = new PSFFasterKV<Key, TValue>(
                                1L << 20, this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                useObjectValues ? new SerializerSettings<Key, TValue> { valueSerializer = () => new TSerializer() } : null);

            if (useMultiGroup)
            {
                var groupOrdinal = 0;
                this.SizePsf = PSFFasterKV.RegisterPSF(CreatePSFRegistrationSettings<SizeKey>(groupOrdinal++), nameof(this.SizePsf),
                                                    (k, v) => new SizeKey((Constants.Size)v.SizeInt));
                this.ColorPsf = PSFFasterKV.RegisterPSF(CreatePSFRegistrationSettings<ColorKey>(groupOrdinal++), nameof(this.ColorPsf),
                                                    (k, v) => new ColorKey(Constants.ColorDict[v.ColorArgb]));
                this.CountBinPsf = PSFFasterKV.RegisterPSF(CreatePSFRegistrationSettings<CountBinKey>(groupOrdinal++), nameof(this.CountBinPsf),
                                                    (k, v) => CountBinKey.GetAndVerifyBin(v.Count, out int bin) ? new CountBinKey(bin) : (CountBinKey?)null);
            }
            else
            {
                var psfs = PSFFasterKV.RegisterPSF(CreatePSFRegistrationSettings<CombinedKey>(0),
                                                new (string, Func<Key, TValue, CombinedKey?>)[]
                                                {
                                                    (nameof(this.SizePsf), (k, v) => new CombinedKey((Constants.Size)v.SizeInt)),
                                                    (nameof(this.ColorPsf), (k, v) => new CombinedKey(Constants.ColorDict[v.ColorArgb])),
                                                    (nameof(this.CountBinPsf), (k, v) => CountBinKey.GetAndVerifyBin(v.Count, out int bin)
                                                                                                    ? new CombinedKey(bin) : (CombinedKey?)null)
                                                });
                this.CombinedSizePsf = psfs[0];
                this.CombinedColorPsf = psfs[1];
                this.CombinedCountBinPsf = psfs[2];
            }
        }

        PSFRegistrationSettings<TKey> CreatePSFRegistrationSettings<TKey>(int groupOrdinal)
        {
            var regSettings = new PSFRegistrationSettings<TKey>
            {
                HashTableSize = 1L << 20,
                LogSettings = this.logFiles.PSFLogSettings[groupOrdinal],
                CheckpointSettings = new CheckpointSettings(),  // TODO checkpoints
                IPU1CacheSize = 0,          // TODO IPUCache
                IPU2CacheSize = 0
            };
            
            // Override some things.
            var regLogSettings = regSettings.LogSettings;
            regLogSettings.PageSizeBits = 20;
            regLogSettings.SegmentSizeBits = 25;
            regLogSettings.MemorySizeBits = 29;
            regLogSettings.CopyReadsToTail = false;    // TODO--test this in both primary and secondary FKV
            if (!(regLogSettings.ReadCacheSettings is null))
            {
                regLogSettings.ReadCacheSettings.PageSizeBits = regLogSettings.PageSizeBits;
                regLogSettings.ReadCacheSettings.MemorySizeBits = regLogSettings.MemorySizeBits;
            }
            return regSettings;
        }

    internal void Close()
        {
            if (!(this.PSFFasterKV is null))
            {
                this.PSFFasterKV.Dispose();
                this.PSFFasterKV = null;
            }
            if (!(this.logFiles is null))
            {
                this.logFiles.Close();
                this.logFiles = null;
            }
        }
    }
}
