// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.IO;

namespace SubsetIndexSampleCommon
{
    class LogFiles
    {
        private IDevice log;
        private IDevice objLog;
        private IDevice[] GroupDevices;

        internal LogSettings LogSettings { get; }

        internal LogSettings[] GroupLogSettings { get; }

        internal string LogDir;

        // Hash and log sizes
        internal const int HashSizeBits = 20;
        private const int MemorySizeBits = 29;
        private const int SegmentSizeBits = 25;
        private const int PageSizeBits = 20;

        internal LogFiles(int numGroups, string appName)
        {
            this.LogDir = Path.Combine(Path.GetTempPath(), appName);

            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We set deleteOnClose to true, so logs will auto-delete on completion.
            this.log = Devices.CreateLogDevice(Path.Combine(this.LogDir, "hlog.log"), deleteOnClose: true);

            this.LogSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = objLog,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = false,
                ReadCacheSettings = null
            };

            this.GroupDevices = new IDevice[numGroups];
            this.GroupLogSettings = new LogSettings[numGroups];
            for (var ii = 0; ii < numGroups; ++ii)
            {
                this.GroupDevices[ii] = Devices.CreateLogDevice(Path.Combine(this.LogDir, $"shi_group_{ii}.hlog.log"), deleteOnClose: true);
                this.GroupLogSettings[ii] = new LogSettings { LogDevice = this.GroupDevices[ii], MemorySizeBits = MemorySizeBits, SegmentSizeBits = SegmentSizeBits, PageSizeBits = PageSizeBits };
                // Note: ReadCache and CopyReadsToTail are not supported in SubsetIndex FKVs
            }
        }

        internal void Close()
        {
            if (!(this.log is null))
            {
                this.log.Dispose();
                this.log = null;
            }
            if (!(this.objLog is null))
            {
                this.objLog.Dispose();
                this.objLog = null;
            }

            foreach (var device in this.GroupDevices)
                device.Dispose();
            this.GroupDevices = null;
        }
    }
}
