using FASTER.core;
using System.IO;

namespace FasterPSFSample
{
    class LogFiles
    {
        private IDevice log;
        private IDevice objLog;
        private IDevice[] PSFDevices;

        internal LogSettings LogSettings { get; }

        internal LogSettings[] PSFLogSettings { get; }

        internal string LogDir;

        // Hash and log sized
        internal const int HashSizeBits = 20;
        private const int MemorySizeBits = 15;
        private const int PageSizeBits = 10;

        internal LogFiles(bool useObjectValue, bool useReadCache, int numPSFGroups)
        {
            this.LogDir = Path.Combine(Path.GetTempPath(), "FasterPSFSample");

            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We set deleteOnClose to true, so logs will auto-delete on completion.
            this.log = Devices.CreateLogDevice(Path.Combine(this.LogDir, "hlog.log"), deleteOnClose: true);
            if (useObjectValue)
                this.objLog = Devices.CreateLogDevice(Path.Combine(this.LogDir, "hlog.obj.log"), deleteOnClose: true);

            this.LogSettings = new LogSettings { LogDevice = log, MemorySizeBits = MemorySizeBits, PageSizeBits = PageSizeBits, ObjectLogDevice = objLog };
            if (useReadCache)
                this.LogSettings.ReadCacheSettings = new ReadCacheSettings { MemorySizeBits = MemorySizeBits, PageSizeBits = PageSizeBits };

            this.PSFDevices = new IDevice[numPSFGroups];
            this.PSFLogSettings = new LogSettings[numPSFGroups];
            for (var ii = 0; ii < numPSFGroups; ++ii)
            {
                this.PSFDevices[ii] = Devices.CreateLogDevice(Path.Combine(this.LogDir, $"psfgroup_{ii}.hlog.log"), deleteOnClose: true);
                this.PSFLogSettings[ii] = new LogSettings { LogDevice = this.PSFDevices[ii] };
                if (useReadCache)
                    this.PSFLogSettings[ii].ReadCacheSettings = new ReadCacheSettings { MemorySizeBits = MemorySizeBits, PageSizeBits = MemorySizeBits };
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

            foreach (var psfDevice in this.PSFDevices)
                psfDevice.Dispose();
            this.PSFDevices = null;
        }
    }
}
