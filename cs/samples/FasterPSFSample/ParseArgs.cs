// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FasterPSFSample
{
    public partial class FasterPSFSampleApp
    {
        private static bool useObjectValue;
        internal static bool useMultiGroups;
        private static bool useAsync;
        private static bool flushAndEvict;
        private static int keyCount = 1000;
        private static bool verbose;

        const string ObjValuesArg = "--objValues";
        const string MultiGroupArg = "--multiGroup";
        const string AsyncArg = "--async";
        const string FlushArg = "--flush";
        const string KeysArg = "--keys";

        const string VerboseArg = "-v";
        const string HelpArg = "--help";

        static bool ParseArgs(string[] argv)
        {
            static bool Usage(string message = null)
            {
                Console.WriteLine();
                Console.WriteLine($"Usage: Run one or more Predicate Subset Functions (PSFs), specifying whether to use object or blittable (primitive) values.");
                Console.WriteLine();
                Console.WriteLine($"    {ObjValuesArg}: Use objects instead of blittable Value; default is {useObjectValue}");
                Console.WriteLine($"    {MultiGroupArg}: Put each PSF in a separate group; default is {useMultiGroups}");
                Console.WriteLine($"    {AsyncArg}: Use Async operations on FasterKV; default is {useAsync}");
                Console.WriteLine($"    {FlushArg}: FlushAndEvict before each operation on FasterKV; default is {useAsync}");
                Console.WriteLine($"    {KeysArg}: Number of keys for initial insert; default is {keyCount}");
                Console.WriteLine($"    {VerboseArg}: Verbose output (show each result set evaluation); default is {useAsync}");
                Console.WriteLine($"    {HelpArg}, /?, or -?: Show this message");
                Console.WriteLine();
                if (!string.IsNullOrEmpty(message))
                {
                    Console.WriteLine("====== Invalid Argument(s) ======");
                    Console.WriteLine(message);
                    Console.WriteLine();
                }
                Console.WriteLine();
                return false;
            }

            for (var ii = 0; ii < argv.Length; ++ii)
            {
                var arg = argv[ii];
                if (string.Compare(arg, ObjValuesArg, ignoreCase: true) == 0)
                {
                    useObjectValue = true;
                    continue;
                }
                if (string.Compare(arg, MultiGroupArg, ignoreCase: true) == 0)
                {
                    useMultiGroups = true;
                    continue;
                }
                if (string.Compare(arg, AsyncArg, ignoreCase: true) == 0)
                {
                    useAsync = true;
                    continue;
                }
                if (string.Compare(arg, FlushArg, ignoreCase: true) == 0)
                {
                    flushAndEvict = true;
                    continue;
                }
                if (string.Compare(arg, KeysArg, ignoreCase: true) == 0)
                {
                    if (ii > argv.Length - 1)
                    {
                        Console.WriteLine($"{arg}: requires a count argument");
                        return false;
                    }
                    var arg1 = argv[ii + 1];
                    if (!int.TryParse(arg1, out keyCount))
                    {
                        Console.WriteLine($"{arg}: requires a count argument; {arg1} is invalid");
                        return false;
                    }
                    ++ii;
                    continue;
                }
                if (string.Compare(arg, VerboseArg, ignoreCase: true) == 0)
                {
                    verbose = true;
                    continue;
                }
                if (string.Compare(arg, HelpArg, ignoreCase: true) == 0 || arg == "/?" || arg == "-?")
                    return Usage();
                return Usage($"Unknown argument: {arg}");
            }
            return true;
        }
    }
}
