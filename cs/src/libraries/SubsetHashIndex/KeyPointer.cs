// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Runtime.CompilerServices;
using System.Text;

namespace FASTER.libraries.SubsetHashIndex
{
    internal struct KeyPointer<TPKey>
    {
        #region Fields
        /// <summary>
        /// The previous address in the hash chain. May be for different PredicateOrdinals than this one due to hash collisions.
        /// </summary>
        internal long PreviousAddress;

        /// <summary>
        /// The offset to the start of the record
        /// </summary>
        private ushort offsetToStartOfKeys; // TODOperf: Make this a byte; key length is constant (pass KeySize to OffsetToStartOfKeys()). The extra byte could be "reserved" for now

        /// <summary>
        /// The ordinal of the current <see cref="Predicate{TPKey, TRecordId}"/>.
        /// </summary>
        private byte predOrdinal;            // Note: 'byte' is consistent with Constants.kInvalidPredicateOrdinal

        /// <summary>
        /// Flags regarding the Predicate.
        /// </summary>
        private byte flags;

        /// <summary>
        /// The Key returned by the <see cref="Predicate{TPKey, TRecordId}"/> execution.
        /// </summary>
        internal TPKey Key;               // TODOperf: for Key size > 4, reinterpret this an offset to the actual value (after the KeyPointer list)
        #endregion Fields

        internal void Initialize(int predOrdinal, ref TPKey key, int keyPointerSize)
        {
            this.PreviousAddress = core.Constants.kInvalidAddress;
            this.PredicateOrdinal = predOrdinal;
            this.offsetToStartOfKeys = (ushort)(predOrdinal * keyPointerSize);   // Note: Assumes null keys are present in the key list
            this.flags = 0;
            this.Key = key;
        }

        #region Accessors
        // For Insert, this identifies a null Predicate result (the record does not match the Predicate and is not included
        // in any TPKey chain for it). Also used in ChangeTracker to determine whether to set kUnlinkOldBit.
        private const byte kIsNullBit = 0x01;

        // For Update and insert, the TPKey has changed; add this record to the new TPKey chain.
        private const byte kIsDeletedBit = 0x02;

        // If Key size is > 4, then reinterpret the Key as an offset to the actual key. (TODOperf not implemented)
        private const byte kIsOutOfLineKeyBit = 0x04;

        // For Update, the TPKey has changed; remove this record from the previous TPKey chain.
        private const byte kUnlinkOldBit = 0x08;

        // For Update and insert, the TPKey has changed; add this record to the new TPKey chain.
        private const byte kLinkNewBit = 0x10;

        internal bool IsNull
        {
            get => (this.flags & kIsNullBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kIsNullBit) : (byte)(this.flags & ~kIsNullBit);
        }

        internal bool IsDeleted
        {
            get => (this.flags & kIsDeletedBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kIsDeletedBit) : (byte)(this.flags & ~kIsDeletedBit);
        }

        internal bool IsUnlinkOld
        {
            get => (this.flags & kUnlinkOldBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kUnlinkOldBit) : (byte)(this.flags & ~kUnlinkOldBit);
        }

        internal bool IsLinkNew
        {
            get => (this.flags & kLinkNewBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kLinkNewBit) : (byte)(this.flags & ~kLinkNewBit);
        }

        internal bool IsOutOfLineKey
        {
            get => (this.flags & kIsOutOfLineKeyBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kIsOutOfLineKeyBit) : (byte)(this.flags & ~kIsOutOfLineKeyBit);
        }

        internal bool HasChanges => (this.flags & (kUnlinkOldBit | kLinkNewBit)) != 0;

        internal int PredicateOrdinal
        {
            get => this.predOrdinal;
            set => this.predOrdinal = (byte)value;
        }

        internal int OffsetToStartOfKeys
        {
            get => this.offsetToStartOfKeys;
            set => this.offsetToStartOfKeys = (ushort)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearUpdateFlags() => this.flags = (byte)(this.flags & ~(kUnlinkOldBit | kLinkNewBit));
        #endregion Accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe static ref KeyPointer<TPKey> CastFromKeyRef(ref TPKey keyRef)
            => ref Unsafe.AsRef<KeyPointer<TPKey>>((byte*)Unsafe.AsPointer(ref keyRef));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref KeyPointer<TPKey> CastFromPhysicalAddress(long physicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPKey>>((byte*)physicalAddress);

        public override string ToString()
        {
            var separator = "";
            var flagStrBuilder = new StringBuilder();
            void appendFlag(bool pred, string name)
            {
                if (pred)
                {
                    flagStrBuilder.Append(separator).Append(name);
                    separator = " | ";
                }
            }
            appendFlag(this.IsNull, nameof(this.IsNull));
            appendFlag(this.IsOutOfLineKey, nameof(this.IsOutOfLineKey));
            appendFlag(this.IsUnlinkOld, nameof(this.IsUnlinkOld));
            appendFlag(this.IsLinkNew, nameof(this.IsLinkNew));
            var flagStr = flagStrBuilder.Length > 0 ? flagStrBuilder.ToString() : "<none>";
            return $"Key: {this.Key}, predOrd {this.predOrdinal}, prevAddr {this.PreviousAddress}, ofsStartKeys {this.offsetToStartOfKeys}, flags {flagStr}";
        }
    }
}
