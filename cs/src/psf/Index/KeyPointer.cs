// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Runtime.CompilerServices;
using System.Text;

namespace PSF.Index
{
    internal struct KeyPointer<TPSFKey>
    {
        #region Fields
        /// <summary>
        /// The previous address in the hash chain. May be for a different PsfOrdinal than this one due to hash collisions.
        /// </summary>
        internal long PreviousAddress;

        /// <summary>
        /// The offset to the start of the record
        /// </summary>
        private ushort offsetToStartOfKeys; // TODOperf: Make this a byte; key length is constant (pass KeySize to OffsetToStartOfKeys()). The extra byte could be "reserved" for now

        /// <summary>
        /// The ordinal of the current <see cref="PSF{TPSFKey, TRecordId}"/>.
        /// </summary>
        private byte psfOrdinal;            // Note: 'byte' is consistent with Constants.kInvalidPsfOrdinal

        /// <summary>
        /// Flags regarding the PSF.
        /// </summary>
        private byte flags;

        /// <summary>
        /// The Key returned by the <see cref="PSF{TPSFKey, TRecordId}"/> execution.
        /// </summary>
        internal TPSFKey Key;               // TODOperf: for Key size > 4, reinterpret this an offset to the actual value (after the KeyPointer list)
        #endregion Fields

        internal void Initialize(int psfOrdinal, ref TPSFKey key, int keyPointerSize)
        {
            this.PreviousAddress = Constants.kInvalidAddress;
            this.PsfOrdinal = psfOrdinal;
            this.offsetToStartOfKeys = (ushort)(psfOrdinal * keyPointerSize);   // Note: Assumes null keys are present in the key list
            this.flags = 0;
            this.Key = key;
        }

        #region Accessors
        // For Insert, this identifies a null PSF result (the record does not match the PSF and is not included
        // in any TPSFKey chain for it). Also used in PSFChangeTracker to determine whether to set kUnlinkOldBit.
        private const byte kIsNullBit = 0x01;

        // For Update and insert, the TPSFKey has changed; add this record to the new TPSFKey chain.
        private const byte kIsDeletedBit = 0x02;

        // If Key size is > 4, then reinterpret the Key as an offset to the actual key. (TODOperf not implemented)
        private const byte kIsOutOfLineKeyBit = 0x04;

        // For Update, the TPSFKey has changed; remove this record from the previous TPSFKey chain.
        private const byte kUnlinkOldBit = 0x08;

        // For Update and insert, the TPSFKey has changed; add this record to the new TPSFKey chain.
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

        internal int PsfOrdinal
        {
            get => this.psfOrdinal;
            set => this.psfOrdinal = (byte)value;
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
        internal unsafe static ref KeyPointer<TPSFKey> CastFromKeyRef(ref TPSFKey keyRef)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)Unsafe.AsPointer(ref keyRef));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe ref KeyPointer<TPSFKey> CastFromPhysicalAddress(long physicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)physicalAddress);

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
            return $"Key: {this.Key}, psfOrd {this.psfOrdinal}, prevAddr {this.PreviousAddress}, ofsStartKeys {this.offsetToStartOfKeys}, flags {flagStr}";
        }
    }
}
