package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMRecord;
import htsjdk.samtools.FileTruncatedException;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordFactory;
import htsjdk.samtools.SAMRecordHelper;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.samtools.util.RuntimeIOException;
import org.apache.hadoop.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.seqdoop.hadoop_bam.BAMSplitGuesser.BLOCKS_NEEDED_FOR_GUESS;

public class BAMPosGuesser {

    // Change this value to simulate hadoop-bam throwing OOMs when attempting to allocate byte-arrays to store
    // excessively large "records" (which are actually just random data from the inside of actual records).
    //
    // By default, this is effectively disabled, as it introduces too much randomness into simulating hadoop-bam's
    // behavior.
    public int maximumRecordLength = Integer.MAX_VALUE;
    private final SeekableStream ss;
    private final SAMRecordFactory samRecordFactory = new LazyBAMRecordFactory();
    private final BinaryCodec binaryCodec = new BinaryCodec();
    private final BlockCompressedInputStream uncompressedBytes;
    private final int                        referenceSequenceCount;
    private final SAMFileHeader header;

    private final ByteBuffer buf =
        ByteBuffer
            .allocate(36)
            .order(ByteOrder.LITTLE_ENDIAN);

    public BAMPosGuesser(SeekableStream ss,
                         int referenceSequenceCount,
                         SAMFileHeader header) {
        this(ss, new BlockCompressedInputStream(ss), referenceSequenceCount, header);
    }

    public BAMPosGuesser(SeekableStream ss,
                         BlockCompressedInputStream uncompressedBytes,
                         int referenceSequenceCount,
                         SAMFileHeader header) {
        this.ss = ss;
        this.uncompressedBytes = uncompressedBytes;
        this.referenceSequenceCount = referenceSequenceCount;
        binaryCodec.setInputStream(uncompressedBytes);
        this.header = header;
    }

    public boolean checkRecordStart(long vPos) {
        try {

            uncompressedBytes.seek(vPos);
            IOUtils.readFully(uncompressedBytes, buf.array(), 0, 36);

            final int remainingBytes = buf.getInt(0);

            // If the first two checks fail we have what looks like a valid
            // reference sequence ID. Assume we're at offset [4] or [24], i.e.
            // the ID of either this read or its mate, respectively. So check
            // the next integer ([8] or [28]) to make sure it's a 0-based
            // leftmost coordinate.
            final int id  = buf.getInt(4);
            final int pos = buf.getInt(8);
            if (id < -1 || id > referenceSequenceCount || pos < -1) {
                return false;
            }

            // Okay, we could be at [4] or [24]. Assuming we're at [4], check
            // that [24] is valid. Assume [4] because we should hit it first:
            // the only time we expect to hit [24] is at the beginning of the
            // split, as part of the first read we should skip.

            final int nid  = buf.getInt(24);
            final int npos = buf.getInt(28);
            if (nid < -1 || nid > referenceSequenceCount || npos < -1) {
                return false;
            }

            // So far so good: [4] and [24] seem okay. Now do something a bit
            // more involved: make sure that [36 + [12]&0xff - 1] == 0: that
            // is, the name of the read should be null terminated.

            final int nameLength = buf.getInt(12) & 0xff;
            if (nameLength < 1) {
                // Names are null-terminated so length must be at least one
                return false;
            }

            int cigarOpsLength = (buf.getInt(16) & 0xffff) * 4;
            int seqLength = buf.getInt(20) + (buf.getInt(20)+1)/2;

            // Pos 36 + nameLength - 1
            IOUtils.skipFully(uncompressedBytes, nameLength - 1);
            IOUtils.readFully(uncompressedBytes, buf.array(), 0, 1);

            if (buf.get(0) != 0) {
                return false;
            }

            // All of [4], [24], and [36 + [12]&0xff] look good. If [0] is also
            // sensible, that's good enough for us. "Sensible" to us means the
            // following:
            //
            // [0] >= 4*([16]&0xffff) + [20] + ([20]+1)/2 + 4*8 + ([12]&0xff)

            // Note that [0] is "length of the _remainder_ of the alignment
            // record", which is why this uses 4*8 instead of 4*9.
            int zeroMin = 4*8 + nameLength + cigarOpsLength + seqLength;

            return remainingBytes >= zeroMin;
        } catch (IOException ignored) {}
        return false;
    }

    public boolean checkSucceedingRecords(long vPos)
        throws IOException {

        // Verify that we can actually decode BLOCKS_NEEDED_FOR_GUESS worth
        // of records starting at vPos.
        uncompressedBytes.seek(vPos);
        boolean decodedAny = false;
        try {
            byte b = 0;
            long prevCP = (vPos >>> 16);
            while (b < BLOCKS_NEEDED_FOR_GUESS)
            {
                SAMRecord record = readLazyRecord();
                if (record == null) {
                    break;
                }

                record.setHeaderStrict(header);
                SAMRecordHelper.eagerDecode(record); // force decoding of fields
                decodedAny = true;

                final long cp2 = (uncompressedBytes.getFilePointer() >>> 16);
                if (cp2 != prevCP) {
                    // The compressed position changed so we must be in a new
                    // block.
                    assert cp2 > prevCP;
                    prevCP = cp2;
                    ++b;
                }
            }

            // Running out of records to verify is fine as long as we
            // verified at least something. It should only happen if we
            // couldn't fill the array.
            if (b < BLOCKS_NEEDED_FOR_GUESS) {
                if (!decodedAny)
                    return false;
            }
        }
        catch (
            SAMFormatException |
                IllegalArgumentException |
                OutOfMemoryError |
                IndexOutOfBoundsException |
                RuntimeIOException e
            ) {
            return false;
        }
        catch (
            FileTruncatedException |
                RuntimeEOFException e
            ) {
            if (!decodedAny && this.ss.eof())
                return false;
        }

        return true;
    }

    /**
     * The beginning of a BAMRecord is a fixed-size block of 8 int32s
     */
    static final int FIXED_BLOCK_SIZE = 8 * 4;

    /**
     * Simulate original/upstream validation of [[SAMRecord]]s downstream of a candidate "split guess":
     *
     *   - validate that its cigar has no invalid operation codes
     *   - simulate reading ahead `recordLength` bytes, possibly triggering a [[RuntimeIOException]] if this would reach
     *     past the end of the buffered [[MAX_BYTES_READ]] bytes of (compressed) data
     *
     * Unfortunately, the original algorithm relied on the available JVM heap size in a way that is hard to reproduce
     * efficiently: bad split guesses' `recordLengths` were arbitrary integers, it would allocate a byte array of that
     * size, and an [[OutOfMemoryError]] would be interpreted as ruling out that bad guess.
     *
     * Such allocations, and pathological other large ones that didn't quite cause [[OutOfMemoryError]]s, caused intense
     * memory pressure on the JVMs doing the checking, dramatically slowing down evaluation of the guesser's accuracy.
     *
     * I've worked around this issue here by utilizing a configurable cap on records' `recordLength` fields, efficiently
     * simulating a maximum allocation size, but with the downside that the original behavior is not exactly reproduced.
     */
    public SAMRecord readLazyRecord() throws IOException {
        int recordLength;
        try {
            recordLength = this.binaryCodec.readInt();
        }
        catch (RuntimeEOFException e) {
            return null;
        }

        if (recordLength < FIXED_BLOCK_SIZE) {
            throw new SAMFormatException("Invalid record length: " + recordLength);
        }

        // Simulate overly large allocations that would have previously caused [[OutOfMemoryError]]s.
        // By default this is effectively disabled, since `maximumRecordLength`'s default value is Integer.MAX_VALUE.
        // This means we are simulating hadoop-bam's behavior in the presence of heaps large enough that it loses the
        // ability to catch false positions via OOMs.
        if (recordLength > maximumRecordLength) {
            throw new IndexOutOfBoundsException();
        }

        final int referenceID = this.binaryCodec.readInt();
        final int coordinate = this.binaryCodec.readInt() + 1;
        final short readNameLength = this.binaryCodec.readUByte();
        final short mappingQuality = this.binaryCodec.readUByte();
        final int bin = this.binaryCodec.readUShort();
        final int cigarLen = this.binaryCodec.readUShort();
        final int flags = this.binaryCodec.readUShort();
        final int readLen = this.binaryCodec.readInt();
        final int mateReferenceID = this.binaryCodec.readInt();
        final int mateCoordinate = this.binaryCodec.readInt() + 1;
        final int insertSize = this.binaryCodec.readInt();
        final int remainingToRead = recordLength - FIXED_BLOCK_SIZE;

        // Only read enough data to validate the cigar operators
        final int numToRead = readNameLength + 4 * cigarLen;
        final byte[] restOfRecord = new byte[numToRead];

        this.binaryCodec.readBytes(restOfRecord);

        // Before any cigar-op validity-checks occur, simulate reading the full [[recordLength]] bytes of this "record",
        // triggering a [[RuntimeIOException]] if that runs past the end of the buffered data.
        try {
            IOUtils.skipFully(uncompressedBytes, remainingToRead - numToRead);
        } catch (EOFException e) {
            throw new RuntimeEOFException("Unexpected EOF while reading record", e);
        }

        final BAMRecord ret = this.samRecordFactory.createBAMRecord(
            null, referenceID, coordinate, readNameLength, mappingQuality,
            bin, cigarLen, flags, readLen, mateReferenceID, mateCoordinate, insertSize, restOfRecord);

        // If the above checks have not failed, validate the cigar.
        ret.getCigar();

        return ret;
    }
}
