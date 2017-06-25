// Copyright (c) 2011 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// File created: 2011-01-17 15:17:59

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMFormatException;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** A class for heuristically finding BAM record positions inside an area of
 * a BAM file.
 */
public class BAMSplitGuesser extends BaseSplitGuesser {
	private       SeekableStream             inFile;
	public BlockCompressedInputStream bgzf;
	private BAMPosGuesser posGuesser;
	private final int                        referenceSequenceCount;

	byte[] arr = new byte[MAX_BYTES_READ];

	// We want to go through this many BGZF blocks fully, checking that they
	// contain valid BAM records, when guessing a BAM record position.
	public final static byte BLOCKS_NEEDED_FOR_GUESS = 3;

	// Since the max size of a BGZF block is 0xffff (64K), and we might be just
	// one byte off from the start of the previous one, we need 0xfffe bytes for
	// the start, and then 0xffff times the number of blocks we want to go
	// through.
	private final static int MAX_BYTES_READ =
		BLOCKS_NEEDED_FOR_GUESS * 0xffff + 0xfffe;

	/** The stream must point to a valid BAM file, because the header is read
	 * from it.
	 */
	public BAMSplitGuesser(
			SeekableStream ss, Configuration conf)
		throws IOException
	{
		this(ss, ss, conf);

		// Secondary check that the header points to a BAM file: Picard can get
		// things wrong due to its autodetection.
		ss.seek(0);
		if (ss.read(buf.array(), 0, 4) != 4 || buf.getInt(0) != BGZF_MAGIC)
			throw new SAMFormatException("Does not seem like a BAM file");
	}

	public BAMSplitGuesser(
			SeekableStream ss, InputStream headerStream, Configuration conf)
		throws IOException
	{
		this(
			ss,
			SAMHeaderReader
				.readSAMHeaderFrom(headerStream, conf)
				.getSequenceDictionary().size()
		);
	}

	public BAMSplitGuesser(SeekableStream ss,
						   int referenceSequenceCount)
		throws IOException
	{
		inFile = ss;
		this.referenceSequenceCount = referenceSequenceCount;
	}

	public void fillBuffer(long beg, long end)
		throws IOException {

		this.inFile.seek(beg);

		int totalRead = 0;
		for (int left = Math.min((int)(end - beg), arr.length); left > 0;) {
			final int r = inFile.read(arr, totalRead, left);
			if (r < 0)
				break;
			totalRead += r;
			left -= r;
		}

		arr = Arrays.copyOf(arr, totalRead);

		in = new ByteArraySeekableStream(arr);

		bgzf = new BlockCompressedInputStream(in);
		bgzf.setCheckCrcs(true);

		posGuesser = new BAMPosGuesser(bgzf, referenceSequenceCount);
	}

	/** Finds a virtual BAM record position in the physical position range
	 * [beg,end). Returns end if no BAM record was found.
	 */
	public long guessNextBAMRecordStart(long beg, long end)
		throws IOException
	{
		// Use a reader to skip through the headers at the beginning of a BAM file, since
		// the headers may exceed MAX_BYTES_READ in length. Don't close the reader
		// otherwise it will close the underlying stream, which we continue to read from
		// on subsequent calls to this method.
		if (beg == 0) {
			this.inFile.seek(beg);
			SamReader open =
				SamReaderFactory
					.makeDefault()
					.setUseAsyncIo(false)
					.open(SamInputResource.of(inFile));

			SAMFileSpan span =
				open
					.indexing()
					.getFilePointerSpanningReads();

			if (span instanceof BAMFileSpan) {
				return ((BAMFileSpan) span).getFirstOffset();
			}
		}

		// Buffer what we need to go through.

		this.inFile.seek(beg);
		fillBuffer(beg, end);

		final int firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

		// cp: Compressed Position, indexes the entire BGZF input.
		for (int cp = 0;; ++cp) {
			final PosSize psz = guessNextBGZFPos(cp, firstBGZFEnd);
			if (psz == null)
				return end;

			final int  cp0     = cp = psz.pos;
			final long cp0Virt = (long)cp0 << 16;
			try {
				bgzf.seek(cp0Virt);

			// This has to catch Throwable, because it's possible to get an
			// OutOfMemoryError due to an overly large size.
			} catch (Throwable e) {
				// Guessed BGZF position incorrectly: try the next guess.
				continue;
			}

			long nextBAMPos = findNextBAMPos(cp0, 0);
			if (nextBAMPos < 0) {
				return end;
			}
			return (beg << 16) + nextBAMPos;
		}
	}

	public long findNextBAMPos(int cp0, int offset)
		throws IOException {

		try {
			long vPos = ((long) cp0 << 16) | offset;
			int numTries = 65536;
			boolean firstPass = true;

			// up: Uncompressed Position, indexes the data inside the BGZF block.
			for (int i = 0; i < numTries; i++) {
				if (firstPass) {
					firstPass = false;
					bgzf.seek(vPos);
				} else {
					bgzf.seek(vPos);
					// Increment vPos, possibly over a block boundary
					IOUtils.skipFully(bgzf, 1);
					vPos = bgzf.getFilePointer();
				}

				if (!posGuesser.checkRecordStart(vPos)) {
					continue;
				}

				if (posGuesser.checkSucceedingRecords(vPos))
					return vPos;
			}
		} catch (EOFException ignored) {}
		return -1;
	}

	public static void main(String[] args) throws IOException {
		final GenericOptionsParser parser;
		try {
			parser = new GenericOptionsParser(args);

		// This should be IOException but Hadoop 0.20.2 doesn't throw it...
		} catch (Exception e) {
			System.err.printf("Error in Hadoop arguments: %s\n", e.getMessage());
			System.exit(1);

			// Hooray for javac
			return;
		}

		args = parser.getRemainingArgs();
		final Configuration conf = parser.getConfiguration();

		long beg = 0;

		if (args.length < 2 || args.length > 3) {
			System.err.println(
				"Usage: BAMSplitGuesser path-or-uri header-path-or-uri [beg]");
			System.exit(2);
		}

		try {
			if (args.length > 2) beg = Long.decode(args[2]);
		} catch (NumberFormatException e) {
			System.err.println("Invalid beg offset.");
			if (e.getMessage() != null)
				System.err.println(e.getMessage());
			System.exit(2);
		}

		SeekableStream ss = WrapSeekable.openPath(conf, new Path(args[0]));
		SeekableStream hs = WrapSeekable.openPath(conf, new Path(args[1]));

		final long end = beg + MAX_BYTES_READ;

		System.out.printf(
			"Will look for a BGZF block within: [%1$#x,%2$#x) = [%1$d,%2$d)\n"+
			"Will then verify BAM data within:  [%1$#x,%3$#x) = [%1$d,%3$d)\n",
			beg, beg + 0xffff, end);

		final long g =
			new BAMSplitGuesser(ss, hs, conf).guessNextBAMRecordStart(beg, end);

		ss.close();

		if (g == end) {
			System.out.println(
				"Didn't find any acceptable BAM record in any BGZF block.");
			System.exit(1);
		}

		System.out.printf(
			"Accepted BGZF block at offset %1$#x (%1$d).\n"+
			"Accepted BAM record at offset %2$#x (%2$d) therein.\n",
			g >> 16, g & 0xffff);
	}
}
