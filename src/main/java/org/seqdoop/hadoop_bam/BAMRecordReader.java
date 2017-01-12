// Copyright (c) 2010 Aalto University
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

// File created: 2010-08-09 14:34:08

package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Interval;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BlockCompressedInputStream;

import org.seqdoop.hadoop_bam.util.MurmurHash3;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

/** The key is the bitwise OR of the reference sequence ID in the upper 32 bits
 * and the 0-based leftmost coordinate in the lower.
 */
public class BAMRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private final LongWritable key = new LongWritable();
	private final SAMRecordWritable record = new SAMRecordWritable();

	private ValidationStringency stringency;

	private Iterator<SAMRecord> iterator;
	private BlockCompressedInputStream bci;
	private long fileStart, virtualStart, virtualEnd;
	private boolean isInitialized = false;
	private boolean keepReadPairsTogether;
	private boolean readPair;
	private boolean lastOfPair;
	private List<Interval> intervals;

	/** Note: this is the only getKey function that handles unmapped reads
	 * specially!
	 */
	public static long getKey(final SAMRecord rec) {
		final int refIdx = rec.getReferenceIndex();
		final int start  = rec.getAlignmentStart();

		if (!(rec.getReadUnmappedFlag() || refIdx < 0 || start < 0))
			return getKey(refIdx, start);

		// Put unmapped reads at the end, but don't give them all the exact same
		// key so that they can be distributed to different reducers.
		//
		// A random number would probably be best, but to ensure that the same
		// record always gets the same key we use a fast hash instead.
		//
		// We avoid using hashCode(), because it's not guaranteed to have the
		// same value across different processes.

		int hash = 0;
		byte[] var;
		if ((var = rec.getVariableBinaryRepresentation()) != null) {
			// Undecoded BAM record: just hash its raw data.
			hash = (int)MurmurHash3.murmurhash3(var, hash);
		} else {
			// Decoded BAM record or any SAM record: hash a few representative
			// fields together.
			hash = (int)MurmurHash3.murmurhash3(rec.getReadName(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getReadBases(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getBaseQualities(), hash);
			hash = (int)MurmurHash3.murmurhash3(rec.getCigarString(), hash);
		}
		return getKey0(Integer.MAX_VALUE, hash);
	}

	/** @param alignmentStart 1-based leftmost coordinate. */
	public static long getKey(int refIdx, int alignmentStart) {
		return getKey0(refIdx, alignmentStart-1);
	}

	/** @param alignmentStart0 0-based leftmost coordinate. */
	public static long getKey0(int refIdx, int alignmentStart0) {
		return (long)refIdx << 32 | alignmentStart0;
	}

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		// This method should only be called once (see Hadoop API). However,
		// there seems to be disagreement between implementations that call
		// initialize() and Hadoop-BAM's own code that relies on
		// {@link BAMInputFormat} to call initialize() when the reader is
		// created. Therefore we add this check for the time being. 
		if(isInitialized)
			close();
		isInitialized = true;

		final Configuration conf = ctx.getConfiguration();

		final FileVirtualSplit split = (FileVirtualSplit)spl;
		final Path             file  = split.getPath();
		final FileSystem       fs    = file.getFileSystem(conf);

		this.stringency = SAMHeaderReader.getValidationStringency(conf);

		final FSDataInputStream in = fs.open(file);

		Path fileIndex = new Path(file.getParent(), file.getName().replaceFirst("\\.bam$",
				BAMIndex.BAMIndexSuffix));
		FSDataInputStream inIndex = fs.exists(fileIndex) ? fs.open(fileIndex) : null;
		SeekableStream inStream = inIndex == null ? null : new
				WrapSeekable<>(inIndex, fs.getFileStatus(fileIndex).getLen(), fileIndex);
		SamReader samReader = createSamReader(new WrapSeekable<>(
				in, fs.getFileStatus(file).getLen(), file), inStream,
				stringency);
		final SAMFileHeader header = samReader.getFileHeader();

		virtualStart = split.getStartVirtualOffset();

		fileStart  = virtualStart >>> 16;
		virtualEnd = split.getEndVirtualOffset();


		SamReader.PrimitiveSamReader primitiveSamReader = ((SamReader
				.PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
		BAMFileReader bamFileReader = (BAMFileReader) primitiveSamReader;
		bci = bamFileReader.getBlockCompressedInputStream(); // get underlying block compressed stream, so we can seek to start of split

		if(BAMInputFormat.DEBUG_BAM_SPLITTER) {
			final long recordStart = virtualStart & 0xffff;
                	System.err.println("XXX inizialized BAMRecordReader byte offset: " +
				fileStart + " record offset: " + recordStart);
		}

		keepReadPairsTogether = SortOrder.queryname.equals(header.getSortOrder()) &&
			conf.getBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, false);
		readPair = false;
		lastOfPair = false;
		intervals = BAMInputFormat.getIntervals(conf);
		if (intervals != null) {
			QueryInterval[] queryIntervals = prepareQueryIntervals(intervals, header.getSequenceDictionary());
			iterator = newIterator(split.getIntervalFilePointers(), queryIntervals, bamFileReader);
			bci.seek(virtualStart);
		} else {
			iterator = samReader.iterator();
		}
		bci.seek(virtualStart); // seek after creating iterator since htsjdk seeks to the beginning of stream when creating the iterator
	}

	private Iterator<SAMRecord> newIterator(long[] intervalFilePointers, QueryInterval[] queryIntervals, BAMFileReader bamFileReader) {
		return bamFileReader.createIndexIterator(queryIntervals, false, intervalFilePointers);
	}

	private SamReader createSamReader(SeekableStream in, SeekableStream inIndex,
			ValidationStringency stringency) {
		SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
				.setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
				.setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
				.setUseAsyncIo(false);
		if (stringency != null) {
			readerFactory.validationStringency(stringency);
		}
		SamInputResource resource = SamInputResource.of(in);
		if (inIndex != null) {
			resource.index(inIndex);
		}
		return readerFactory.open(resource);
	}

	/**
	 * Converts a List of SimpleIntervals into the format required by the SamReader query API
	 * @param rawIntervals SimpleIntervals to be converted
	 * @return A sorted, merged list of QueryIntervals suitable for passing to the SamReader query API
	 */
	// TODO: this is from GATK's SamReaderQueryingIterator
	public static QueryInterval[] prepareQueryIntervals( final List<Interval>
			rawIntervals, final SAMSequenceDictionary sequenceDictionary ) {
		if ( rawIntervals == null || rawIntervals.isEmpty() ) {
			return null;
		}

		// This might take a while with large interval lists, so log a status message
		System.out.println("Preparing intervals for traversal");

		// Convert each SimpleInterval to a QueryInterval
		final QueryInterval[] convertedIntervals =
				rawIntervals.stream()
						.map(rawInterval -> convertSimpleIntervalToQueryInterval(rawInterval, sequenceDictionary))
						.toArray(QueryInterval[]::new);

		// Intervals must be optimized (sorted and merged) in order to use the htsjdk query API
		return QueryInterval.optimizeIntervals(convertedIntervals);
	}
	/**
	 * Converts an interval in SimpleInterval format into an htsjdk QueryInterval.
	 *
	 * In doing so, a header lookup is performed to convert from contig name to index
	 *
	 * @param interval interval to convert
	 * @param sequenceDictionary sequence dictionary used to perform the conversion
	 * @return an equivalent interval in QueryInterval format
	 */
	// TODO: this is from GATK's SamReaderQueryingIterator
	public static QueryInterval convertSimpleIntervalToQueryInterval( final Interval interval, final SAMSequenceDictionary sequenceDictionary ) {
		if (interval == null) {
			throw new IllegalArgumentException("interval may not be null");
		}
		if (sequenceDictionary == null) {
			throw new IllegalArgumentException("sequence dictionary may not be null");
		}

		final int contigIndex = sequenceDictionary.getSequenceIndex(interval.getContig());
		if ( contigIndex == -1 ) {
			throw new IllegalArgumentException("Contig " + interval.getContig() + " not present in reads sequence " +
					"dictionary");
		}

		return new QueryInterval(contigIndex, interval.getStart(), interval.getEnd());
	}

	@Override public void close() throws IOException { bci.close(); }

	/** Unless the end has been reached, this only takes file position into
	 * account, not the position within the block.
	 */
	@Override public float getProgress() {
		final long virtPos = bci.getFilePointer();
		final long filePos = virtPos >>> 16;
		if (virtPos >= virtualEnd)
			return 1;
		else {
			final long fileEnd = virtualEnd >>> 16;
			// Add 1 to the denominator to make sure it doesn't reach 1 here when
			// filePos == fileEnd.
			return (float)(filePos - fileStart) / (fileEnd - fileStart + 1);
		}
	}
	@Override public LongWritable      getCurrentKey  () { return key; }
	@Override public SAMRecordWritable getCurrentValue() { return record; }

	@Override public boolean nextKeyValue() {
		long virtPos;
		while ((virtPos = bci.getFilePointer()) < virtualEnd || (keepReadPairsTogether && readPair && !lastOfPair)) {

			if (!iterator.hasNext())
				return false;
			final SAMRecord r = iterator.next();

			// Since we're reading from a BAMRecordCodec directly we have to set the
			// validation stringency ourselves.
			if (this.stringency != null)
				r.setValidationStringency(this.stringency);

			readPair = r.getReadPairedFlag();
			if (readPair) {
				boolean first = r.getFirstOfPairFlag(), second = r.getSecondOfPairFlag();
				// According to the SAM spec (section 1.4) it is possible for pairs to have
				// multiple segments (i.e. more than two), in which case both `first` and
				// `second` will be true.
				boolean firstOfPair = first && !second;
				lastOfPair = !first && second;
				// ignore any template that is not first in a pair right at the start of a split
				// since it will have been returned in the previous split
				if (virtPos == virtualStart && keepReadPairsTogether && !firstOfPair) {
					continue;
				}
			}

			key.set(getKey(r));
			record.set(r);
			return true;
		}
		return false;
	}
}
