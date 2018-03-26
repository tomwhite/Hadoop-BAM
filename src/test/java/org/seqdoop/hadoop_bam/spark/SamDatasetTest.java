package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seqdoop.hadoop_bam.BAMTestUtil;

@RunWith(JUnitParamsRunner.class)
public class SamDatasetTest extends BaseTest {

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
        {"1.bam", null, ".bam", 128 * 1024, false},
        {"1.bam", null, ".bam", 128 * 1024, true},
        {"valid.cram", "valid.fasta", ".bam", 128 * 1024, false},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(String inputFile, String cramReferenceFile, String outputExtension, int splitSize, boolean useNio)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();

    SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize)
        .useNio(useNio);
    ReferenceSource referenceSource = null;
    if (cramReferenceFile != null) {
      String cramReferencePath = ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI().toString();
      samDatasetFactory.referenceSourcePath(cramReferencePath);
      referenceSource = new ReferenceSource(new File(ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI()));
    }

    SamDataset samDataset = samDatasetFactory.read(inputPath);

    int expectedCount = getBAMRecordCount(new File(inputPath.replace("file:", "")), referenceSource);
    Assert.assertEquals(expectedCount, samDataset.getReadsRdd().count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    samDatasetFactory.write(samDataset, outputPath);

    Assert.assertEquals(expectedCount, getBAMRecordCount(outputFile, referenceSource));
  }

  @Test
  public void testReadBamsInDirectory() throws IOException, URISyntaxException {
    SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
        .splitSize(128 * 1024);

    // directory containing two BAM files
    File inputDir = new File(ClassLoader.getSystemClassLoader().getResource("HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam").toURI());
    String inputPath = inputDir.toURI().toString();

    SamDataset samDataset = samDatasetFactory.read(inputPath);

    int expectedCount =
        getBAMRecordCount(new File(inputDir, "part-r-00000.bam")) +
        getBAMRecordCount(new File(inputDir, "part-r-00001.bam"));
    Assert.assertEquals(expectedCount, samDataset.getReadsRdd().count());
  }

  private Object[] parametersForTestIntervals() {
    return new Object[][] {
        {
          new TraversalParameters<>(Arrays.asList(
            new Interval("chr21", 5000, 9999), // includes two unpaired fragments
            new Interval("chr21", 20000, 22999)
          ), false)
        },
        {
            new TraversalParameters<>(Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
            ), false)
        },
        {
            new TraversalParameters<>(Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)
            ), true)
        },
        {
            new TraversalParameters<>(null, true)
        },
    };
  }

  @Test
  @Parameters
  public <T extends Locatable> void testIntervals(TraversalParameters<T> traversalParameters) throws Exception {
    String inputPath = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate).toURI().toString();

    SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
        .splitSize(40000)
        .useNio(false);

    SamDataset samDataset = samDatasetFactory.read(inputPath, traversalParameters);

    int expectedCount = getBAMRecordCount(new File(inputPath.replace("file:", "")), null, traversalParameters);
    Assert.assertEquals(expectedCount, samDataset.getReadsRdd().count());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMappedOnlyFails() throws Exception {
    String inputPath = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate).toURI().toString();

    SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
        .splitSize(40000)
        .useNio(false);

    samDatasetFactory.read(inputPath, new TraversalParameters<>(null, false));
  }

  private static int getBAMRecordCount(final File bamFile) throws IOException {
    return getBAMRecordCount(bamFile, null);
  }

  private static int getBAMRecordCount(final File bamFile, ReferenceSource referenceSource) throws IOException {
    return getBAMRecordCount(bamFile, referenceSource, null);
  }

  private static <T extends Locatable> int getBAMRecordCount(final File bamFile, ReferenceSource referenceSource, TraversalParameters<T> traversalParameters) throws IOException {
    int recCount = 0;
    try (SamReader bamReader = SamReaderFactory.makeDefault()
        .referenceSource(referenceSource)
        .open(SamInputResource.of(bamFile))) {
      Iterator<SAMRecord> it;
      if (traversalParameters == null) {
        it = bamReader.iterator();
      } else if (traversalParameters.getIntervalsForTraversal() == null) {
        it = Collections.emptyIterator();
      } else {
        SAMSequenceDictionary sequenceDictionary = bamReader.getFileHeader()
            .getSequenceDictionary();
        QueryInterval[] queryIntervals = BoundedTraversalUtil
            .prepareQueryIntervals(traversalParameters.getIntervalsForTraversal(), sequenceDictionary);
        it = bamReader.queryOverlapping(queryIntervals);
      }
      while (it.hasNext()) {
        it.next();
        recCount++;
      }
    }

    if (traversalParameters != null && traversalParameters.getTraverseUnplacedUnmapped()) {
      try (SamReader bamReader = SamReaderFactory.makeDefault()
          .referenceSource(referenceSource)
          .open(SamInputResource.of(bamFile))) {
        Iterator<SAMRecord> it = bamReader.queryUnmapped();
        while (it.hasNext()) {
          it.next();
          recCount++;
        }
      }
    }

    return recCount;
  }

}
