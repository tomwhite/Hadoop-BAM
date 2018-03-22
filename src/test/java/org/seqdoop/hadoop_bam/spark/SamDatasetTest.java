package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class SamDatasetTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    jsc = new JavaSparkContext("local", "myapp");
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
        {"1.bam", ".bam", 128 * 1024, false},
        {"1.bam", ".bam", 128 * 1024, true},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(String inputFile, String outputExtension, int splitSize, boolean useNio)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();
    SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
        .splitSize(splitSize)
        .useNio(useNio);

    SamDataset samDataset = samDatasetFactory.read(inputPath);

    int expectedCount = getBAMRecordCount(new File(inputPath.replace("file:", "")));
    Assert.assertEquals(expectedCount, samDataset.getReadsRdd().count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    samDatasetFactory.write(samDataset, outputPath);

    Assert.assertEquals(expectedCount, getBAMRecordCount(outputFile));
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

  private static int getBAMRecordCount(final File bamFile) throws IOException {
    final SamReader bamReader = SamReaderFactory.makeDefault()
        .open(SamInputResource.of(bamFile));
    final Iterator<SAMRecord> it = bamReader.iterator();
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    bamReader.close();
    return recCount;
  }

}
