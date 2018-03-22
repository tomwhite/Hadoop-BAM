package org.seqdoop.hadoop_bam.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.seqdoop.hadoop_bam.spark.BgzfBlockGuesser.BgzfBlock;

@RunWith(JUnitParamsRunner.class)
public class BgzfBlockSourceTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    jsc = new JavaSparkContext("local", "myapp");
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }
  
  @Test
  @Parameters({ "false", "true" })
  public void testFindAllBlocks(boolean useNio) throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    int splitSize = 128 * 1024;

    // find all the blocks in each partition
    JavaRDD<BgzfBlock> bgzfBlocks = new BgzfBlockSource(useNio).getBgzfBlocks(jsc, inputPath, splitSize);
    List<BgzfBlock> collect = bgzfBlocks.collect();

    Assert.assertEquals(26, collect.size());

    Assert.assertEquals(0, collect.get(0).pos);
    Assert.assertEquals(14146, collect.get(0).cSize);
    Assert.assertEquals(65498, collect.get(0).uSize);
  }
}
