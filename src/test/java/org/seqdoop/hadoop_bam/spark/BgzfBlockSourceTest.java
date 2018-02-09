package org.seqdoop.hadoop_bam.spark;

import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seqdoop.hadoop_bam.spark.BgzfBlockGuesser.BgzfBlock;

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
  public void testFindAllBlocks() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the blocks in each partition
    JavaRDD<BgzfBlock> bgzfBlocks = new BgzfBlockSource().getBgzfBlocks(jsc, path, splitSize);
    List<BgzfBlock> collect = bgzfBlocks.collect();

    Assert.assertEquals(26, collect.size());

    Assert.assertEquals(0, collect.get(0).pos);
    Assert.assertEquals(14146, collect.get(0).cSize);
    Assert.assertEquals(65498, collect.get(0).uSize);
  }

  @Test
  public void testFindAllBlocksNio() throws IOException {
    int splitSize = 1 * 128 * 1024;

    String path = "file:///Users/tom/workspace/spark-bam/test_bams/src/main/resources/1.bam";

    // find all the blocks in each partition
    JavaRDD<BgzfBlock> bgzfBlocks = new BgzfBlockSource(true).getBgzfBlocks(jsc, path, splitSize);
    List<BgzfBlock> collect = bgzfBlocks.collect();

    Assert.assertEquals(26, collect.size());

    Assert.assertEquals(0, collect.get(0).pos);
    Assert.assertEquals(14146, collect.get(0).cSize);
    Assert.assertEquals(65498, collect.get(0).uSize);
  }


}
