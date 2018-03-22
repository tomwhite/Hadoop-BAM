package org.seqdoop.hadoop_bam.spark;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class VcfDatasetFactory {

  private JavaSparkContext sparkContext;
  private int splitSize;

  public static VcfDatasetFactory makeDefault(JavaSparkContext sparkContext) {
    return new VcfDatasetFactory(sparkContext);
  }

  private VcfDatasetFactory(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  public VcfDatasetFactory splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  public VcfDataset read(String path) throws IOException {
    VcfSource vcfSource = new VcfSource();
    JavaRDD<VariantContext> variants = vcfSource.getVariants(sparkContext, path, splitSize);
    VCFHeader header = vcfSource.getFileHeader(sparkContext, path);
    return new VcfDataset(header, variants);
  }

  public void write(VcfDataset vcfDataset, String path) throws IOException {
    new VcfSink().save(sparkContext, vcfDataset.getFileHeader(), vcfDataset.getVariantsRdd(), path);
  }

}
