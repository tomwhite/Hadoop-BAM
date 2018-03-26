package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.util.Locatable;
import java.util.List;

public class TraversalParameters<T extends Locatable> {
  private final List<T> intervalsForTraversal;
  private final boolean traverseUnplacedUnmapped;

  public TraversalParameters(List<T> intervalsForTraversal, boolean traverseUnplacedUnmapped) {
    this.intervalsForTraversal = intervalsForTraversal;
    this.traverseUnplacedUnmapped = traverseUnplacedUnmapped;
  }

  public List<T> getIntervalsForTraversal() {
    return intervalsForTraversal;
  }

  public boolean getTraverseUnplacedUnmapped() {
    return traverseUnplacedUnmapped;
  }
}
