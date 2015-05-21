package com.amazonaws.dags.hadoop.examples.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.amazonaws.dags.hadoop.examples.join.inputformats.CombinedLogKey;

public class CombinedLogGroupingComparator extends WritableComparator {
  
  protected CombinedLogGroupingComparator() {
    super(CombinedLogKey.class, true);
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CombinedLogKey key1 = (CombinedLogKey) w1;
    CombinedLogKey key2 = (CombinedLogKey) w2;
    return key1.getPage().compareTo(key2.getPage());  
  }
}