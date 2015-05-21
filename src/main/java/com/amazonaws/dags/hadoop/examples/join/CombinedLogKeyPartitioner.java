package com.amazonaws.dags.hadoop.examples.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.amazonaws.dags.hadoop.examples.join.inputformats.CombinedLogKey;

public class CombinedLogKeyPartitioner extends Partitioner<CombinedLogKey, Text>{
  @Override
  public int getPartition(CombinedLogKey key, Text value, int numReduceTasks) {
    return Math.abs(key.getPage().hashCode()) % numReduceTasks;
  }
}