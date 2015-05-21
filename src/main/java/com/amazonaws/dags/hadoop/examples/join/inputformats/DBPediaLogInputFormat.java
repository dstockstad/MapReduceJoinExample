package com.amazonaws.dags.hadoop.examples.join.inputformats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

public class DBPediaLogInputFormat extends FileInputFormat<DBPediaLogKey, DBPediaLogValue>{

  public static class DBPediaLogReader extends RecordReader<DBPediaLogKey, DBPediaLogValue> {

    private DBPediaLogKey key = new DBPediaLogKey();
    private DBPediaLogValue value = new DBPediaLogValue();
    private LineReader in;
    private long start;
    private long end;
    private long currentPos;
    private Text line = new Text();

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public DBPediaLogKey getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public DBPediaLogValue getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit split = (FileSplit) inputSplit;
      Configuration conf = context.getConfiguration();
      Path path = split.getPath();
      FSDataInputStream is = path.getFileSystem(conf).open(path);
      in = new LineReader(is, conf);
      start = split.getStart();
      end = start + split.getLength();

      // Seek to the start of the Split
      is.seek(start);

      // If this is not the first block in the file, then skip the first line,
      // assuming that the map task which processes the previous block will read
      // it.
      if (start != 0) {
        start += in.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
      }

      currentPos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      //Read one line beyond the end of the split
      if (currentPos > end) {
        return false;
      }
      currentPos += in.readLine(line);
      if (line.getLength() == 0) {
        return false;
      }

      String[] values = StringUtils.split(line.toString(), '\\', '\t');
      key.setPage(values[0]);
      value.setWww(values[1]);
      value.setCategory(values[2]);
      value.setFlag(values[3]);

      return true;
    }

  }

  @Override
  public RecordReader<DBPediaLogKey, DBPediaLogValue> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
  InterruptedException {
    return new DBPediaLogReader();
  }
}
