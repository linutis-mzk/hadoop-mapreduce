import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class Main {

  public static class AreaMapper extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens;
      tokens = line.split(",");

      Text colour = new Text(tokens[0]);

      int width = Integer.parseInt(tokens[1]);
      int height = Integer.parseInt(tokens[2]);
      int area = width * height;

      Text area_txt = new Text(Integer.toString(area));

      context.write(colour,area_txt);
    }

  }

  public static class AreaReducer extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      int max = 0;
      Integer value;
      for (Text val : values) {
        value =  Integer.parseInt(val.toString());
        if (max < value)
        {
          max = value;
        }
      }
      context.write(new Text(key),new Text(Integer.toString(max)));
    }
  }

  public static class SortMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens;
      tokens = line.split("\t");

      String colour = tokens[0];
      String area = tokens[1];

      context.write(new Text(area), new Text(colour));
    }

  }

  // public static class DecreasingComparator extends Comparator {
        
  //   @Override
  //   public int compare(WritableComparable a, WritableComparable b) {
  //     return -super.compare(a, b);
  //   }
  //   @Override
  //   public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
  //     return -super.compare(b1, s1, l1, b2, s2, l2);
  //   }
  // }

  // public class SortFloatComparator extends WritableComparator {

  //   //Constructor.
    
  //   protected SortFloatComparator() {
  //     super(FloatWritable.class, true);
  //   }
    
  //   @SuppressWarnings("rawtypes")

  //   @Override
  //   public int compare(WritableComparable w1, WritableComparable w2) {
  //     FloatWritable k1 = (FloatWritable)w1;
  //     FloatWritable k2 = (FloatWritable)w2;
      
  //     return -1 * k1.compareTo(k2);
  //   }
  // }


  public static class SortReducer extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      Text item = new Text();

      for (Text val : values) {
        context.write(val,key);
      }
      
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job area_job = Job.getInstance(conf1, "word count");
    area_job.setJarByClass(Main.class);
    area_job.setMapperClass(AreaMapper.class);
    area_job.setCombinerClass(AreaReducer.class);
    area_job.setReducerClass(AreaReducer.class);
    area_job.setOutputKeyClass(Text.class);
    area_job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(area_job, new Path(args[0]));
    FileOutputFormat.setOutputPath(area_job, new Path(args[1]));
    area_job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job sort_job = Job.getInstance(conf2, "sort count");
    sort_job.setJarByClass(Main.class);
    sort_job.setMapperClass(SortMapper.class);
    //sort_job.setCombinerClass(SortReducer.class); 

    sort_job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

    sort_job.setReducerClass(SortReducer.class);
    sort_job.setOutputKeyClass(Text.class);
    sort_job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(sort_job, new Path(args[1]));
    FileOutputFormat.setOutputPath(sort_job, new Path(args[2]));
    System.exit(sort_job.waitForCompletion(true) ? 0 : 1);
  }
}

