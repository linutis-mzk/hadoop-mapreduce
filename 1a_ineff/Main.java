import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparator;

public class Main {

  public static class FloatComparator extends WritableComparator {

    public FloatComparator() {
        super(FloatWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,byte[] b2, int s2, int l2) {

      float thisValue = readFloat(b1, s1);
      float thatValue = readFloat(b2, s2);
      return Float.compare(thisValue, thatValue) * (-1);
    }
  }

  public static class SumMapper extends Mapper<Object, Text, Text, FloatWritable>{

    HashMap<Text, Float> netSalesStore = new HashMap<Text, Float>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      
      String[] tokens;
      tokens = line.split("\\|",-1);


      boolean is_null = (tokens[0].isEmpty()) || (tokens[7].isEmpty()) || ( tokens[20].isEmpty());
      if(!is_null){
        Integer ss_sold_date_sk = Integer.parseInt(tokens[0]);
        Text ss_store_sk = new Text(tokens[7]);
        Float ss_net_paid = Float.parseFloat(tokens[20]);

        Configuration conf = context.getConfiguration();
        Integer start_date = Integer.parseInt(conf.get("start_date"));
        Integer end_date = Integer.parseInt(conf.get("end_date"));

        if((ss_sold_date_sk >= start_date) && (ss_sold_date_sk <= end_date)){
          if(netSalesStore.get(ss_store_sk) == null){
            netSalesStore.put(ss_store_sk, ss_net_paid);
          }
          else{
            netSalesStore.put(ss_store_sk,  netSalesStore.get(ss_store_sk) + ss_net_paid);
          }
        }
      }  
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Text key : netSalesStore.keySet()) {
          //String str_paid = Float.toString(netSalesStore.get(key));
           //context.write(key,new Text(str_paid));
          context.write(key,new FloatWritable(netSalesStore.get(key)));
      } 
    } //~cleanup

  }

  public static class SumReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    
    private TreeMap<Float, Text> TopKMap = new TreeMap<Float, Text>() ;

    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      Integer K = Integer.parseInt(conf.get("K"));

      float sum = 0;
      //Float value;
      for (FloatWritable val : values) {
        //value =  Float.parseFloat(val.toString());
        //sum += value;
        sum += val.get();
      }

      TopKMap.put(new Float(sum),new Text(key));
      if (TopKMap.size() > K) {
          TopKMap.remove(TopKMap.firstKey());
      }
            
    }
  
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Float key : TopKMap.keySet()) {
          // context.write(TopKMap.get(key),new Text(Float.toString(key)));
          context.write(TopKMap.get(key),new FloatWritable(key));
      }   
    } //~cleanup
  
  }

  

  public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens;
      tokens = line.split("\t");

      String store_num = tokens[0];
      String net_sales = tokens[1];

      // context.write(new Text(net_sales), new Text(store_num));
      context.write(new FloatWritable(Float.parseFloat(net_sales)), new Text(store_num));
    }

  }

  public static class SortReducer extends Reducer<FloatWritable,Text,Text,FloatWritable> {

    public void reduce(FloatWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

      for (Text val : values) {
        context.write(val,key);
      }
      
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();

    conf1.set("K", args[0]);
    conf1.set("start_date", args[1]);
    conf1.set("end_date", args[2]);

    Job area_job = Job.getInstance(conf1, "word count");

    area_job.setJarByClass(Main.class);
    area_job.setMapperClass(SumMapper.class);
    area_job.setReducerClass(SumReducer.class);

    area_job.setMapOutputKeyClass(Text.class);
    area_job.setMapOutputValueClass(FloatWritable.class);
    area_job.setOutputKeyClass(Text.class);
    area_job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(area_job, new Path(args[3]));
    FileOutputFormat.setOutputPath(area_job, new Path(args[4]+"_temp"));

    area_job.waitForCompletion(true);

    Configuration conf2 = new Configuration();

    Job sort_job = Job.getInstance(conf2, "sort count");

    sort_job.setJarByClass(Main.class);
    sort_job.setMapperClass(SortMapper.class);

    sort_job.setSortComparatorClass(FloatComparator.class);
    sort_job.setReducerClass(SortReducer.class);

    sort_job.setMapOutputKeyClass(FloatWritable.class);
    sort_job.setMapOutputValueClass(Text.class);
    sort_job.setOutputKeyClass(Text.class);
    sort_job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(sort_job, new Path(args[4]+"_temp"));
    FileOutputFormat.setOutputPath(sort_job, new Path(args[4]));
      
    System.exit(sort_job.waitForCompletion(true) ? 0 : 1);

  }
}

