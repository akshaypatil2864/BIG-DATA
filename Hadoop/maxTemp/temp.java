import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;



   public class temp
 {
   public static class TempMapper extends  Mapper <Object,Text,Text,FloatWritable>  {
   public void map(Object key,Text value,Context context) throws IOException,InterruptedException
   {
   String line[]=value.toString().split("\\s+");
   float maxTemp=Float.parseFloat(line[5]);
   String year=line[1].substring(0,4);
   if(maxTemp >-60.0f && maxTemp <60.0f)
   context.write(new Text(year),new FloatWritable(maxTemp));
   }
   }
  public static class TempReducer extends  Reducer <Text,FloatWritable,Text,FloatWritable>  {
  String year=null;
  float globaltemp=0.0f;
   public void reduce(Text key,Iterable<FloatWritable>values,Context context) throws IOException,InterruptedException
   {
   float max=-100;
   for(FloatWritable temp :values) {
   if(temp.get() > max)
   max=temp.get();
   }
   if (max > globaltemp){
   globaltemp=max;
   year=key.toString();
   }
  // context.write(key,new FloatWritable(max));
   }
   
   public void cleanup(Context context) throws IOException,InterruptedException{
   context.write(new Text(year),new FloatWritable(globaltemp));
   }
}
public static void main(String args[]) throws Exception
{
 //create the object of configuration class
 Configuration conf = new Configuration();
 
 // create the object of job class
 Job job=new Job(conf,"max temp.");
 
 //set the data type of output key
 job.setOutputKeyClass(Text.class);
 
 //set the data type of output value 
 job.setOutputValueClass(FloatWritable.class);
 
 //set the data format of output
 job.setOutputFormatClass(TextOutputFormat.class);
 
 //set the data format of input
 job.setInputFormatClass(TextInputFormat.class);
 
 //set the name of mapper class
 job.setMapperClass(TempMapper.class);
 
 //set the name of reducer class
 job.setReducerClass(TempReducer.class);
 // set the input files path from 0 th arguement
 FileInputFormat.addInputPath(job, new Path(args[0]));
 
 // set the output files path from 1 st arguement
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
 //execute the job and wait for completion
 job.waitForCompletion(true);
 }
 }
 

 
 
 
