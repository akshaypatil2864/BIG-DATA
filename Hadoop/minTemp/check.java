import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.FloatWritable; 
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
public class check
{

	public static class TempMapper extends Mapper <Object,Text,Text, FloatWritable>
	{


		public void map(Object key, Text value, Context context) throws IOException,InterruptedException
		{
		String line []= value.toString().split("\\s+");
		Float maxTemp = Float.parseFloat(line[6]);
		String year = line[1].substring(0,4);
		if(maxTemp> -60.0f && maxTemp < 60.0f )
		{
			context.write(new Text(year), new FloatWritable(maxTemp));
		}
		}
	}
public static class TempReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
{
	float h=0.0f;
	String year=null;
	public void reduce (Text key, Iterable<FloatWritable> values,Context context) throws IOException,InterruptedException
	{
		float max=100;
		
		for(FloatWritable temp : values)
		{
		if(temp.get()<max)
			max=temp.get();
		}
		if(max<h)
		{
		h=max;
		year=key.toString();
		}
	}
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text(year), new FloatWritable(h));
		
		}
		
	
	
}
	public static void main(String args[]) throws Exception
	{
	// create the object of Configuration class
	Configuration conf = new Configuration();

	// create the object og Job class
	Job job = new Job(conf,"Hottest Year");

	//Set the data type of output key
	job.setOutputKeyClass(Text.class);

	//Set the data type of output value
	job.setOutputValueClass(FloatWritable.class);

	// Set the data format of output
	job.setOutputFormatClass(TextOutputFormat.class);

	// Set the data format of input
	job.setInputFormatClass(TextInputFormat.class);
	
	//set the name of Mapper class
	job.setMapperClass(TempMapper.class);
	job.setReducerClass(TempReducer.class);
	//job.setNumReduceTasks(0);

	// Set the name of Reducer Class

	//Set the input files path from 0th argument
	FileInputFormat.addInputPath(job,new Path(args[0]));

	//Set the output files  path from 1st argument
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	//Execute the job and wait for completion
	job.waitForCompletion(true);

	}
	}


