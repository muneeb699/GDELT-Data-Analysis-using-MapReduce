package au.rmit.bde;

/*
 Hadoop libraries
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJobConf extends Configured implements Tool {

	// Map Class
	static public class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		//Key is set as Text Type
		private Text Key = new Text();
		
		//Value set as IntWritable
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			//read a line
			String line = value.toString();
	        
			//Split the line by tab delimiter
			String fields[] = line.split("\\t");
			
			//Catch the Number Format Exception
	        try{
	        	
	        	//Check the event code is Protest(14)	
	        	if ((Integer.parseInt(fields[27]) == 14)) 
	        	{
	        		//Set the country name
	        		String countryName = fields[51];
	            	if (!countryName.equalsIgnoreCase("") && countryName.equalsIgnoreCase("PK")) 
	            	{
	            	//Date format to Year/Month
	                String date = fields[2].substring(0, 4) + "/"
	                                + fields[2].substring(4, 6); 
	                
	                //set key value
	                Key.set(countryName + " " + date);
	                context.write(Key, one);
	            	}
	        	}
	        }
	        catch(NumberFormatException ex)
	        {
	        	System.err.println("Wrong Format");
	        }
		}
	}

	// Reducer
	static public class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			
			//calculate the sum of counts
			int sum = 0;
			for(IntWritable count :values)
			{
				sum+= count.get();
				
			}
			context.write(key, new IntWritable(sum));
	       
		}
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// Initialising Map Reduce Job
		Job job = new Job(configuration, "Word Count");

		// Set Map Reduce main jobconf class
		job.setJarByClass(WordCountJobConf.class);

		// Set Mapper class
		job.setMapperClass(WordCountMapper.class);

		// Set Combiner class
		job.setCombinerClass(WordCountReducer.class);

		// set Reducer class
		job.setReducerClass(WordCountReducer.class);

		// set Input Format
		job.setInputFormatClass(TextInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountJobConf(), args));
	}
}