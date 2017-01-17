package cn.itcase.mr.demo01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntegleChemDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(ChemMapper.class);
		job.setReducerClass(ChemReducer.class);
		
		job.setJarByClass(IntegleChemDriver.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
	/**
	 * mapper
	 * @author Administrator
	 *
	 */
	static class ChemMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		protected void map(LongWritable key, Text value, Context context) 
				throws java.io.IOException ,InterruptedException {
			String str = value.toString();
			
			str = str.replaceAll("[\\pP\\p{Punct}]", "");  
			
			String[] strarr = str.split("");
			int length = strarr.length;
			
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < length; i++) {
				sb.delete(0, sb.length());
				if(strarr[i].trim().equals("")) {
					continue;
				}
				sb.append(strarr[i]);
				context.write(new Text(sb.toString()), new IntWritable(1));
				
				for(int j = i+1; j < length; j++) {
					if(strarr[j].trim().equals("")) {
						continue;
					}
					sb.append(strarr[j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
			}
		};
	}
	
	/**
	 * reducer
	 * @author Administrator
	 *
	 */
	static class ChemReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws java.io.IOException ,InterruptedException {
			int count = 0;
			for(IntWritable iw : values) {
				count += iw.get();
			}
			
			if (count > 1) {
				context.write(key, new Text("\t"+count+"\nx:1"));
			}
		};
	}
}
