/*
	Project 2
	Name: Ismail Vandeliwala (1000990475)
	Name: Prerna Patil (1001054381)
*/

package org.practice.hadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Project2 {

public static class MonthMapper extends Mapper<Object, Text, Text, DoubleWritable> {
 private Text temp = new Text();
 private Text prec = new Text();
  
 @Override
 public void map(Object key, Text value,
   Context contex) throws IOException, InterruptedException {
  // Break line into words for processing
	 String line = value.toString();
	 String words[] = line.split(",");
	 
	 // make a key.. station + year + month
	 String k = words[1] + " " + words[2].substring(0, words[2].length() - 2);
	 // check for null or junk value in temperature column else will discard the row
	 if(words[4] != null && words[5] != null && Double.parseDouble(words[4]) != -9999
			 && Double.parseDouble(words[5]) != -9999){
		 // calculate avg temerature for a day from max n min temp
		 Double avgTempForSingleDay = (Double.parseDouble(words[4]) + Double.parseDouble(words[5]))/2;
		 DoubleWritable avgTempSingleDay = new DoubleWritable(avgTempForSingleDay);
		 
		 temp.set(k + "- Temperature");
		 contex.write(temp, avgTempSingleDay);
	 }
	 
	 // check for null or junk value in precipitation column else will discard the row
	 if(words[3] != null && Double.parseDouble(words[3]) != -9999){
		 
		 Double precipitationForSingleDay = Double.parseDouble(words[3]);
		 DoubleWritable precSingleDay = new DoubleWritable(precipitationForSingleDay);
		 
		 prec.set(k + "- Precipitation");
		 contex.write(prec, precSingleDay);
	 }
 }
} 

public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  
  
 @Override
 public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
  DoubleWritable one = new DoubleWritable();
  Iterator<DoubleWritable> it=values.iterator();
  double total = 0D;
  int count = 0;
  
  // sum up all values for a particular key
  while (it.hasNext()) {
   total += it.next().get();
   count++;
  }
  // calculate the average
  total = total/count;
  one.set(total);
  context.write(key, one);
 }
}

public static class SeasonalMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	 private Text temp = new Text();
	 private Text prec = new Text();
	  
	 @Override
	 public void map(Object key, Text value,
	   Context contex) throws IOException, InterruptedException {
	  // Break line into words for processing
		 String line = value.toString();
		 String words[] = line.split(",");
		 
		 // Define intital key : station + year
		 String k = words[1] + " " + words[2].substring(0,4); 
		 String month = words[2].substring(4, 6);
		 
		 // check for month. categorize them into different season and append it to the key
		 if(month.equals("12") || month.equals("01") || month.equals("02"))
			 k = k + " " + "Winter";
		 else if(month.equals("03") || month.equals("04") || month.equals("05"))
			 k = k + " " + "Spring";
		 else if(month.equals("06") || month.equals("07") || month.equals("08"))
			 k = k + " " + "Summer";
		 else
			 k = k + " " + "Fall";
		 
		 if(words[4] != null && words[5] != null && Double.parseDouble(words[4]) != -9999
				 && Double.parseDouble(words[5]) != -9999){
			 
			 Double avgTempForSingleDay = (Double.parseDouble(words[4]) + Double.parseDouble(words[5]))/2;
			 DoubleWritable avgTempSingleDay = new DoubleWritable(avgTempForSingleDay);
			 
			 temp.set(k + "- Temperature");
			 contex.write(temp, avgTempSingleDay);
		 }
		 
		 if(words[3] != null && Double.parseDouble(words[3]) != -9999){
			 
			 Double precipitationForSingleDay = Double.parseDouble(words[3]);
			 DoubleWritable precSingleDay = new DoubleWritable(precipitationForSingleDay);
			 
			 prec.set(k + "- Precipitation");
			 contex.write(prec, precSingleDay);
		 }
			 
	 }
	} 
	
public static void main(String[] args) throws Exception {
        if (args.length != 5) {
          System.out.println("usage: [input] [output] [type] [reducer] [mapper]");
          System.exit(-1);
        }
   
        Calendar calendar = Calendar.getInstance();
        long start = calendar.getTimeInMillis();
        
        int fileSize = 79490414;
        int no_of_mappers = Integer.parseInt(args[4]);
        
        Configuration configuration = new Configuration();
		
		// split the file size depending on number of mappers to be used
        configuration.setInt("mapreduce.input.fileinputformat.split.minsize", fileSize/no_of_mappers);
        configuration.setInt("mapreduce.input.fileinputformat.split.maxsize", fileSize/no_of_mappers);
        
        Job job = Job.getInstance(configuration,"ismail");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
 
        if(args[2].equalsIgnoreCase("month"))
        	job.setMapperClass(MonthMapper.class);
        else
        	job.setMapperClass(SeasonalMapper.class);
        job.setReducerClass(AverageReducer.class); 
        
        job.setNumReduceTasks(Integer.parseInt(args[3]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.setJarByClass(Project2.class);
 
        job.submit();
        
        job.waitForCompletion(true);
        
        Calendar calendar1 = Calendar.getInstance();
        long end = calendar1.getTimeInMillis();
        
        System.out.println("Total time in mili : "+(end-start));
   
 }
}


// References: 
// www.codefusion.blogspot.com/2013/10/hadoop-wordcount-with-new-map-reduce-api.html

// How to run:
// Will run on eclipse.
// Parameters to be passed: inputFilePath outputFilePath Type(month or season) no_of_reducer no_of_mapper