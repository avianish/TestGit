/*******************************************************************************
   � 2014 Brain4ce Education Solutions Pvt. Ltd
 ******************************************************************************/

package in.edureka.mapreduce.advance;

import java.io.IOException;
import java.util.Date;

/*
 * All org.apache.hadoop packages can be imported using the jars present in lib 
 * directory of this java project.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Edureka
 * @version 1.0
 * @since 13-Apr-2014
 * @package in.edureka.mapreduce.advance
 * @copyright � 2014 Brain4ce Education Solutions Pvt. Ltd 
 * <p>MyCounter is a map only MapReduce code that uses the counter to find the 
 * frequency of the below months after analyzing dates
 * 		December
 * 		January
 * 		February
 * The sole purpose of this class is to understand how to implement counters in a 
 * MapReduce code
 */


public class MyCounter {
	
	//Defining MONTH of type enum
		public static enum MONTH{
			DEC,
			JAN,
			FEB
		};
	
	
	//Mapper
	
		/** 
		 * @author Edureka
		 * <p>MyMapper class is static and extends Mapper 
		 * class having four hadoop generics type LongWritable,Text, Text, Text.
		 */
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {

		//Defining a local variable out of type text
        private Text out = new Text();
        
    	/**
    	 * @method map
    	 *<p>Map is spliting each input record and extracting the date segment. 
    	 * Finally from the object of Date (name of the object is time), month is being 
    	 * extracted with the help of getmonth method.
    	 * On the basis of value of month we increment the values for DEC, JAN, FEB enums.                               
    	 * @method_arguments key, value, context
    	 * @return void
    	 */
		
        
        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

        	//Converting the record (single line) to String and storing it in a String variable line
        	String line = value.toString();

        	//Splitting line by using �,� as delimiter and storing it to array of String variable strts
        	String[]  strts = line.split(",");
        	
        	//Saving the date part of the record in the variable lts of type Long 
        	long lts = Long.parseLong(strts[1]);
        	
        	//Creating the object of Date class initialized with the value of lts
        	Date time = new Date(lts);
        	
        	//Extractin the month from the date object
        	int m = time.getMonth();
        	
        	//If month is 11, the DEC enum will be incremented by 10
        	if(m==11){
        		context.getCounter(MONTH.DEC).increment(10);	
        	}
        	
        	//If month is 0, the JAN enum will be incremented by 20
        	if(m==0){      	  	
      	  		context.getCounter(MONTH.JAN).increment(20);
        	}
        	
        	//If month is 1, the FEB enum will be incremented by 30
        	if(m==1){
      	  		context.getCounter(MONTH.FEB).increment(30);
        	}
        	
        	//Setting the value sucess to the variable out of type Text
      	  	out.set("success");
      	  	
      	  //Dumping the key and value from Mapper
      	  context.write(out,out);
        }  
}
	
	//Driver

    /**
     * @method main
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
     */
		
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	  	//Creating a Job object and assigning a job name for identification purposes
	    Job job = new Job();
	    job.setJobName("CounterTest");
    
    	//Setting jar with MyCounter class
	    job.setJarByClass(MyCounter.class);
    
    	//Setting number of reduce task to 0
	    job.setNumReduceTasks(0);

	    //Providing the mapper class name
	    job.setMapperClass(MyMapper.class);

	    //Setting job object with the Data Type of output Key and Value of mapper
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
    
	    //The hdfs input and output directory to be fetched from the command line

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
	    //Waiting for the job to complete
	    job.waitForCompletion(true);
    
	    //Getting the counters for the job
	    Counters counters = job.getCounters();
	    
	    //Find the counter for enum DEC
	    Counter c1 = counters.findCounter(MONTH.DEC);
	    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
	    
	    //Find the counter for enum JAN
	    c1 = counters.findCounter(MONTH.JAN);
	    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
	    
	    //Find the counter for enum FEB
	    c1 = counters.findCounter(MONTH.FEB);
	    System.out.println(c1.getDisplayName()+ " : " + c1.getValue());
	    
  }
}

