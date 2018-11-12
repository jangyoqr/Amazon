package test3;

import com.google.gson.Gson;
import com.google.gson.*;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class test3 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
		
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new test3(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

		Configuration conf1=new Configuration();
        Job job = Job.getInstance(conf1);
        job.setJarByClass(test3.class);
        job.setOutputKeyClass(Text.class);
		
      	job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setMapperClass(Map3.class);
        job.setReducerClass(Reduce3.class);
		
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
/*
		 FileInputFormat.addInputPath(job, new Path(args[0]));  
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
  		job.waitForCompletion(true);
*/
					
		Path outputPath=new Path("FirstMapper");
		 FileInputFormat.addInputPath(job,new Path(args[0]));
		 FileOutputFormat.setOutputPath(job,outputPath);
		outputPath.getFileSystem(conf1).delete(outputPath);
  		job.waitForCompletion(true);
		Configuration conf2=new Configuration();
	    Job j2=Job.getInstance(conf2);
		j2.setJarByClass(test3.class);
		j2.setMapperClass(Map3_2.class);
		j2.setReducerClass(Reduce3_2.class);
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(DoubleWritable.class);
		j2.setMapOutputValueClass(MapWritable.class);
		Path outputPath1=new Path(args[1]); 
        FileInputFormat.addInputPath(j2, outputPath);
	    FileOutputFormat.setOutputPath(j2, outputPath1);
        outputPath1.getFileSystem(conf2).delete(outputPath1, true);
		j2.waitForCompletion(true);
  
		return 0;
    }
 	 
    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text("result");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

				
				String line = value.toString();
			    String[] tuple = line.split("\\n");
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
					
				    Info info = gson.fromJson(tuple[i], Info.class);
					long num = (long)info.getOverall();
					word.set(info.getAsin());
					context.write(word, new LongWritable(num));
				   }
			//		context.write(new Text("result"), new LongWritable(1));
				}
				catch(Exception e){e.printStackTrace();}
                
                
            }
        }
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

				String line = value.toString();
			    String[] tuple = line.split("\\n");
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
					
				    Info info = gson.fromJson(tuple[i], Info.class);
					String id = info.getReviewerID();
					word.set(id);
					context.write(word, ONE);
				   }
				}
				catch(Exception e){e.printStackTrace();}
                
				}
	}

    public static class Map2_2 extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text("result");
		private Text word2 = new Text();
		MapWritable minfo = new MapWritable();
		@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
				long num =0;
				
				String line = value.toString();
			    String[] tuple = line.split("\\n+");
				
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
						String []data = tuple[i].split("\\s+");
						minfo.put(new Text(data[0]),new LongWritable(Long.parseLong(data[1])));
					} 
				}

				catch(Exception e){e.printStackTrace();}
			}	
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(word,minfo);
			}
	}

    public static class Map3 extends Mapper<LongWritable, Text, Text, MapWritable> {
		private Text word2 = new Text();
		@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
				double num =0;
				
				String line = value.toString();
			    String[] tuple = line.split("\\n");
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
								
					 MapWritable minfo = new MapWritable();
				    Info info = gson.fromJson(tuple[i], Info.class);
					int [] help = info.getHelpful();
					if(help[1]>10){
					word2.set(info.getAsin());
					double num1 = (double)help[0];
					double num2 = (double)help[1];
					minfo.put(new DoubleWritable(help[0]), new DoubleWritable(help[1]));
					context.write(word2,minfo);
					} 
				}
			
			}

				catch(Exception e){e.printStackTrace();}
			}

	}

    public static class Map3_2 extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text("result");
		private Text word2 = new Text();
		MapWritable minfo = new MapWritable();
		@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
				long num =0;
				
				String line = value.toString();
			    String[] tuple = line.split("\\n+");
				
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
						String []data = tuple[i].split("\\s+");
						minfo.put(new Text(data[0]),new DoubleWritable(Double.parseDouble(data[1])));
					} 
				}

				catch(Exception e){e.printStackTrace();}
			}	
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(word,minfo);
			}
	}

    public static class Map4 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

				String line = value.toString();
			    String[] tuple = line.split("\\n");
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
					
				    Info info = gson.fromJson(tuple[i], Info.class);
					String id = info.getReviewerID();
					int []help = info.getHelpful();
					long num = (long)help[0]+help[1];
					word.set(id);
					context.write(word, new LongWritable(num));
				   }
				}
				catch(Exception e){e.printStackTrace();}
                
				}
	}
	
    public static class Map4_2 extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text("result");
		private Text word2 = new Text();
		MapWritable minfo = new MapWritable();
		@Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
				long num =0;
				
				String line = value.toString();
			    String[] tuple = line.split("\\n+");
				
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
						String []data = tuple[i].split("\\s+");
						minfo.put(new Text(data[0]),new LongWritable(Long.parseLong(data[1])));
					} 
				}

				catch(Exception e){e.printStackTrace();}
			}	
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(word,minfo);
			}
	}
	
	


    public static class Map5 extends Mapper<LongWritable, Text, Text, MapWritable> {
        private Text word = new Text();
		MapWritable minfo = new MapWritable(); 
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
				String line = value.toString();
			    String[] tuple = line.split("\\n");
				Gson gson = new Gson();
				try{
					for(int i=0; i<tuple.length; i++)
					{
					
				    Info info = gson.fromJson(tuple[i], Info.class);
					String id = info.getReviewerID();
					String date = info.getReviewTime();
					String changeDate = change(date);
					word.set(id);
				   	minfo.put(word, new LongWritable(Long.parseLong(changeDate)));
					}
				}
				catch(Exception e){e.printStackTrace();}
                
				}

		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(new Text("result"), minfo);
			}
	}
    public static class Reduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
			double sum = 0;
			long total = 0;
            for (LongWritable val : values) {
                sum += val.get();
            	total +=1;
			}
			sum = (double)sum/total;
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static class Reduce2 extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
			long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
			}
			
            context.write(key, new LongWritable(sum));
        }
    }
		
    public static class Reduce2_2 extends Reducer<Text, MapWritable, Text, LongWritable> {
        private long max =0;
		long temp =0;
		private Text txt = new Text();
		@Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {	
					for(MapWritable val : values){
					Set<Writable> keyset = val.keySet();
					for(Writable id : keyset){
						temp = (long)((LongWritable)val.get(id)).get();
						if(max<temp){
							max = temp;
							txt.set(id.toString());
							
						}
					}
				}
		}
			
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(txt, new LongWritable(max));
			
			}
    }

    public static class Reduce3 extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private double a =0;
		double b =0;
		String temp;
		private Text txt = new Text();
		@Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {	
					for(MapWritable val : values){
					Set<Writable> keyset = val.keySet();
					for(Writable id : keyset){
						b += (double)((DoubleWritable)val.get(id)).get();			
						temp = id.toString();	
						a += (double)((DoubleWritable)id).get();
					}
				}
				Double num = a/b;
				context.write(key, new DoubleWritable(num));
		}
	}

    public static class Reduce3_2 extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private double max =(double)0;
		double temp =(double)0;
		private Text txt = new Text();
		@Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
			
				for(MapWritable val : values)
				{
					Set<Writable> keyset = val.keySet();
					for(Writable id : keyset){
						temp = ((DoubleWritable)val.get(id)).get();
						if(max<temp){
							max = temp;
							txt.set(id.toString());
						}
					}
				}
			}
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(txt, new DoubleWritable(max));
			}
	}
			
    public static class Reduce4 extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
			long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
			}
			
            context.write(key, new LongWritable(sum));
        }
	}

    public static class Reduce4_2 extends Reducer<Text, MapWritable, Text, LongWritable> {
        private long max =0;
		long temp =0;
		private Text txt = new Text();
		@Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
			
				for(MapWritable val : values)
				{
					Set<Writable> keyset = val.keySet();
					for(Writable id : keyset){
						temp = ((LongWritable)val.get(id)).get();
						if(max<temp){
							max = temp;
							txt.set(id.toString());
						}
					}
				}
			}
		@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				context.write(txt, new LongWritable(max));
			}
	}

	public class Info{
			String reviewerID;
			String asin;
			String reviewerName;
			int[] helpful = new int[2];
			String reviewText;
			float overall;
			String summary;
			int unixReviewTime;
			String reviewTime;
			public String getReviewerID() {return reviewerID;}
			public void setReviewerID(String reviewrID) { this.reviewerID = reviewerID;}
			public String getAsin() {return asin;}
			public void setAsin(String asin) {this.asin = asin;}
			public String getReviewerName() {return reviewerName;}
			public void setReviewerName(String reviewerName) { this.reviewerName = reviewerName;}			
			public int[] getHelpful() {return helpful;	}
			public void setHelpful(int[] helpful) {	this.helpful = helpful;}
			public String getReviewText() {	return reviewText;	}
			public void setReviewText(String reviewText) {	this.reviewText = reviewText;	}		
			public float getOverall() {return overall;}										
			public void setOverall(float overall) {this.overall = overall;}										
			public String getSummary() {return summary;}
            public void setSummary(String summary) {this.summary = summary;}
			public int getUnixReviewTime() {return unixReviewTime;}
		    public void setUnixReviewTime(int unixReviewTime) {this.unixReviewTime = unixReviewTime;}
			public String getReviewTime() {return reviewTime;}
			public void setReviewTime(String reviewTime) {this.reviewTime = reviewTime;}
																										
	}
	public static String addLeadingZero (String a) {
		if(a.length() == 1) {
			a = "0" + a;
		}
		return a;
	}
		public static String change(String a) {
			String[] splitStr = a.split(", ");
			String[] splitStr2 = splitStr[0].split(" ");
			String str = splitStr[1] + addLeadingZero(splitStr2[0]) + addLeadingZero(splitStr2[1]);
			return str;
		}
}

