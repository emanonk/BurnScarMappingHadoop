import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.slf4j.Logger;

public class MedianFilterPhase extends Configured implements Tool {
	public static int t1=0;
	//public static String strt1,strt2,strt3,strt4;
	//private static final Logger logger= org.slf4j.LoggerFactory.getLogger(MainClass.class);
	
	
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable five = new IntWritable(5);
	    private Text word = new Text();

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int x;
			int y;
			String line = value.toString();
			//logger.info(line);
			StringTokenizer itr = new StringTokenizer(line, ",");
			
			String xstr = itr.nextToken();
			String ystr = itr.nextToken();
			x=Integer.parseInt(xstr);
			y=Integer.parseInt(ystr);
			String filename = itr.nextToken();
			
			String xSizestr = itr.nextToken();
			String ySizestr = itr.nextToken();
			int xSize = Integer.parseInt(xSizestr);
			int ySize = Integer.parseInt(ySizestr);
			
			// o pinakas me ta pixel
			//				0	..	ySize
			//			-------------
			//	0		|	1	2	3
			//	..		|	4	5	6
			//	xSize	|	7	8	9
			//parakatw oi periptwseis einai gia to poia 8esi ston pinaka brisketai to pixel
			//pou meletaw,wste min bgw ektos pinaka
			
			
			//pixel 1
			if(x==0 && y==0){
				//	x	1
				//	2	3
				
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, five);
				//1
				word.set(x + "," + (y+1) + "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//2
				word.set((x+1) + "," + y + "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//3
				word.set((x+1) + "," + (y+1) + "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				
			}
			//pixel 2
			if(x==0 && y>0 && y<ySize){
				//	1	x	2
				//	3	4	5
				
				//1
				word.set(x + "," + (y-1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, five);
				//2
				word.set(x + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//3
				word.set((x+1) + "," + (y-1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//4
				word.set((x+1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//5
				word.set((x+1) + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
			}
			//pixel 3
			if(x==0 && y==ySize){
				//	1	x
				//	2	3
				
				//1
				word.set(x + "," + (y-1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, five);
				//2
				word.set((x+1) + "," + (y-1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//3
				word.set((x+1) + "," + y+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
			}
			//pixel 4
			if(x>0 && y==0 && x<xSize){
				//	1	2
				//	x	3
				//	4	5
				
				//1
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, five);
				//3
				word.set(x + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//4
				word.set((x+1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//5
				word.set((x+1) + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
			}
			//pixel 5
			if(x>0 && y>0 && x<xSize && y<ySize){
				//	1	2	3
				//	4	x	5
				//	6	7	8
				
				//1
				word.set((x-1) + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//3
				word.set((x-1) + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//4
				word.set(x + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, five);
				//5
				word.set(x + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//6
				word.set((x+1) + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//7
				word.set((x+1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//8
				word.set((x+1) + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
			}
			//pixel 6
			if(x>0 && y==ySize && x<xSize){
				//	1	2
				//	3	x
				//	4	5
				
				//1
				word.set((x-1) + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//3
				word.set(x + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, five);
				//4
				word.set((x+1) + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//5
				word.set((x+1) + "," + y+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
			}
			//pixel 7
			if(y==0 && x==xSize){
				//	1	2
				//	x	3
				
				//1
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + (y+1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, five);
				//3
				word.set(x + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
			}
			//pixel 8
			if(y>0 && y<ySize && x==xSize){
				//	1	2	3
				//	4	x	5
				
				//1
				word.set((x-1) + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//3
				word.set((x-1) + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//4
				word.set(x + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, five);
				//5
				word.set(x + "," + (y+1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
			}
			//pixel 9
			if(y==ySize && x==xSize){
				//	1	2
				//	3	x
				
				//1
				word.set((x-1) + "," + (y-1)+ "," + xSizestr + "," + ySizestr + "," + filename);
				output.collect(word, one);
				//2
				word.set((x-1) + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//3
				word.set(x + "," + (y-1) + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, one);
				//x
				word.set(x + "," + y + "," + xSizestr + "," + ySizestr+ "," + filename);
				output.collect(word, five);
			}
			
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException {
			String keyValue = key.toString();
			StringTokenizer itr = new StringTokenizer(keyValue, ",");
			String xstr = itr.nextToken();
			String ystr = itr.nextToken();
			String xSizestr = itr.nextToken();
			String ySizestr = itr.nextToken();
			String filename = itr.nextToken();
			int sum = 0;
		    while (values.hasNext()) {
		    	sum += values.next().get();
		    }
		    if(sum>4){
		    	key.set(filename+":"+xstr+":"+ystr+":"+xSizestr+":"+ySizestr+":");
		    	t1++;
		    	output.collect(key, new IntWritable(sum));
		    }
			
	  }//reduce
	}//reduce_mapclass

	static int printUsage() {
		System.out.println(".. [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), MainClass.class);
		conf.setJobName("wordcount");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MapClass.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "+ args[i - 1]);
				return printUsage();
			}
		}
		FileInputFormat.setInputPaths(conf, "hdfs://localhost:9000/fireProject/MainClassOutput/");
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:9000/fireProject/MedianFilterOutput/"));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Parallel Satellite Image Processing Program\n");
		int res = ToolRunner.run(new Configuration(), new MedianFilterPhase(), args);
		System.out.println("counter  "+t1);//3695296
		System.exit(res);
		
	}

}