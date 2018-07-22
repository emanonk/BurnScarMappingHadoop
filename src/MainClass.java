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
import org.slf4j.Logger;

public class MainClass extends Configured implements Tool {
	public static int counter=0;
	//public static double t1,t2,t3,t4;
	//public static String strt1,strt2,strt3,strt4;
	private static final Logger logger= org.slf4j.LoggerFactory.getLogger(MainClass.class);

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text one = new Text();
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line, ",");
			String filename = itr.nextToken();
			String band = itr.nextToken();
			String xSizestr = itr.nextToken();
			String ySizestr = itr.nextToken();
			String linestr = itr.nextToken();
			int column = 0;

			while (itr.hasMoreTokens()) {
				String pixelValue = itr.nextToken();
				word.set(linestr + "," + column + "," + filename);
				one.set(band + "," +xSizestr+ "," +ySizestr+ "," + pixelValue);
				// logger.info(pixel+":"+value);
				output.collect(word, one);
				column++;
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException {
			int value3 =0;
			int value4 =0;
			int value7 =0;
			double val3 = 0.0;
			double val4 = 0.0;
			double val7 = 0.0;
			String value;
			String band;
			String xSize;
			String ySize;
			String pixelValue;
			
			String keyValue = key.toString();
			// logger.info("key:"+keyValue);
			StringTokenizer itr = new StringTokenizer(keyValue, ",");
			String line = itr.nextToken();
			String column = itr.nextToken();
			String filename = itr.nextToken();
			
			
			while (values.hasNext()) {
				value = values.next().toString();
				StringTokenizer itra = new StringTokenizer(value, ",");
				
				band = itra.nextToken();
				xSize = itra.nextToken();
				ySize = itra.nextToken();
				
				
				pixelValue = itra.nextToken();
				key.set(line + "," + column + "," + filename + "," + xSize +"," + ySize + ",");
				if(band.equals("B03")){
					value3 = Integer.parseInt(pixelValue);
					val3 = 1.0 * value3;
				}else if(band.equals("B04")){
					value4 = Integer.parseInt(pixelValue);
					val4 = 1.0 * value4;
				}else if(band.equals("B07")){
					value7 = Integer.parseInt(pixelValue);
					val7 = 1.0 * value7;
				}else{
					//logger.info("Band not found..ERROR"+xSize+ySize);
				}
				
				
			}// while
			//arxikopoiw 0 ta thresholds
			
			
			double t1 = 126.0;
			double t2 = 60.0;
			double t3 = 50.0;
			double nbr = 0.0; 
			//double ndvi = 0.0;
			double albedo = 0.0;
			//elexgw tis times gia na min petaksei ArithmeticException: / by zero
			
			double	sum47 = val4+val7;
			double dif47 = val4-val7;
			if (sum47!=0){
				nbr = dif47/sum47;
				nbr = (nbr + 1.0) * 255.0 / 2.0;
			}
			
			//ndvi = (val4-val3)/(val4+val3);
			albedo = (val4+val3)/2;
			
			if (nbr>t1 && value4>t2 && albedo>t3) {
				//logger.info(line+" "+ column);
				counter++;
				output.collect(key, new IntWritable(1));//burned
			}else if(nbr>(t1+1) && value4>t2 && albedo>t3){
				//unburned
			}else{
				//###########@@@@@@@@@@@!!!!!!!!!!!!!@@@@@@@@@@@################@@@@@@@@@@@!!!!!!!!!!!!
				//output.collect(key, new IntWritable(2));//unknown
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
		conf.setMapOutputValueClass(Text.class);
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
		FileInputFormat.setInputPaths(conf, "hdfs://localhost:9000//fireProject/tifToTxt/");
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:9000/fireProject/MainClassOutput/"));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		System.out.println("Parallel Satellite Image Processing Program(MainClass\n");
		int res = ToolRunner.run(new Configuration(), new MainClass(), args);
		System.out.println("counter"+counter+"\n");//29157063
		System.exit(res);
	}

}