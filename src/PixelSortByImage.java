/*
 * o codikas einai se auto to link
 * https://sites.google.com/site/hadoopandhive/home/how-to-write-output-to-multiple-named-files-in-hadoop-using-multipletextoutputformat
 * 
 * i iksodos tou medianFilterPhase einai i eisodos se auto to programma
 * ti kanei?
 * pairnei apo ena arxeio pou periexei ta kamena pixel apo oles tis eikones
 * kai ta xwrizei se arxeia ana eikona...
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.slf4j.Logger;

public class PixelSortByImage{
	public static int counter=0; 
	private static final Logger logger= org.slf4j.LoggerFactory.getLogger(MainClass.class);
	
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    
		
		private Text val = new Text();

		public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String [] dall=value.toString().split(":");
			val.set(dall[1]+":"+dall[2]);
			output.collect(new Text(dall[0]+":"+dall[3]+":"+dall[4]),val);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)throws IOException {
			
			String [] dall=key.toString().split(":");
			String filename = dall[0];
			//logger.info("filename............................."+filename);
			String xSizestr = dall[1];
			//logger.info("xSizestr............................."+xSizestr);
			String ySizestr = dall[2];
			//logger.info("ySizestr............................."+ySizestr);
			int xSize = Integer.parseInt(xSizestr);
			int ySize = Integer.parseInt(ySizestr);
			int[][] pixels=new int[xSize][ySize];
			
			for(int x=0;x<xSize;x++){
            	for(int y=0;y<ySize;y++){
                	pixels[x][y]=0;
                }
            }
			
			while (values.hasNext()) {
				String [] dalla=values.next().toString().split(":");
				String xIn = dalla[0];
				String yIn = dalla[1];
				int xI = Integer.parseInt(xIn);
                int yI = Integer.parseInt(yIn);
				pixels[xI][yI]=1;//ta kamena pixel ta kanw 1
                }
			//filename="201";
			int[][] result =ConnectComponent.ConComp(xSize, ySize, pixels);
			//String fle = filename.substring(14, 16);
			//String fle = filename.substring(1, 3);
			for(int x=0;x<xSize;x++){
            	for(int y=0;y<ySize;y++){
            		if(result[x][y]!=0)
        			output.collect(new Text(filename), new Text(x+":"+y));
            		counter++;
            		
                }
            }
			
			//output.collect(key, values.next());
			
	  }//reduce
	}//reduce_mapclass

	static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
        protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                return key.toString();
        }
}

	public static void main(String[] args) throws Exception {

        Configuration mycon=new Configuration();
        JobConf conf = new JobConf(mycon,PixelSortByImage.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(MultiFileOutput.class);

        FileInputFormat.setInputPaths(conf,"hdfs://localhost:9000/fireProject/MedianFilterOutput/");
        FileOutputFormat.setOutputPath(conf,new Path("hdfs://localhost:9000/fireProject/SortByImageOutput/"));
        JobClient.runJob(conf);
		System.out.println("counter "+ counter);//58314125
	}

}
	
