import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;


import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;


public class Control {

	 
	 public static int[][] read_pixels_from_file(String fileName) throws IOException {

		 String line ;
         StringTokenizer tokenizer;
         String filename =null;
         String xIndicator ;
         String yIndicator ;
         String xSizestr ;
         String ySizestr ;
         int xSize=0,ySize=0,xI,yI;
	        BufferedReader buff = null;
	        int[][] pixels=null;
	        
	        try {
	            buff = new BufferedReader(new FileReader(fileName));
	        } catch (IOException ex) {
	            System.err.println("Could not read " + fileName);
	        } catch (Exception ex) {
	            System.err.println("Error occurred");
	            System.err.println(ex.getMessage());
	        }



	        if (buff != null) {
	            
                //to kanw prwti fora edw gia na parw to filename,kai to x,ySize gia na ftiaksw to pinaka pixels
                line = buff.readLine();
                tokenizer = new StringTokenizer(line, "\t:");
                
            	filename = tokenizer.nextToken();
                xIndicator = tokenizer.nextToken();
                yIndicator = tokenizer.nextToken();
                xSizestr = tokenizer.nextToken();
                ySizestr = tokenizer.nextToken();
                xSize = Integer.parseInt(xSizestr)+1;
                ySize = Integer.parseInt(ySizestr)+1;
                //dimiourgw ton pinaka
                pixels = new int[xSize][ySize];
                //arxikopoiw ola ta pixel wste osa pixel den brei sto arxeio na einai 0
                for(int x=0;x<xSize;x++){
                	for(int y=0;y<ySize;y++){
                    	pixels[x][y]=0;
                    }
                }
	            while ((line = buff.readLine()) != null) {
	            	tokenizer = new StringTokenizer(line, "\t:");
	                
	            	filename = tokenizer.nextToken();
	                xIndicator = tokenizer.nextToken();
	                yIndicator = tokenizer.nextToken();
	                xI = Integer.parseInt(xIndicator);
	                yI = Integer.parseInt(yIndicator);
	                //xSizestr = tokenizer.nextToken();
	                //ySizestr = tokenizer.nextToken();
	                //System.out.println("filename:" + filename + "..x:" + xIndicator + "..y:" + yIndicator + ".xSize:" + xSize + "..ySize:" + ySize );
	    	        pixels[xI][yI]=1;//ta kamena pixel ta kanw 1.
	                
	            }
	            buff.close();
	            buff = null;
	            
	        }
	        int[][] test=ConnectComponent.ConComp (xSize,ySize,pixels);
	        
	        System.out.print("\n");System.out.print("\n");System.out.print("\n");
			  for (int i = 0; i < xSize; ++i) {
				  System.out.print("\n");
				    for (int j = 0; j < ySize; ++j){
				    	System.out.print(test[i][j]);
				    }
			  }
	        
	        return pixels;
	    }

	 public static File[] load_data_from_folder() throws IOException {

	        File data = new File("/home/emanon/OutputSortByImage");
	        File[] listOfImages = data.listFiles();

	        for (int y = 0; y < listOfImages.length; y++) {
	        	//Control.championshipList.get(0).TeamsList.add(Tools.read_teams_from_file(listOfFiles[i].getPath()));
	        	System.out.println(listOfImages[y]);
	            
	        }
	        return listOfImages;
	    }
	

	 public static void writeToFile(String message) throws URISyntaxException {
			
			Configuration configuration = new Configuration();
			configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/core-site.xml"));
			configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/hdfs-site.xml"));
			
			try{
			FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
			Path file = new Path("hdfs://localhost:9000/user/emanon/yyyy/Report.txt");
			//Path file1 = new Path("hdfs://localhost:9000/pict/LT51830342011259B04.tif");
			Path file1 = new Path("hdfs://localhost:9000//pict2/LT51830342011259B04.tif");
			if(hdfs.exists(file1)){
				System.out.println(file1.toUri());
				
			}
			
			if ( hdfs.exists( file )) { 
				System.out.println("ok");
				hdfs.delete( file, true ); } 
			//System.out.println("ok");
			OutputStream os = hdfs.create(file); 
			
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
			br.write("hhahahah");
			br.close();
			hdfs.close();
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		     
		   }
	 
	 
	 
	
	public static void main(String[] args) {
		
		try {
			writeToFile("ffff");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		try {
			read_pixels_from_file("/home/emanon/OutputSortByImageold/LT51830342011201");
			//Control.load_data_from_folder();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		/*
		 * Configuration configuration = new Configuration();
		
		configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/core-site.xml"));
		configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/hdfs-site.xml"));
		
		try{
		FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
		Path file = new Path("hdfs://localhost:9000/fireProject/imagesInfo/"+filename+".txt");
		
		if ( hdfs.exists( file )) { 
			//System.out.println("ok");
			hdfs.delete( file, true ); } 
		//System.out.println("ok");
		OutputStream os = hdfs.create(file); 
		
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		
		
		br.write("file:"+filename+"\n");
		br.write("xSize:"+xSize+"\n");
		br.write("ySize:"+ySize+"\n");
		br.write("0:"+adfGeoTransform[0]+":1:"+adfGeoTransform[1]+":2:"+adfGeoTransform[2]+
				":3:"+adfGeoTransform[3]+":4:"+adfGeoTransform[4]+":5:"+adfGeoTransform[5]+"\n");
		if (poDataset.GetProjectionRef() != null)
			br.write("Projection:" + poDataset.GetProjectionRef()+ "\n");
		
		br.close();
		hdfs.close();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 */
		
		
		/*try{
		FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
		Path file = new Path("hdfs://localhost:9000/fireProject/tifToTxt/"+filename+band+"."+xSize+"."+ySize+".txt");
	
	if ( hdfs.exists( file )) { 
		//System.out.println("ok");
		hdfs.delete( file, true ); } 
	//System.out.println("ok");
	OutputStream os = hdfs.create(file); 
	
	BufferedWriter out = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	
	
	out.write(filename+","+band+","+xSize+","+ySize+","+line+","); //grafei stin prwti grami tou arxeiou to onoma,tin bada,xsize,ysize
	for (int i = 0; i <bytes.length; i++) {				  //grafei ola ta bytes(pixels) sto arxeio
		
		if (i % poBand.getYSize() == 0 && i>2) {		  //an allazei grami ka8e ysize wste meta na pairneis san eisodo tis grammes
			line++;
			out.write("\n");							  //System.out.print("\n");
			out.write(filename+","+band+","+xSize+","+ySize+","+line+",");  //grafei se ka8e nea grammi tis info tou arxeiou
		}
		out.write(bytes[i]+",");		    			  //System.out.print(bytes[i]+ ",");  //grafei ta bytes
	}
	out.close();
	hdfs.close();
	}catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/
		
	}
}
