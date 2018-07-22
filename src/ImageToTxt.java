import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Scanner;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

public class ImageToTxt {

	public static void tifToTxt(String inputName,String folderPath,String outputpath) throws IOException, URISyntaxException {

		double[] adfGeoTransform = new double[6];
		String path = (folderPath+"/"+inputName);//String path = ("/home/emanon/landsat5/"+inputName);
		File f = new File(path);
		//int len = inputName.length();
		
		//arxizei i gdal na diavazei to arxeio
		gdal.AllRegister();
		Dataset poDataset = null;
		try {
			poDataset = (Dataset) gdal.Open(f.getAbsolutePath(),gdalconst.GA_ReadOnly);
			if (poDataset == null) {
				System.out.println("The image could not be read.");
			}
		} catch (Exception e) {
			System.err.println("Exception caught.");
			System.err.println(e.getMessage());
			e.printStackTrace();
			// return null;
		}
		/*if (poDataset.GetProjectionRef() != null)
			System.out.println("Projection is `" + poDataset.GetProjectionRef()+ "'");
		*/
		poDataset.GetGeoTransform(adfGeoTransform);
		//System.out.println("Origin = (" + adfGeoTransform[0] + ", "+ adfGeoTransform[3] + ")");
		//System.out.println("Pixel Size = (" + adfGeoTransform[1] + ", "	+ adfGeoTransform[5] + ")");
		//System.out.println("2 = (" + adfGeoTransform[2] + ", "+ adfGeoTransform[4] + ")");
		
		
		Band poBand = null;
		byte[] bytes;
		int buf_type = 0, buf_size = 0;

		poBand = poDataset.GetRasterBand(1);
		
		int xsize = poBand.getXSize();
		int ysize = poBand.getYSize();
		int pixels = xsize * ysize;

		buf_type = poBand.getDataType();
		buf_size = pixels * gdal.GetDataTypeSize(buf_type) / 8;

		 //System.out.println("Band size is: " + poBand.getXSize() + "x"+ poBand.getYSize());
		 //System.out.println("pixel are: " + pixels);
		 //System.out.println("Allocating ByteBuffer of size: " + buf_size);

		ByteBuffer data = ByteBuffer.allocateDirect(buf_size);
		data.order(ByteOrder.nativeOrder());

		int returnVal = 0;
		try {
			returnVal = poBand.ReadRaster_Direct(0, 0, poBand.getXSize(),poBand.getYSize(), xsize, ysize, buf_type, data);
		} catch (Exception ex) {
			System.err.println("Could not read raster data.");
			System.err.println(ex.getMessage() + "returnVal="+returnVal);
			ex.printStackTrace();
		}
		bytes = new byte[pixels];
		data.get(bytes);
		/////////////////////////////edw exoume ta butes mesa ston pinaka bytes
		
		String xSize = (poBand.getXSize()+"");
		String ySize = (poBand.getYSize()+"");
		int len = inputName.length();
		String filename = inputName.substring(0,len-7);
		String band = inputName.substring(len-7, len-4);
		//System.out.println(filename+"..................."+band);
		int line=0;
		
		//grafw se arxeio sto hdfs tou hadoop
		
		String filePath = outputpath+"/imagesInfo/"+filename+".txt";
		File finfo = new File(filePath);
		if(!finfo.exists()) { 
		FileWriter fn = new FileWriter(filePath);
		BufferedWriter bn = new BufferedWriter(fn);
		PrintWriter outn = new PrintWriter(bn);
		
		outn.write("file:"+filename+"\n");
		outn.write("xSize:"+xSize+"\n");
		outn.write("ySize:"+ySize+"\n");
		outn.write("0:"+adfGeoTransform[0]+":1:"+adfGeoTransform[1]+":2:"+adfGeoTransform[2]+
				":3:"+adfGeoTransform[3]+":4:"+adfGeoTransform[4]+":5:"+adfGeoTransform[5]+"\n");
		if (poDataset.GetProjectionRef() != null)
			outn.write("Projection:" + poDataset.GetProjectionRef()+ "\n");
		outn.close();
		}
		
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
			
			e.printStackTrace();
		}
		 * 
		 */
		
		
		////////////////////////////////
		
		
		FileWriter fr = new FileWriter(outputpath+"/output/"+filename+band+"."+xSize+"."+ySize+".txt");
		BufferedWriter br = new BufferedWriter(fr);
		PrintWriter out = new PrintWriter(br);
		
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
			
			e.printStackTrace();
		}*/
		
		/////////////////////////////////
		
		
		
	}

	
	public static void main(String[] args) throws URISyntaxException, IOException {
		
		
		
		
		Scanner keyboard = new Scanner(System.in);
		System.out.println("Parallel Satellite Image Processing Program\n");
		System.out.println("Put all images(bands 3,4,7.tif for each image) and .aux.xml , .tif.hdr files to one folder");
		//System.out.println("The lenght of the image-name must be 19 + .tif(4) = 23");
		System.out.println("Give images-folder path(ex. /home/emanon/rootfolder/input)\n");
		String folderpath = keyboard.nextLine();
		
		System.out.println("Give images-folder OUTPUT path(ex. /home/emanon/rootfolder/output)\n");
		String outputpath = "/home/emanon/output";
		System.out.println(" ");
		
		outputpath = keyboard.nextLine();
		boolean o=new File(outputpath).mkdir();
		o=new File(outputpath+"/imagesInfo").mkdir();
		o=new File(outputpath+"/output").mkdir();
		File data = new File(folderpath);
        File[] listOfFolders = data.listFiles();

        
        
        
        
       for (int y = 0; y < listOfFolders.length; y++) {

            /*System.out.println("Bands:"+ listOfFolders[y]);
            System.out.println("Bands:"+ listOfFolders[y].getPath());
            System.out.println("Bands:"+ listOfFolders[y].getName());*/
        	
        	int len = listOfFolders[y].getName().length();
        	String filex = listOfFolders[y].getName().substring(len-7, len);
        	if(filex.equals("B03.tif")||filex.equals("B04.tif")||filex.equals("B07.tif")){
        	//System.out.println("filename ex:"+ filex);
        	
            //System.out.println("Bands:"+ listOfFolders[y].getParent());
        		
        		System.out.println("Converting->"+ listOfFolders[y].getName());
        		tifToTxt(listOfFolders[y].getName(),listOfFolders[y].getParent(),outputpath);
        		System.out.println(listOfFolders[y].getName()+"-> finished" );
        	}
            
            
        }
		
       System.out.println("Program end");
	}//main ends

}//Class Test ends