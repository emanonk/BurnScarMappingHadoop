import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import javax.swing.JTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TableTest {

	/**
	 * @param args
	 */
	
	public static final byte[] intToByteArray(int value) {
	    return new byte[] {
	            (byte)(value >>> 256),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	public static void main(String[] args) {
		
		
		/*byte[] g = intToByteArray(240);
		
		for(int i =0;i<4;i++){
		      System.out.println(g[i]+"");
			}*/
		/*Scanner keyboard = new Scanner(System.in);
		System.out.println("Table testing..\n");
		System.out.println("Rows?\n");
		String strt1 = keyboard.nextLine();
		int t1 = Integer.parseInt(strt1);
		System.out.println("Columns?\n");
		String strt2 = keyboard.nextLine();
		int t2 = Integer.parseInt(strt2);

		int[][] intArray = new int[t1][t2];

		for (int x = 0; x < t1; x++) {
			for (int y = 0; y < t2; y++) {
				intArray[x][y] = 0;
			}
		}

		for (int x = 0; x < t1; x++) {
			System.out.println("\n");
			for (int y = 0; y < t2; y++) {
				System.out.print(intArray[x][y]);
			}
		}

		for (int a = 0; a < 2; a++) {

			System.out.println("Table testing..\n");
			System.out.println("Rows?\n");
			strt1 = keyboard.nextLine();
			t1 = Integer.parseInt(strt1);
			int[] intArraya = new int[t1];
			System.out.print(intArraya.length);
			for (int x = 0; x < t1; x++) {

				intArraya[x] = 0;

			}
			
			for (int x = 0; x < t1; x++) {

				System.out.print(intArraya[x]);
			}

		}*/
		
		
		
		byte[] a = new byte[] { (byte)(0xFF & 255), 0x53, 0x1c,
			    (byte)0x87, (byte)0xa0, 0x42, (byte)0x69, 0x12, (byte)0xa2, (byte)0xea, 0x08,
			    0x00, 0x2b, 0x30, 0x30, (byte)0x9d };
		for(int i =0;i<15;i++){
	      System.out.println(a[i]+"");
		}
	}
}

/*public static void load_data_from_folder() throws IOException {

// System.out.println("START");
Configuration conf = new Configuration();

conf.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/core-site.xml"));
conf.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/hdfs-site.xml"));

FileSystem fs = FileSystem.get(conf);

try {
	Path pt = new Path("/pict/testtext.txt");
	//Path pt=new Path("hdfs://localhost:9000/pict/testtext.txt");

	if (!fs.exists(pt))
		System.out.println("Input file not found");
	if (!fs.isFile(pt))
		System.out.println("Input should be a file");

	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
	String line;

	line = br.readLine();
	//System.out.println(line);
	while (line != null) {
		System.out.println(line);
		line = br.readLine();
	}
} catch (Exception e) {
}







}




/*public static RasterBytesSize openFile(String filename) throws IOException {



File f= new File("hdfs://localhost:9000/pict2/LT51830342011259B03.tif");


System.out.println("gdal starts..");
/*
gdal.AllRegister();
Dataset poDataset = null;

try {
	poDataset = (Dataset) gdal.Open(f.getAbsolutePath(),gdalconst.GA_ReadOnly);//getAbsolutePath()
	if (poDataset == null) {
		System.out.println("The image could not be read.");
		printLastError();
		//return null;
	}
} catch(Exception e) {
	System.err.println("Exception caught.");
	System.err.println(e.getMessage());
	e.printStackTrace();
	//return null;
}

//System.out.println("Driver: " + poDataset.GetDriver().GetDescription());
//System.out.println("Size is: " + poDataset.getRasterXSize() + "x"+ poDataset.getRasterYSize() + "  bands:"+ poDataset.getRasterCount());

Band poBand = null;
byte[] bytes;
int xsize = 1024;
int ysize = 1024;
int pixels = xsize * ysize;
int buf_type = 0, buf_size = 0;


poBand = poDataset.GetRasterBand(1);
	
buf_type = poBand.getDataType();
buf_size = pixels * gdal.GetDataTypeSize(buf_type) / 8;

//System.out.println("Band size is: " + poBand.getXSize() + "x"+ poBand.getYSize());
//System.out.println("Allocating ByteBuffer of size: " + buf_size);

ByteBuffer data = ByteBuffer.allocateDirect(buf_size);
data.order(ByteOrder.nativeOrder());

int returnVal = 0;
	try {
		returnVal = poBand.ReadRaster_Direct(0, 0, poBand.getXSize(), poBand.getYSize(), xsize, ysize,buf_type, data);
	} catch(Exception ex) {
		System.err.println("Could not read raster data.");
		System.err.println(ex.getMessage());
		ex.printStackTrace();
	}
		bytes = new byte[pixels];
		data.get(bytes);
		for(int j=85000;j<95110;j++){
			System.out.print(bytes[j]+ "");
		}
RasterBytesSize mc= new RasterBytesSize(bytes,poBand.getYSize(),poBand.getXSize());
byte[] bytes=new byte[10];
RasterBytesSize mc= new RasterBytesSize(bytes,0,0);
		return mc;
		//return bytes;
}*/

/*
 
	
	 public static void writeToFile(String message) throws URISyntaxException {
	
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/core-site.xml"));
		configuration.addResource(new Path("/home/emanon/hadoop-1.1.2/conf/hdfs-site.xml"));
		
		try{
		FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
		Path file = new Path("hdfs://localhost:9000/user/emanon/Report.txt");
		Path file1 = new Path("hdfs://localhost:9000/pict/LT51830342011259B04.tif");
		//Path file1 = new Path("hdfs://localhost:9000//pict2/LT51830342011259B04.tif");
		if(hdfs.exists(file1)){
			System.out.println(file1.toUri());
			
		}
		
		if ( hdfs.exists( file )) { 
			System.out.println("ok");
			hdfs.delete( file, true ); } 
		//System.out.println("ok");
		OutputStream os = hdfs.create(file); 
		
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write(file1.getName());
		br.close();
		hdfs.close();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     
	   }
 */

