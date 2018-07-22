import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;

public class TTI {

	public static void tifToTxt(String inputName,String outputName) throws IOException {

		double[] adfGeoTransform = new double[6];
		String path = ("/home/emanon/landsat5/"+inputName+".tif");
		File f = new File(path);
		int len = path.length();
		gdal.AllRegister();
		Dataset poDataset = null;

		try {
			poDataset = (Dataset) gdal.Open(f.getAbsolutePath(),gdalconst.GA_ReadOnly);
			if (poDataset == null) {
				System.out.println("The image could not be read.");
				// printLastError();
				// return null;
			}
		} catch (Exception e) {
			System.err.println("Exception caught.");
			System.err.println(e.getMessage());
			e.printStackTrace();
			// return null;
		}
		
		poDataset.GetGeoTransform(adfGeoTransform);
		
		Band poBand = null;
		byte[] bytes;


		poBand = poDataset.GetRasterBand(1);
		
		int xsize = poBand.getXSize();
		int ysize = poBand.getYSize();
		int pixels = xsize * ysize;
/*
		buf_type = poBand.getDataType();
		buf_size = pixels * gdal.GetDataTypeSize(buf_type) / 8;

		 System.out.println("Band size is: " + poBand.getXSize() + "x"+ poBand.getYSize());
		 System.out.println("pixel are: " + pixels);
		 System.out.println("Allocating ByteBuffer of size: " + buf_size);

		ByteBuffer data = ByteBuffer.allocateDirect(buf_size);
		data.order(ByteOrder.nativeOrder());

		int returnVal = 0;
		try {
			poBand.ReadRaster_Direct(0, 0, poBand.getXSize(),poBand.getYSize(), xsize, ysize, buf_type, data);
		} catch (Exception ex) {
			System.err.println("Could not read raster data.");
			System.err.println(ex.getMessage());
			ex.printStackTrace();
		}*/
		bytes = new byte[pixels];
		//data.get(bytes);
		
		
		//edw kanw mia dokimi gia na dw an douleuei i foto
		for(int i=0;i<pixels;i++){
			if(i<pixels/2){
			bytes[i]=(byte)0;
			//System.out.println(bytes[i]);
			}else{
				bytes[i]=(byte)0x69;
			}
		}
		
		
		
		Dataset dataset = null;
        Driver driver = null;
        Band banda = null;
		String patha = ("/home/emanon/rootfolder/test/200.tif");
        driver = gdal.GetDriverByName("GTiff");
        dataset = driver.Create(patha, poBand.getXSize(), poBand.getYSize(), 1, gdalconst.GDT_Byte);
        //System.out.println(gdalconst.GDT_Byte);
        banda = dataset.GetRasterBand(1);
        dataset.SetGeoTransform(adfGeoTransform);
        
        String a = poDataset.GetProjectionRef();
        dataset.SetProjection(a);
        banda.WriteRaster(0,0,poBand.getXSize(),poBand.getYSize(),bytes);
        
		
		
		
	}

	public static void main(String[] args) {
		
		try {
			for(int i=0; i<1; i++){
				tifToTxt("LT51830342011259B04","20"+i);
			}
			/*for(int i=0; i<1; i++){
				tifToTxt("LT51830342011259B04","20"+i);
			}
			for(int i=0; i<1; i++){
				tifToTxt("LT51830342011259B07","20"+i);
			}*/
			
			
		} catch (IOException e) {e.printStackTrace();}//try ends
		
	}//main ends

}//Class Test ends