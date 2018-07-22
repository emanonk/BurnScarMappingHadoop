import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.gdal.gdal.gdal;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdalconst.gdalconst;

public class TxtToImage {

	public static void TxtToTif(String fileName) throws IOException {

		String line,filename;
		StringTokenizer tokenizer;
		BufferedReader buff = null;
		
		double[] adfGeoTransform = new double[6];

		
		//pairnw apo to info arxeio tis info pou xreiazomai
		try {
			buff = new BufferedReader(new FileReader("/home/emanon/rootfolder/imagesInfo/" + fileName+".txt"));
		} catch (IOException ex) {
			System.err.println("Could not read " + fileName);
		} catch (Exception ex) {
			System.err.println("Error occurred");
			System.err.println(ex.getMessage());
		}

		line = buff.readLine();
		String[] dalla = line.toString().split(":");
		String imageName = dalla[1];

		//anoigw tin tif gia na parw etoima ta stoixia kai ta bytes
		
		//String path = ("/home/emanon/rootfolder/ProgramInputTif/"+imageName+"B03.tif");
		String path = ("/home/emanon/landsat5/"+imageName+"B07.tif");
		File f = new File(path);
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
		}
		
		poDataset.GetGeoTransform(adfGeoTransform);
		Band poBand = null;
		poBand = poDataset.GetRasterBand(1);
		
		int xsize = poBand.getXSize();
		int ysize = poBand.getYSize();
		int pixels = xsize * ysize;

		
		
		Dataset dataset = null;
        Driver driver = null;
        Band band = null;
        
  
        driver = gdal.GetDriverByName("GTiff");
        float[] floatArray = new float[pixels];
        float[][] fArray = new float[xsize][ysize];
        String patha = ("/home/emanon/rootfolder/TxtToTif/2121.tif");
        dataset = driver.Create(patha, xsize, ysize, 1, gdalconst.GDT_Float32);
        band = dataset.GetRasterBand(1);
        
        dataset.SetGeoTransform(adfGeoTransform);
        String a = poDataset.GetProjectionRef();
        dataset.SetProjection(a);
        
       
        
        //band.SetStatistics(0.0, 245.0, 0.0, 0.0);
       
		//..........................................................................................
		//pairnw apo to txt arxeio ta kamena pixel
		try {
			buff = new BufferedReader(new FileReader("/home/emanon/rootfolder/SortByImageOutput/"+fileName));
		} catch (IOException ex) {
			System.err.println("Could not read " + fileName);
		} catch (Exception ex) {
			System.err.println("Error occurred");
			System.err.println(ex.getMessage());
		}
		if (buff != null) {
			while ((line = buff.readLine()) != null) {
				tokenizer = new StringTokenizer(line, "\t:");
				filename = tokenizer.nextToken();
				String xIndicator = tokenizer.nextToken();
				String yIndicator = tokenizer.nextToken();
				int xI = Integer.parseInt(xIndicator);
				int yI = Integer.parseInt(yIndicator);
				fArray[xI][yI] = (float)245;
				//bytes[(xI*ysize)+yI]=(byte)0x69;
			}
			buff.close();
		}
		
        /*for( int i = 0; i < ysize; i++) {
            for( int j = 0; j < xsize; j++) {
                fArray[j][i] = (float)0;
            }
        }*/
        
        /*
		
		for( int i = 0; i < ysize; i++) {
            for( int j = 0; j < xsize; j++) {
            	if(fArray[j][i]==(float)245){
            		floatArray[j] = (float)fArray[j][i];
            	}else{
            		floatArray[j] = (float)0;
            	}
                
            }
            band.WriteRaster(0, i, xsize, 1, floatArray);
        }
		
		*/
		
		byte[] bArray = new byte[pixels];
        
		
		
		int k=0;
		for( int i = 0; i < xsize; i++) {
            for( int j = 0; j < ysize; j++) {
            	
            	if(fArray[i][j]==(float)245){
            		//floatArray[k] = (float)fArray[i][j];
            		bArray[k] = (byte)0;
            		
            	}else{
            		//floatArray[k] = (float)0;
            		bArray[k] = (byte)(byte)0x69;
            	}
            	
                //floatArray[k] = (float)fArray[i][j];
               //if(fArray[i][j]!=0){System.out.println("i:"+i+".j:"+j+".val:"+floatArray[k]);}
                k++;
            }
            
        }
		//band.WriteRaster(0, 0, xsize, ysize, floatArray);
		//dataset.delete();
		poDataset.delete();
		
        
        
        
        
        Dataset bdataset = null;
        Driver bdriver = null;
        Band bband = null;
        
  
        bdriver = gdal.GetDriverByName("GTiff");
        
        String bpath = ("/home/emanon/rootfolder/TxtToTif/2122.tif");
        bdataset = bdriver.Create(bpath, xsize, ysize, 1, gdalconst.GDT_Byte);
        bband = bdataset.GetRasterBand(1);
        
        bdataset.SetGeoTransform(adfGeoTransform);
        
        bdataset.SetProjection(a);
        bband.WriteRaster(0, 0, xsize, ysize, bArray);
        bdataset.delete();
        System.out.println("finised");
	}

	public static void main(String[] args) throws IOException {
		TxtToTif("LT51830342011259");		
	}

}
