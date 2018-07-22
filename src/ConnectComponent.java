import java.io.IOException;

public class ConnectComponent {

	
	public static int[][] ConComp (int row_count,int col_count,int[][] m){
		
	    int label[][] = new int[row_count][col_count];
		
	    for(int x=0;x<row_count;x++){
        	for(int y=0;y<col_count;y++){
        		label[x][y]=0;
            }
        }
	    
	    int component = 0;
		  for (int i = 0; i < row_count; ++i) 
		    for (int j = 0; j < col_count; ++j) 
		      if (label[i][j]==0 && m[i][j]==1) ConnectComponent.dfs(i, j, ++component,m,label,row_count,col_count);
		
		  System.out.print(component+"\n");
		  
		  int[] flags = new int[component+1];
		  
		  for (int c=0; c<=component; c++){
			  flags[c]= 0;
		  }
		  

		  for (int i = 0; i < row_count; i++) {
			  for (int j = 0; j < col_count; j++){
				  if(label[i][j]!=0){
				  flags[label[i][j]] = flags[label[i][j]] + 1;
			  
				  }
			  }
		  }
		  
		// an einai oi geitonies mikroteres apo 10 tote kane tis 0  
		 for (int c=0; c<=component; c++){
			 if(flags[c]<3){
				 flags[c]=0;
			 }
		 }
		 for (int i = 0; i < row_count; i++) {
			  for (int j = 0; j < col_count; j++){
				  
					  if(flags[label[i][j]]<1){
						  label[i][j]=0;
					  }else{
						  label[i][j]=9;
					  }
				  
				  
			  }
		  }
		 
		  /*
		  for (int i = 0; i < row_count; ++i) {
			  System.out.print("\n");
			    for (int j = 0; j < col_count; ++j){
			    	System.out.print(m[i][j]);
			    }
		  }
		  System.out.print("\n");System.out.print("\n");System.out.print("\n");
		  for (int i = 0; i < row_count; ++i) {
			  System.out.print("\n");
			    for (int j = 0; j < col_count; ++j){
			    	System.out.print(label[i][j]);
			    }
		  }*/
		  return label;
	}
	
	
	
	
    public static void dfs(int x, int y, int current_label,int[][] m,int[][] label,int row_count,int col_count) {
    	int dx[] = {+1, 0, -1, 0};
	    int dy[] = {0, +1, 0, -1};  
    	if (x < 0 || x == row_count) return; // out of bounds
    	if (y < 0 || y == col_count) return; // out of bounds
    	if (label[x][y]!=0 || m[x][y]!=1) return; // already labeled or not marked with 1 in m

    	  // mark the current cell
    	  label[x][y] = current_label;

    	  // recursively mark the neighbors
    	  for (int direction = 0; direction < 4; ++direction)
    	    dfs(x + dx[direction], y + dy[direction], current_label,m,label,row_count,col_count);
    	}
    /*public static void main(String[] args) throws IOException {
    	
    int[][] pixels = new int[][]{
    			  { 1, 1, 1, 0, 0, 0, 0, 0, 1, 0 },
    			  { 0, 0, 1, 0, 1, 0, 0, 0, 1, 1 },
    			  { 0, 1, 1, 0, 0, 1, 1, 0, 0, 0 },
    			  { 0, 0, 0, 0, 1, 0, 0, 0, 0, 0 },
    			  { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    			  { 0, 0, 1, 0, 1, 0, 0, 0, 0, 0 },
    			  { 0, 0, 1, 0, 1, 0, 0, 1, 1, 1 },
    			  { 0, 0, 1, 1, 0, 0, 0, 1, 1, 1 },
    			  { 0, 0, 0, 0, 0, 0, 0, 1, 1, 1 },
    			  { 0, 0, 0, 0, 0, 0, 0, 1, 1, 1 }
    			};
    			
    			int[][] result =ConnectComponent.ConComp(10, 10, pixels);
		// Control.load_data_from_folder();

		for(int i=0;i<10;i++){
			System.out.print("\n");
			for(int y=0;y<10;y++){
				System.out.print(result[i][y]);
			}
		}
    	*/
    	
    /*	int[][] pixels = new int[][]{
  			  { 1, 1, 1, 1, 0 },
  			  { 0, 0, 0, 1, 1 },
  			  { 1, 1, 0, 0, 0 },
  			  { 0, 0, 0, 0, 1 },
  			  { 0, 0, 1, 1, 1 }
  			};
    	int[][] result =ConnectComponent.ConComp(5, 5, pixels);
		// Control.load_data_from_folder();

		for(int i=0;i<5;i++){
			System.out.print("\n");
			for(int y=0;y<5;y++){
				System.out.print(result[i][y]);
			}*/
    
    
		//}
    	
		
		
		
//	}

}
