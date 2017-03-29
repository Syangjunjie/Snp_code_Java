package fixedTest;
import java.sql.*;  
 
public class Main {       
        static String url = "jdbc:mysql://localhost/snps";         
        static String user = "root";                     
        static String password = "yjunjie";  
        
        public static void main(String[] args){ 
        	Connection conn = null;
        	Statement stmt = null;
        	Statement stmt1 = null;
                try { 
                  System.out.println("Connecting to the Database...");
                  conn = DriverManager.getConnection(url, user, password);                    
                  System.out.println("Creating statement...");
                  stmt = conn.createStatement();  
                  stmt1 = conn.createStatement(); 
                  String sql = "";
                  for(int i=1;i<26;i++){
                	  int count = 0;
                	  int regionStart = 0;
                   	  int regionEnd = 500;
                   	  String chr = " ";
                	  //System.out.println(" ");
                	  switch(i){
                	  case 1:
                		  sql = "select * from hapmapSnpsCEU partition(chr1)";
                		  chr =  "1";
                		  break;
                	  case 2:
                		  sql = "select * from hapmapSnpsCEU partition(chr2)";
                		  chr =  "2";
                		  break;
                	  case 3:
                		  sql = "select * from hapmapSnpsCEU partition(chr3)";
                		  chr =  "3";
                		  break;
                	  case 4:
                		  sql = "select * from hapmapSnpsCEU partition(chr4)";
                		  chr =  "4";
                		  break;
                	  case 5:
                		  sql = "select * from hapmapSnpsCEU partition(chr5)";
                		  chr = "5";
                		  break;
                	  case 6:
                		  sql = "select * from hapmapSnpsCEU partition(chr6)";
                		  chr =  "6";
                		  break;
                	  case 7:
                		  sql = "select * from hapmapSnpsCEU partition(chr7)";
                		  chr =  "7";
                		  break;
                	  case 8:
                		  sql = "select * from hapmapSnpsCEU partition(chr8)";
                		  chr =  "8";
                		  break;
                	  case 9:
                		  sql = "select * from hapmapSnpsCEU partition(chr9)";
                		  chr =  "9";
                		  break;
                	  case 10:
                		  sql = "select * from hapmapSnpsCEU partition(chr10)";
                		  chr =  "10";
                		  break;
                	  case 11:
                		  sql = "select * from hapmapSnpsCEU partition(chr11)";
                		  chr =  "11";
                		  break;
                	  case 12:
                		  sql = "select * from hapmapSnpsCEU partition(chr12)";
                		  chr =  "12";
                		  break;
                	  case 13:
                		  sql = "select * from hapmapSnpsCEU partition(chr13)";
                		  chr =  "13";
                		  break;
                	  case 14:
                		  sql = "select * from hapmapSnpsCEU partition(chr14)";
                		  chr = "14";
                		  break;
                	  case 15:
                		  sql = "select * from hapmapSnpsCEU partition(chr15)";
                		  chr = "15";
                		  break;
                	  case 16:
                		  sql = "select * from hapmapSnpsCEU partition(chr16)";
                		  chr = "16";
                		  break;
                	  case 17:
                		  sql = "select * from hapmapSnpsCEU partition(chr17)";
                		  chr = "17";
                		  break;
                	  case 18:
                		  sql = "select * from hapmapSnpsCEU partition(chr18)";
                		  chr = "18";
                		  break;
                	  case 19:
                		  sql = "select * from hapmapSnpsCEU partition(chr19)";
                		  chr = "19";
                		  break;
                	  case 20:
                		  sql = "select * from hapmapSnpsCEU partition(chr20)";
                		  chr = "20";
                		  break;
                	  case 21:
                		  sql = "select * from hapmapSnpsCEU partition(chr21)";
                		  chr = "21";
                		  break;
                	  case 22:
                		  sql = "select * from hapmapSnpsCEU partition(chr22)";
                		  chr = "22";
                		  break;
                	  case 23:
                		  sql = "select * from hapmapSnpsCEU partition(chrM)";
                		  chr = "M";
                		  break;
                	  case 24:
                		  sql = "select * from hapmapSnpsCEU partition(chrX)";
                		  chr = "X";
                		  break;
                	  case 25:
                		  sql = "select * from hapmapSnpsCEU partition(chrY)";
                		  chr = "Y";
                		  break;
                		  
                	  }            	  
                	  ResultSet rs = stmt.executeQuery(sql);                	
                	  while(rs.next()){
                		  boolean c =true;
                		  int p = rs.getInt("chromStart");
                		  while(c){                			 
                			  if(p>=regionStart&&p<=regionEnd-1){
                				  count++;                				  
                				  c = false;
                			  }	
                			  else{
                				  if(count!=0){
                					  stmt1.executeUpdate("insert into fixedResult values(concat('chr','"+chr+"'),concat('[','"+regionStart+","+regionEnd+"',']'),'"+count+"')");                					  
                					  //System.out.print("chr"+chr+"["+regionStart+","+regionEnd+"]:"+count+"  ");
                					  count = 0;
                				  }                				  
                				  regionStart +=1;
                				  regionEnd +=1;                				  
                			  }
                		  }
                	  }
                	  rs.close();	  
                  }
                  stmt.close();
                  conn.close();
                  System.out.println("done!");
                 }
                catch(Exception e) { 
                	System.out.println(e.getMessage());
                }
        }
}
                	                
