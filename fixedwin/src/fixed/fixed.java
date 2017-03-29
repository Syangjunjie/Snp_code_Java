package fixed;

import java.sql.*;

public class fixed {

	public static void main(String[] args) {
		try {
			// 与mysql建立连接
			String url = "jdbc:mysql://localhost/snps";
			String user = "root";
			String password = "yjunjie";
			Connection conn = DriverManager.getConnection(url, user, password);
			Statement stmt = conn.createStatement();
			Statement stmt1 = conn.createStatement();
			System.out.println("connected success!");

			for (int i = 1; i < 25; i++) {
				int po = 1; // 第po个将要移出窗口的snp
				int pi = 1; // 第pi个将要移进窗口的snp
				int count = 0; // 滑窗中的snp数
				int regionStart = 0;
				int regionEnd = 500;
				String sql = Select.sql(i)[0];// 被执行的sql语句
				String chr = Select.sql(i)[1];// 被执行的染色体名称
				ResultSet rs = stmt.executeQuery(sql);
				rs.absolute(1); // 从第1行数据开始读取（第1个snp既是将要移进窗口，也是将要移出窗口）
				int in = rs.getInt("chromEnd");// 获取第1个（下一个）将移进窗口的snp位置in

				while (!rs.isLast()) {// 直到读取到最后一条数据，跳出循环，运行下一条染色体
					while (in > regionStart && in <= regionEnd)// 检测下一个将移进窗口的snp是已经在滑窗内
					{
						count++; // 在窗口内则计数加1
						pi++;// 下一个
						rs.absolute(pi); // 继续读取下一条数据
						in = rs.getInt("chromEnd"); // 获取下一个将移进窗口的snp位置in
					}
					
					if (count != 0) // 将滑窗内snp数大于零的滑窗区间及其中snp数插入数据表
						stmt1.executeUpdate("insert into slideResult_1 values"
								+ "('" + chr + "',concat('[','" + regionStart
								+ "," + regionEnd + "',']'),'" + count + "')");


				}

			}
			stmt.close();
			conn.close();
			System.out.println("done!");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}

class Select {
	static String[] arr = new String[2];
	// 根据i，选择并返回sql语句和当前执行的chr
	public static String[] sql(int i) {
		switch (i) {
			case 1 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr1)";
				arr[1] = "chr1";
				break;
			case 2 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr2)";
				arr[1] = "chr2";
				break;
			case 3 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr3)";
				arr[1] = "chr3";
				break;
			case 4 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr4)";
				arr[1] = "chr4";
				break;
			case 5 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr5)";
				arr[1] = "chr5";
				break;
			case 6 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr6)";
				arr[1] = "chr6";
				break;
			case 7 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr7)";
				arr[1] = "chr7";
				break;
			case 8 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr8)";
				arr[1] = "chr8";
				break;
			case 9 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr9)";
				arr[1] = "chr9";
				break;
			case 10 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr10)";
				arr[1] = "chr10";
				break;
			case 11 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr11)";
				arr[1] = "chr11";
				break;
			case 12 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr12)";
				arr[1] = "chr12";
				break;
			case 13 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr13)";
				arr[1] = "chr13";
				break;
			case 14 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr14)";
				arr[1] = "chr14";
				break;
			case 15 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr15)";
				arr[1] = "chr15";
				break;
			case 16 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr16)";
				arr[1] = "chr16";
				break;
			case 17 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr17)";
				arr[1] = "chr17";
				break;
			case 18 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr18)";
				arr[1] = "chr18";
				break;
			case 19 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr19)";
				arr[1] = "chr19";
				break;
			case 20 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr20)";
				arr[1] = "chr20";
				break;
			case 21 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr21)";
				arr[1] = "chr21";
				break;
			case 22 :
				arr[0] = "select * from hapmapSnpsCEU partition(chr22)";
				arr[1] = "chr22";
				break;
			case 23 :
				arr[0] = "select * from hapmapSnpsCEU partition(chrX)";
				arr[1] = "chrX";
				break;
			case 24 :
				arr[0] = "select * from hapmapSnpsCEU partition(chrY)";
				arr[1] = "chrY";
				break;
		}
		return arr;
	}
}
