import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Calendar;


public class Query {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection con;
		String dbUrl = "jdbc:odbc:Project3";
		Calendar calendar = Calendar.getInstance();
		Long startTime;
		Long endTime;
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			con = DriverManager.getConnection(dbUrl, "ismail", "ismail143");
			java.sql.Statement s = con.createStatement();
			
			String query = "CREATE TABLE Project3.uspci (areaName VARCHAR(50),Income1990 INT(10),Rank1990 VARCHAR(10),Income1991 INT(10),Rank1991 VARCHAR(10),"+
							"Income1992 INT(10),Rank1992 VARCHAR(10),Income1993 INT(10),Rank1993 VARCHAR(10),Income1994 INT(10),Rank1994 VARCHAR(10),Income1995 INT(10),"+ 
							"Rank1995 VARCHAR(10),Income1996 INT(10),Rank1996 VARCHAR(10),Income1997 INT(10),Rank1997 VARCHAR(10),Income1998 INT(10),Rank1998 VARCHAR(10),"+
							"Income1999 INT(10),Rank1999 VARCHAR(10),Income2000 INT(10),Rank2000 VARCHAR(10),Income2001 INT(10),Rank2001 VARCHAR(10),Income2002 INT(10),"+ 
							"Rank2002 VARCHAR(10),Income2003 INT(10),Rank2003 VARCHAR(10),Income2004 INT(10),Rank2004 VARCHAR(10),Income2005 INT(10),Rank2005 VARCHAR(10),"+
							"Income2006 INT(10),Rank2006 VARCHAR(10),Income2007 INT(10),Rank2007 VARCHAR(10),Income2008 INT(10),Rank2008 VARCHAR(10),Income2009 INT(10),"+ 
							"Rank2009 VARCHAR(10),Income2010 INT(10),Rank2010 VARCHAR(10),Income2011 INT(10),Rank2011 VARCHAR(10),Income2012 INT(10),Rank2012 VARCHAR(10));";
			
			s.executeUpdate(query);
			
			System.out.println("Uploading Us-pci data");
			startTime = calendar.getTimeInMillis();
			String path = "D:/books/semester4/Cloud_Computing/Project3- AWS/upload/us-pci-real.csv";
			String upload = " LOAD DATA LOCAL INFILE '" + path +
                    "' INTO TABLE Project3.uspci " +
                    " FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"'" +
                    " LINES TERMINATED BY \'\\n\'";
			s.executeUpdate(upload);
			System.out.println("Uploading Us-pci data done");
			calendar = Calendar.getInstance();
			endTime = calendar.getTimeInMillis();
			System.out.println("Total time taken in miliseconds : "+(endTime-startTime));
			
			query = "CREATE TABLE Project3.hd2013 (UNIVID INT(10),INSTNM VARCHAR(100),ADDR VARCHAR(500),CITY VARCHAR(100),STABBR VARCHAR(10),"+
					"ZIP VARCHAR(20),FIPS INT(5),OBEREG INT(10),CHFNM VARCHAR(200),CHFTITLE VARCHAR(50),GENTELE INT(20),FAXTELE INT(20),"+
					"EIN INT(20),OPEID INT(20),OPEFLAG VARCHAR(3),WEBADDR VARCHAR(200),ADMINURL VARCHAR(200),FAIDURL VARCHAR(200),APPLURL VARCHAR(200)," +
					"NPRICURL VARCHAR(200),SECTOR INT(10),ICLEVEL INT(10),CONTROL INT(10),HLOFFER INT(10),UGOFFER INT(10)," +
					"GROFFER INT(10),HDEGOFR1 INT(10),DEGGRANT INT(10),HBCU INT(10),HOSPITAL INT(10),MEDICAL INT(10),TRIBAL INT(10)," +
					"LOCALE INT(10),OPENPUBL INT(10),ACT VARCHAR(10),NEWID INT(10),DEATHYR INT(10),CLOSEDAT INT(10),CYACTIVE INT(10)," +
					"POSTSEC INT(10),PSEFLAG INT(10),PSET4FLG INT(10),RPTMTH INT(10),IALIAS VARCHAR(100),INSTCAT INT(10),CCBASIC INT(10)," +
					"CCIPUG INT(10),CCIPGRAD INT(10),CCUGPROF INT(10),CCENRPRF INT(10),CCSIZSET INT(10),CARNEGIE INT(10)," +
					"LANDGRNT INT(10),INSTSIZE INT(10),CBSA INT(10),CBSATYPE INT(10),CSA INT(10),NECTA INT(10),F1SYSTYP INT(10)," +
					"F1SYSNAM VARCHAR(500),F1SYSCOD INT(10),COUNCTYCD INT(10),COUNTYNM VARCHAR(500),CNGDSTCD INT(10)," +
					"LONGITUD VARCHAR(100),LATITUDE VARCHAR(100))";
			
			s.executeUpdate(query);
			
			System.out.println();
			System.out.println("Uploading hd 2013 data");
			calendar = Calendar.getInstance();
			startTime = calendar.getTimeInMillis();
			path = "D:/books/semester4/Cloud_Computing/Project3- AWS/upload/hd2013-real.csv";
			upload = " LOAD DATA LOCAL INFILE '" + path +
                    "' INTO TABLE Project3.hd2013 " +
                    " FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"'" +
                    " LINES TERMINATED BY \'\\n\'";
			s.executeUpdate(upload);
			System.out.println("Uploading hd 2013 data done");
			calendar = Calendar.getInstance();
			endTime = calendar.getTimeInMillis();
			System.out.println("Total time taken in miliseconds : "+(endTime-startTime));
			
			query = "select count(1) as Total_no_of_univ,H.STABBR as State_Code,"+
					"(U.Income1990 + U.Income1991 + U.Income1992 + U.Income1993 + U.Income1994 + U.Income1995 "+
					"+ U.Income1996 + U.Income1997 + U.Income1998 + U.Income1999 + U.Income2000 + U.Income2001 "+
					"+ U.Income2002 + U.Income2003 + U.Income2004 + U.Income2005 + U.Income2006 + U.Income2007 "+
					"+ U.Income2008 + U.Income2009 + U.Income2010 + U.Income2011 + U.Income2012) / 23 as Average_Per_Capita_Income "+
					"from Project3.hd2013 H, Project3.uspci U where H.STABBR = U.areaName group by H.STABBR " +
					"order by Average_Per_Capita_Income desc";
			
			System.out.println();
			System.out.println("Fetching state income education relationship data");
			calendar = Calendar.getInstance();
			startTime = calendar.getTimeInMillis();
			ResultSet resultSet = s.executeQuery(query);
			System.out.println("Fetching state income education relationship data done");
			calendar = Calendar.getInstance();
			endTime = calendar.getTimeInMillis();
			System.out.println("Total time taken in miliseconds : "+(endTime-startTime));
            if(resultSet != null)
            {
            	System.out.printf("%-30s %-15s %-10s\n", "Total No of Universities","State Code","Average Capital Income");
            	while(resultSet.next())
            	{
            		int totalNoOfUniv = resultSet.getInt(1);
            		String stateCode = resultSet.getString(2);
            		String averageCapitaIncome = resultSet.getString(3);
            		System.out.printf("%-30d %-15s %-10s\n", totalNoOfUniv,stateCode,averageCapitaIncome);
            	}
            }
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
