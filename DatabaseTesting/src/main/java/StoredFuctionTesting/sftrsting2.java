package StoredFuctionTesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class sftrsting2 {
	Connection con=null;
	Statement stmt;
	ResultSet rs;
	ResultSet rs1;
	ResultSet rs2;
	
	@BeforeClass
	void setup() throws SQLException {
		con=DriverManager.getConnection("jdbc:mysql://localhost:3307/classicmodels\",\"root\",\"root");
		
	}
	@Test(priority=1)
	void test_storedfunctionExists() throws SQLException {
		stmt=con.createStatement();
		rs=stmt.executeQuery("Show function status where name='customerLevel'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"),"customerLevel");
	}
	
	void test_customerLevel_with_SQLStatement() throws SQLException
	{
		rs1=con.createStatement().executeQuery("Select customerName,CustomerLevel(creditLimit)from customers");
		rs2=con.createStatement().executeQuery("Select customerName,CASE where creditLimit>5000 then 'PLATINUM' WHEN creditLimit>=10000 AND creditLimit<5000 then 'GOLD' when creditLimit<10000 then 'SILVER' END ascustomerlevel from customers");
		 Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	public boolean compareResultSets(ResultSet resultSet1,ResultSet resultSet2)throws SQLException{
		while(resultSet1.next()) {
			resultSet2.next();
			int count=resultSet1.getMetaData().getColumnCount();
			for(int i=1;i<=count;i++) {
				if(!StringUtils.equals(resultSet1.getString(i),resultSet2.getString(i))) {
					return false;
				}
			}
		
		}
		return false;
	}
	void teardown() throws SQLException {
		con.close();
	}
	
}
