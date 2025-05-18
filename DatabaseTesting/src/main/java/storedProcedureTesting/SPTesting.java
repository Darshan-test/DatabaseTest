package storedProcedureTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;


/*
 * Syntax                                Stores Procedures
 * { call procedure_name() }     Accept no parameter and return no value
 * { call procedure_name(?,?) }     Accept two parameter and return no value
 * {  ?= call procedure_name() }     Accept no parameter and return  value
 * { ?= call procedure_name(?) }     Accept one parameter and return  value
 **/
public class SPTesting {
	
	Connection con=null;
	Statement stmt=null;
	ResultSet rs;
	CallableStatement cstmt;
	ResultSet rs1;
	ResultSet rs2;
	
	@BeforeClass
	void setup() throws SQLException
	{
		con=DriverManager.getConnection("jdbc:mysql://localhost:3307/classicmodels","root","root");
	}
	
	
	@Test(priority=1)
	void test_storedProceduresExists() throws SQLException {
		stmt=con.createStatement();
		rs=stmt.executeQuery("show procedure status where name='selectallcustomers'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"),"selectallcustomers");
		
	}
	@ Test(priority=2)
	void test_SelectAllCustomers() throws  SQLException {
		cstmt=con.prepareCall("{call selectallcustomers()}");
		rs1=cstmt.executeQuery();
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("select * from customers");
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	
	
	
	
	@Test(priority=3)
	void test_SelectAllCustomersByCity() throws SQLException {
		cstmt=con.prepareCall("{call  selectallcustomersbycity(?)}");
		cstmt.setString(1,"Stavern");
		rs1=cstmt.executeQuery();
		
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("Select * from customers where city='Stavern'");
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	
	@Test(priority=4)
	void test_SelectAllCustomersByCityAndPincode() throws SQLException {
		cstmt=con.prepareCall("{call selectcustomerbycityandpincode(?,?)}");
		cstmt.setString(1,"Stavern");
		cstmt.setString(2, "079903");
		rs1=cstmt.executeQuery();
		
		
		Statement stmt=con.createStatement();
		rs2=stmt.executeQuery("Select * from customers where city='Stavern'and postalCode=079903");
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	
	@Test(priority=5)
	void test_get_order_by_cust() throws SQLException {
		cstmt=con.prepareCall("{call  get_odr_by_cust(?,?,?,?,?)}");
		cstmt.setInt(1,141);
		cstmt.registerOutParameter(2,Types.INTEGER);
		cstmt.registerOutParameter(3, Types.INTEGER);
		cstmt.registerOutParameter(4, Types.INTEGER);
		cstmt.registerOutParameter(5, Types.INTEGER);
		
		cstmt.executeQuery();
		
		int shipped=cstmt.getInt(2);
		int canceled=cstmt.getInt(3);
		int resolved=cstmt.getInt(4);
		int disputed=cstmt.getInt(5);
		
		Statement stmt=con.createStatement();
		rs=stmt.executeQuery("Select(Select count(*) as 'shipped' from orders where  customerNumber=141 And status ='Shipped')As Shipped,      (Select count(*) as 'canceled' from orders where  customerNumber=141 And status ='Canceled ')as  Canceled,     (Select count(*) as 'resolved' from orders where  customerNumber=141 And status ='Resolved ')As  Resolved,   (Select count(*) as 'disputed' from orders where  customerNumber=141 And status ='Disputed' )as Disputed ");
	
		rs.next();
		int exp_shipped=rs.getInt("shipped");
		int exp_canceled=rs.getInt("canceled");
		int exp_resolved=rs.getInt("resolved");
		int exp_disputed=rs.getInt("disputed");
		
		
		if(shipped==exp_shipped && canceled==exp_canceled && resolved==exp_resolved && disputed==exp_disputed)
			Assert.assertTrue(true);
		else
			Assert.assertTrue(false);
		//System.out.println(shipped +" "+canceled+" "+resolved+ " "+disputed);
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
		
		
		return true;
		
	}
	@AfterClass
	void teardown() throws SQLException {
		con.close();
	}
}
