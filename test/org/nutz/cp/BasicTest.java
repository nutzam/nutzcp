package org.nutz.cp;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import static org.junit.Assert.*;

import org.junit.Test;
import org.nutz.ioc.impl.PropertiesProxy;
import org.nutz.lang.Stopwatch;

public class BasicTest {

	@Test
	public void test_get() throws Throwable {
		Connection conn = getDataSouce().getConnection();
		assertTrue(conn.isValid(1));
	}
	
	@Test(expected=SQLException.class)
	public void test_getMore() throws Throwable {
		for (int i = 0; i < 10; i++) {
			try {
				getDataSouce().getConnection();
			}
			catch (SQLException e) {
			}
		}
		getDataSouce().getConnection();//超过限额,报异常
	}
	
	@Test
	public void test_get_get_get() throws Throwable {
		NutDataSource ds = getDataSouce();
		for (int i = 0; i < 10000; i++) {
			ds.getConnection().close();
		}
		Stopwatch sw = Stopwatch.begin();
		for (int i = 0; i < 100000; i++) {
			ds.getConnection().close();
		}
		sw.stop();
		System.out.println(sw);
	}
	
	private static NutDataSource ds;
	public static final NutDataSource getDataSouce(){
		if (ds != null)
			return ds;
		PropertiesProxy pp = new PropertiesProxy(true);
		pp.setPaths("nutz-test.properties");
		ds = new NutDataSource();
		ds.setDriverClass(pp.get("driver"));
		ds.setUsername(pp.get("username"));
		ds.setPassword(pp.get("password"));
		ds.setJdbcUrl(pp.get("url"));
		ds.setSize(10);
		return ds;
	}
}
