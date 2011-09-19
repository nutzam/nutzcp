package org.nutz.cp;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

import lombok.Delegate;
import lombok.Getter;

public class NutJdbcConnection implements Connection {

	public NutJdbcConnection(Connection _conn, NutDataSource _ds) {
		this._conn = _conn;
		this._ds = _ds;
	}
	
	public void close() throws SQLException {
		this._ds._pushConnection(this);
	}
	
	@Delegate(excludes=Closeable.class)
	@Getter
	private Connection _conn;

	private NutDataSource _ds;
	
	protected void finalize() throws Throwable {
		close();
	}
}
