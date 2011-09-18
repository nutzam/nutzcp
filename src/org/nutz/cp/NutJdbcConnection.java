package org.nutz.cp;

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
	
	@Delegate(excludes=closeAble.class)
	@Getter
	private Connection _conn;

	private NutDataSource _ds;

	private interface closeAble {
		void close();
	}
	
	protected void finalize() throws Throwable {
		if (!_conn.isClosed())
			_conn.close();
		super.finalize();
	}
}
