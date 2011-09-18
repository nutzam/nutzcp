package org.nutz.cp;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

import javax.sql.DataSource;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

@Data
public class NutDataSource implements DataSource {

	private LinkedList<NutJdbcConnection> conns = new LinkedList<NutJdbcConnection>();

	private Object lock = new Object();

	private int missConn = 0;

	@Setter(value = AccessLevel.PROTECTED)
	private boolean needInit = true;

	public Connection getConnection() throws SQLException {
		if (needInit)
			_init();
		NutJdbcConnection conn = null;
		synchronized (lock) {
			if (missConn > 0) {
				int needCreate = missConn;
				for (int i = 0; i < needCreate; i++) {
					conns.push(_newConnection());
					missConn--;
				}
			}
			while (!conns.isEmpty()) {
				conn = conns.poll();
				if (_beforeReturn(conn))
					return conn;
				else {
					boolean ok = false;
					try {
						conns.push(_newConnection());
						ok = true;
					}
					finally {
						if (!ok)
							missConn++;
					}
				}
			}
			if (conn == null) {
				// if (overflow) {
				// throw new UnsupportedOperationException("Not support yet");
				// } else {
				throw new SQLException("Too many connection!!");
				// }
			}
		}
		return conn;
	}

	protected void _init() throws SQLException {
		synchronized (lock) {
			if (needInit) {
				try {
					Class.forName(driver);
				}
				catch (ClassNotFoundException e) {
					throw new SQLException(e);
				}
				conns = new LinkedList<NutJdbcConnection>();
				for (int i = 0; i < size; i++) {
					conns.push(_newConnection());
				}
			}
		}
	}

	protected void _pushConnection(NutJdbcConnection _conn) throws SQLException {
		if (conns.size() < size) {
			synchronized (lock) {
				if (conns.size() < size) {
					conns.push(_conn);
					return;
				}
			}
		}
		_conn.get_conn().close();
	}

	protected void _beforePush(Connection conn) throws SQLException {
		conn.setAutoCommit(defaultAutoCommit);
		conn.setTransactionIsolation(defaultTransaction);
	}

	protected boolean _beforeReturn(NutJdbcConnection conn) throws SQLException {
		Connection _conn = conn.get_conn();
		if (vaildSql == null) {
			return _conn.isValid(5);
		} else {
			try {
				_conn.createStatement().execute(vaildSql);
			}
			catch (Throwable e) {
				return false;
			}
			return true;
		}
	}

	protected NutJdbcConnection _newConnection() throws SQLException {
		if (closed)
			throw new SQLException("Datasource is closed!!!");
		Connection _conn = DriverManager.getConnection(jdbcUrl, user, password);
		_beforePush(_conn);
		NutJdbcConnection conn = new NutJdbcConnection(_conn, this);
		return conn;
	}

	public void close() {
		if (closed)
			return;
		synchronized (lock) {
			for (NutJdbcConnection conn : conns) {
				try {
					conn.get_conn().close();
				}
				catch (SQLException e) {}
			}
		}
		closed = true;
	}

	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	public PrintWriter getLogWriter() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public int getLoginTimeout() throws SQLException {
		throw new UnsupportedOperationException();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public Connection getConnection(String username, String password) throws SQLException {
		throw new UnsupportedOperationException();
	}

	private String user;
	private String password;
	private String jdbcUrl;
	private String driver;
	// private boolean overflow;
	private int size = 50;
	private String vaildSql;
	private boolean closed;
	private int defaultTransaction = Connection.TRANSACTION_READ_COMMITTED;
	private boolean defaultAutoCommit = false;

}
