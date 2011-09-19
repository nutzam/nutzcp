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

	/**全局锁*/
	private Object lock = new Object();

	private int missConn = 0;

	@Setter(value = AccessLevel.PRIVATE)
	private boolean needInit = true;

	/**
	 * 获取一个连接
	 */
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
						if (!conn.get_conn().isClosed())
							conn.get_conn().close();
					} catch (Throwable e) {}
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
					Class.forName(driverClassName);
				}
				catch (ClassNotFoundException e) {
					throw new SQLException(e);
				}
				conns = new LinkedList<NutJdbcConnection>();
				for (int i = 0; i < size; i++) {
					conns.push(_newConnection());
				}
				needInit = false;
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
		conn.setTransactionIsolation(defaultTransactionIsolation);
	}

	protected boolean _beforeReturn(NutJdbcConnection conn) throws SQLException {
		Connection _conn = conn.get_conn();
		if (validationQuery == null) {
			return _conn.isValid(validationQueryTimeout);
		} else {
			try {
				_conn.createStatement().execute(validationQuery);
			}
			catch (Throwable e) {
				try {
					if (!_conn.isClosed())
						_conn.close();
				} catch (SQLException e2) {
				}
				return false;
			}
			return true;
		}
	}

	/**真正的从JDBC获取一个新连接*/
	protected NutJdbcConnection _newConnection() throws SQLException {
		if (closed)
			throw new SQLException("Datasource is closed!!!");
		Connection _conn = DriverManager.getConnection(url, username, password);
		_beforePush(_conn);
		NutJdbcConnection conn = new NutJdbcConnection(_conn, this);
		return conn;
	}

	/**关闭这个连接池*/
	public void close() {
		if (closed)
			return;
		synchronized (lock) {
			if (closed)
				return;
			for (NutJdbcConnection conn : conns) {
				try {
					conn.get_conn().close();
				}
				catch (SQLException e) {}
			}
			conns.clear();
			closed = true;
		}
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

	/**用户名*/
	private String username;
	/**密码*/
	private String password;
	/**JDBC URL*/
	private String url;
	/**数据库驱动类*/
	private String driverClassName;
	// private boolean overflow;
	/**连接池大小*/
	private int size = 10;
	/**返回Conntion之前需要执行的SQL语句*/
	private String validationQuery;
	/**校验连接是否有效的超时设置,仅当validationQuery为null时有效*/
	private int validationQueryTimeout = 1;
	/**标记这个连接池是否已经关闭*/
	private boolean closed;
	/**默认的事务级别*/
	private int defaultTransactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
	/**默认的AutoCommit设置*/
	private boolean defaultAutoCommit = false;

}
