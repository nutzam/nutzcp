package org.nutz.cp;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;

import org.nutz.lang.Lang;

@Data
public class NutDataSource implements DataSource {

	private LinkedList<NutJdbcConnection> conns = new LinkedList<NutJdbcConnection>();

	/** 全局锁 */
	private final ReentrantLock lock = new ReentrantLock();

	private int missConn = 0;

	@Setter(value = AccessLevel.PRIVATE)
	private boolean needInit = true;

	/**
	 * 获取一个连接
	 */
	public Connection getConnection() throws SQLException {
		if (needInit)
			_init();
		lock.lock();
		try {
			NutJdbcConnection conn = null;
			if (conns.isEmpty()) {
				if (missConn < 1)
					throw new SQLException("Too many connection!!");
				conn = _newConnection();
				missConn--;
				return conn;
			}
			conn = conns.poll();
			if (_beforeReturn(conn))
				return conn;
			else {
				try {
					return _newConnection();
				}
				catch (Throwable e) {
					missConn++;
					lock.unlock();
					throw Lang.wrapThrow(e, SQLException.class);
				}
			}
		}
		finally {
			lock.unlock();
		}
	}

	protected void _init() throws SQLException {
		lock.lock();
		try {
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
		finally {
			lock.unlock();
		}
	}

	protected void _pushConnection(NutJdbcConnection conn) throws SQLException {
		lock.lock();
		try {
			if (conns.size() < size)
				conns.push(conn);
			else
				_TrueClose(conn.get_conn());
		}
		finally {
			lock.unlock();
		}
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
				}
				catch (SQLException e2) {}
				return false;
			}
			return true;
		}
	}

	/** 真正的从JDBC获取一个新连接 */
	protected NutJdbcConnection _newConnection() throws SQLException {
		if (closed)
			throw new SQLException("Datasource is closed!!!");
		Connection _conn = null;
		try {
			_conn = DriverManager.getConnection(url, username, password);
			_beforePush(_conn);
			NutJdbcConnection conn = new NutJdbcConnection(_conn, this);
			return conn;
		}
		catch (Throwable e) {
			_TrueClose(_conn);
			throw Lang.wrapThrow(e, SQLException.class);
		}
	}

	/** 关闭这个连接池 */
	public void close() {
		if (closed)
			return;
		lock.lock();
		try {
			if (closed)
				return;
			for (NutJdbcConnection conn : conns) {
				_TrueClose(conn.get_conn());
			}
			conns.clear();
			closed = true;
		}
		finally {
			lock.unlock();
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

	/** 用户名 */
	private String username;
	/** 密码 */
	private String password;
	/** JDBC URL */
	private String url;
	/** 数据库驱动类 */
	private String driverClassName;
	// private boolean overflow;
	/** 连接池大小 */
	private int size = 10;
	/** 返回Conntion之前需要执行的SQL语句 */
	private String validationQuery;
	/** 校验连接是否有效的超时设置,仅当validationQuery为null时有效 */
	private int validationQueryTimeout = 1;
	/** 标记这个连接池是否已经关闭 */
	private boolean closed;
	/** 默认的事务级别 */
	private int defaultTransactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
	/** 默认的AutoCommit设置 */
	private boolean defaultAutoCommit = false;

	protected static void _TrueClose(Connection _conn) {
		try {
			if (_conn != null)
				if (!_conn.isClosed())
					_conn.close();
		}
		catch (Throwable e) {}
	}
}
