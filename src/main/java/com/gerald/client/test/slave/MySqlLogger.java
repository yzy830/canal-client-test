package com.gerald.client.test.slave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.log.Log;

public class MySqlLogger implements Log {
	
	protected Logger logger = null;
	
	public MySqlLogger(String name) {
		logger = LoggerFactory.getLogger(name);
	}

	@Override
	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}

	@Override
	public boolean isErrorEnabled() {
		return logger.isErrorEnabled();
	}

	@Override
	public boolean isFatalEnabled() {
		return logger.isErrorEnabled();
	}

	@Override
	public boolean isInfoEnabled() {
		return logger.isInfoEnabled();
	}

	@Override
	public boolean isTraceEnabled() {
		return logger.isTraceEnabled();
	}

	@Override
	public boolean isWarnEnabled() {
	    return logger.isWarnEnabled();
	}

	@Override
	public void logDebug(Object msg) {
		logger.debug(msg.toString());
	}

	@Override
	public void logDebug(Object msg, Throwable thrown) {
		logger.debug(msg.toString(), thrown);
	}

	@Override
	public void logError(Object msg) {
		logger.error(msg.toString());
	}

	@Override
	public void logError(Object msg, Throwable thrown) {
		logger.error(msg.toString(), thrown);
	}

	@Override
	public void logFatal(Object msg) {
		logger.error(msg.toString());
	}

	@Override
	public void logFatal(Object msg, Throwable thrown) {
		logger.error(msg.toString(), thrown);
	}

	@Override
	public void logInfo(Object msg) {
		logger.info(msg.toString());
	}

	@Override
	public void logInfo(Object msg, Throwable thrown) {
		logger.info(msg.toString(), thrown);
	}

	@Override
	public void logTrace(Object msg) {
		logger.trace(msg.toString());
	}

	@Override
	public void logTrace(Object msg, Throwable thrown) {
		logger.trace(msg.toString(), thrown);
	}

	@Override
	public void logWarn(Object msg) {
		logger.warn(msg.toString());
	}

	@Override
	public void logWarn(Object msg, Throwable thrown) {
		logger.warn(msg.toString(), thrown);
	}
}
