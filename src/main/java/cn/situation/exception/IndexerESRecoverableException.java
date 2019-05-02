package cn.situation.exception;

public class IndexerESRecoverableException extends Exception {

	public IndexerESRecoverableException() {
	}

	public IndexerESRecoverableException(String message) {
		super(message);
	}

	public IndexerESRecoverableException(Throwable cause) {
		super(cause);
	}

	public IndexerESRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexerESRecoverableException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
