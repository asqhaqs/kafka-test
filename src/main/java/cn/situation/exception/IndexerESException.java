package cn.situation.exception;

public class IndexerESException extends Exception {

	public IndexerESException() {
	}

	public IndexerESException(String message) {
		super(message);
	}

	public IndexerESException(Throwable cause) {
		super(cause);
	}

	public IndexerESException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexerESException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
