package com.mantis.library;

public class SolrConnectorException extends RuntimeException{

	private static final long serialVersionUID = -7587434373929531057L;

	private String className,methodName;
	private Exception exception;
	
	public SolrConnectorException(String className, String methodName, Exception exception) {
		this.className = className;
		this.methodName = methodName;
		this.exception = exception;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	public Exception getException() {
		return exception;
	}
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	



}
