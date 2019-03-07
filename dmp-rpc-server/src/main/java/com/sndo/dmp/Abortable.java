package com.sndo.dmp;

public interface Abortable {

    void abort(String why, Throwable t);

    boolean isAborted();
}
