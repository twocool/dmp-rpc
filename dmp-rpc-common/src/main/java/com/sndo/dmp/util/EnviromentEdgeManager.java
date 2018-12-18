package com.sndo.dmp.util;

/**
 * @author yangqi
 * @date 2018/12/18 17:31
 **/
public class EnviromentEdgeManager {

    private static volatile EnviromentEdge delegate = new DefaultEnviromentEdge();

    public EnviromentEdgeManager() {}

    public static EnviromentEdge getDelegate() {
        return delegate;
    }

    public static void injectEdge(EnviromentEdge edge) {
        if (edge == null) {
            reset();
        } else {
            delegate = edge;
        }
    }

    public static void reset() {
        injectEdge(new DefaultEnviromentEdge());
    }

    public static long currentTime() {
        return delegate.currentTime();
    }
}
