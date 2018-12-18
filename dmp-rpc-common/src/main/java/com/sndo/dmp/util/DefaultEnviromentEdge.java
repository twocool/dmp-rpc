package com.sndo.dmp.util;

/**
 * @author yangqi
 * @date 2018/12/18 17:32
 **/
public class DefaultEnviromentEdge implements EnviromentEdge {

    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }
}
