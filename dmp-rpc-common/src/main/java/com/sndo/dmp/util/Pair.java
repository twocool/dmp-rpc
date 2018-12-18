package com.sndo.dmp.util;

import java.io.Serializable;

/**
 * @author yangqi
 * @date 2018/12/18 16:38
 **/
public class Pair<T1, T2> implements Serializable {
    private static final long serialVersionUID = 1463792275409049561L;
    private T1 first = null;
    private T2 second = null;

    public Pair() {}

    public Pair(T1 a, T2 b) {
        this.first = a;
        this.second = b;
    }

    public static <T1, T2> Pair<T1, T2> newPair(T1 a, T2 b) {
        return new Pair<T1, T2>(a, b);
    }

    public T1 getFirst() {
        return first;
    }

    public void setFirst(T1 first) {
        this.first = first;
    }

    public T2 getSecond() {
        return second;
    }

    public void setSecond(T2 second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
        return second != null ? second.equals(pair.second) : pair.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
