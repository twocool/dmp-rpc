package com.sndo.dmp.client;

/**
 * @author yangqi
 * @date 2018/12/29 16:01
 **/
public class MetricsConnection {

    public static class CallStats {
        private long requestSizeBytes = 0;
        private long responseSizeBytes = 0;
        private long startTime = 0;
        private long callTimeMs = 0;

        public long getRequestSizeBytes() {
            return requestSizeBytes;
        }

        public void setRequestSizeBytes(long requestSizeBytes) {
            this.requestSizeBytes = requestSizeBytes;
        }

        public long getResponseSizeBytes() {
            return responseSizeBytes;
        }

        public void setResponseSizeBytes(long responseSizeBytes) {
            this.responseSizeBytes = responseSizeBytes;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getCallTimeMs() {
            return callTimeMs;
        }

        public void setCallTimeMs(long callTimeMs) {
            this.callTimeMs = callTimeMs;
        }
    }

    public static CallStats callStats() {
        return new CallStats();
    }
}
