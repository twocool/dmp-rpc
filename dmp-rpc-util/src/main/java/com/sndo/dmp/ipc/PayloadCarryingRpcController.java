package com.sndo.dmp.ipc;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;

import java.util.List;

public class PayloadCarryingRpcController extends TimeLimitedRpcController implements CellScannable {

    private int priority = HConstants.NORMAL_QOS;

    private CellScanner cellScanner;

    public PayloadCarryingRpcController() {
        this((CellScanner) null);
    }

    public PayloadCarryingRpcController(final CellScanner cellScanner) {
        this.cellScanner = cellScanner;
    }

    public PayloadCarryingRpcController(final List<CellScannable> cellIterables) {
        this.cellScanner = (cellIterables == null ? null : CellUtil.createCellScanner(cellIterables));
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setCellScanner(final CellScanner cellScanner) {
        this.cellScanner = cellScanner;
    }

    @Override
    public void reset() {
        this.priority = 0;
        this.cellScanner = null;
    }

    @Override
    public CellScanner cellScanner() {
        return this.cellScanner;
    }
}
