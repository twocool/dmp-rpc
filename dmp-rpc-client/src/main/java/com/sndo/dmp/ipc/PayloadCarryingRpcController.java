package com.sndo.dmp.ipc;

import com.sndo.dmp.CellScannable;
import com.sndo.dmp.CellScanner;

/**
 * @author yangqi
 * @date 2018/12/18 16:25
 **/
public class PayloadCarryingRpcController extends TimeLimitedRpcController implements CellScannable {

    @Override
    public CellScanner cellScanner() {
        // TODO
        return null;
    }
}
