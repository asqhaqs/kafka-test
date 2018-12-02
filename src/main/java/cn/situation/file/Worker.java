package cn.situation.file;

import cn.situation.cons.SystemConstant;
import cn.situation.util.LogUtil;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

public class Worker implements Runnable {

    private static final Logger LOG = LogUtil.getInstance(Worker.class);

    static {

    }

    private ZMQ.Context context;

    public Worker (ZMQ.Context context) {
        this.context = context;
    }

    @Override
    public void run() {
        ZMQ.Socket socket = context.socket(ZMQ.PULL);
        socket.connect ("inproc://workers");
        socket.setHWM(Integer.valueOf(SystemConstant.ZMQ_RCVHWM));
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] result = socket.recv(0);
                LOG.info(String.format("[%s]: result<%s>, thread-id<%s>", "Worker.run", new String(result),
                        getId()));
                action(result);
            } catch (Exception e) {
                LOG.error("[%s]: message<%s>, thread-id<%s>", "Worker.run", e.getMessage(), getId());
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        socket.close();
    }

    private void action(byte[] data) throws Exception {
        try {
            String logValue = new String(data);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }

    private long getId() {
        return Thread.currentThread().getId();
    }
}
