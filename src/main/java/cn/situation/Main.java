package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.file.Worker;
import cn.situation.util.LogUtil;
import org.slf4j.Logger;
import org.zeromq.ZMQ;

public class Main {

    private static final Logger LOG = LogUtil.getInstance(Main.class);

    public static void main(String[] args) {

        ZMQ.Context context = ZMQ.context(0);

        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.bind("inproc://workers");
        sender.setHWM(Integer.valueOf(SystemConstant.ZMQ_SNDHWM));

        int threadNum = Integer.valueOf(SystemConstant.WORKER_THREAD_NUM);
        for(int num = 0; num < threadNum; num++) {
            Worker worker = new Worker(context);
            new Thread(worker).start();
        }

        LOG.error(String.format("[%s]: message<%s>", "main", "start work..."));

        while (!Thread.currentThread ().isInterrupted ()) {
            try {
                String data = "test";
                sender.send(data, 0);
                try {
                    Thread.sleep(Long.parseLong(SystemConstant.EXEC_INTERVAL_MS));
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }
        sender.close();
        context.term();
    }
}
