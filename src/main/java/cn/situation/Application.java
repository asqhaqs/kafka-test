package cn.situation;

import cn.situation.cons.SystemConstant;
import cn.situation.util.LogUtil;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Application {
    private static final Logger LOG = LogUtil.getInstance(Application.class);

    public static void main(String[] args) {
        LOG.info("Starting Application...");
        ClassPathXmlApplicationContext indexerContext = new ClassPathXmlApplicationContext(SystemConstant.SPRING_APPLICATION_CONTEXT);
        indexerContext.registerShutdownHook();
        LOG.info("Application is started OK");
    }
}