package com.ergodicity.marketdb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class App {
    private static Logger log = LoggerFactory.getLogger(App.class);

    final boolean[] shutdown = new boolean[]{false};

    public static void main(String[] args) throws IOException {
        App app = new App();
        app.startUp();
    }

    private void startUp() throws IOException {
        log.info("Starting MarketDB.");

        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        ((ConfigurableApplicationContext) context).registerShutdownHook();

        try {
            waitForShutdown();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to execute waitForShutdown");
        }

        log.info("Shutdown MarketDB");
    }

    /**
     * Wait for a shutdown invocation elsewhere
     *
     * @throws Exception If some shit happens
     */
    protected void waitForShutdown() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (shutdown) {
                    shutdown[0] = true;
                    shutdown.notify();
                }
            }
        });

        // Wait for any shutdown event
        synchronized (shutdown) {
            while (!shutdown[0]) {
                try {
                    shutdown.wait();
                } catch (InterruptedException e) {
                    log.debug("Shutdown interrupted.");
                }
            }
        }
    }
}
