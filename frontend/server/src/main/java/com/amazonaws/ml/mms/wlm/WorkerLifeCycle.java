/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.ml.mms.wlm;

import com.amazonaws.ml.mms.archive.Manifest;
import com.amazonaws.ml.mms.metrics.Metric;
import com.amazonaws.ml.mms.util.ConfigManager;
import com.amazonaws.ml.mms.util.Connector;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerLifeCycle {

    static final Logger logger = LoggerFactory.getLogger(WorkerLifeCycle.class);

    private ConfigManager configManager;
    private Model model;
    private int pid = -1;
    private Process process;
    private CountDownLatch latch;
    private boolean success;
    private Connector connector;

    public WorkerLifeCycle(ConfigManager configManager, Model model) {
        this.configManager = configManager;
        this.model = model;
    }

    private String[] getEnvString(String cwd, String modelPath, String handler) {
        ArrayList<String> envList = new ArrayList<>();
        Pattern blackList = configManager.getBlacklistPattern();
        String handlerFile = handler;
        if (handler.contains(":")) {
            handlerFile = handler.split(":")[0];
            if (handlerFile.contains("/")) {
                handlerFile = handlerFile.substring(0, handlerFile.lastIndexOf('/'));
            }
        }

        StringBuilder pythonPath = new StringBuilder();
        HashMap<String, String> environment = new HashMap<>(System.getenv());
        environment.putAll(configManager.getBackendConfiguration());
        pythonPath.append(handlerFile).append(File.pathSeparatorChar);
        if (System.getenv("PYTHONPATH") != null) {
            pythonPath.append(System.getenv("PYTHONPATH")).append(File.pathSeparatorChar);
        }

        pythonPath.append(modelPath);

        if (!cwd.contains("site-packages") && !cwd.contains("dist-packages")) {
            pythonPath.append(File.pathSeparatorChar).append(cwd);
        }

        environment.put("PYTHONPATH", pythonPath.toString());

        for (Map.Entry<String, String> entry : environment.entrySet()) {
            if (!blackList.matcher(entry.getKey()).matches()) {
                envList.add(entry.getKey() + '=' + entry.getValue());
            }
        }

        return envList.toArray(new String[0]); // NOPMD
    }

    public void startWorker(int port) throws WorkerInitializationException, InterruptedException {
        File workingDir = new File(configManager.getModelServerHome());
        File modelPath;
        setPort(port);
        try {
            modelPath = model.getModelDir().getCanonicalFile();
        } catch (IOException e) {
            throw new WorkerInitializationException("Failed get MMS home directory", e);
        }

        String[] args = new String[6];
        Manifest.RuntimeType runtime = model.getModelArchive().getManifest().getRuntime();
        if (runtime == Manifest.RuntimeType.PYTHON) {
            args[0] = configManager.getPythonExecutable();
        } else {
            args[0] = runtime.getValue();
        }
        args[1] = new File(workingDir, "mms/model_service_worker.py").getAbsolutePath();
        args[2] = "--sock-type";
        args[3] = connector.getSocketType();
        args[4] = connector.isUds() ? "--sock-name" : "--port";
        args[5] = connector.getSocketPath();

        String[] envp =
                getEnvString(
                        workingDir.getAbsolutePath(),
                        modelPath.getAbsolutePath(),
                        model.getModelArchive().getManifest().getModel().getHandler());

        try {
            latch = new CountDownLatch(1);

            synchronized (this) {
                process = Runtime.getRuntime().exec(args, envp, modelPath);

                String threadName =
                        "W-"
                                + port
                                + '-'
                                + model.getModelName()
                                        .substring(0, Math.min(model.getModelName().length(), 25));
                new ReaderThread(threadName, process.getErrorStream(), true, this).start();
                new ReaderThread(threadName, process.getInputStream(), false, this).start();
            }

            if (latch.await(2, TimeUnit.MINUTES)) {
                if (!success) {
                    throw new WorkerInitializationException("Backend stream closed.");
                }
                return;
            }
            throw new WorkerInitializationException("Backend worker startup time out.");
        } catch (IOException e) {
            throw new WorkerInitializationException("Failed start worker process", e);
        } finally {
            if (!success) {
                exit();
            }
        }
    }

    public synchronized void exit() {
        if (process != null) {
            process.destroyForcibly();
            connector.clean();
        }
    }

    public synchronized Integer getExitValue() {
        if (process != null && !process.isAlive()) {
            return process.exitValue();
        }
        return null;
    }

    void setSuccess(boolean success) {
        this.success = success;
        latch.countDown();
    }

    public synchronized int getPid() {
        return pid;
    }

    public synchronized void setPid(int pid) {
        this.pid = pid;
    }

    private synchronized void setPort(int port) {
        connector = new Connector(port);
    }

    private static final class ReaderThread extends Thread {

        private InputStream is;
        private boolean error;
        private WorkerLifeCycle lifeCycle;
        static final org.apache.log4j.Logger loggerModelMetrics =
                org.apache.log4j.Logger.getLogger(ConfigManager.MODEL_METRICS_LOGGER);

        public ReaderThread(String name, InputStream is, boolean error, WorkerLifeCycle lifeCycle) {
            super(name + (error ? "-stderr" : "-stdout"));
            this.is = is;
            this.error = error;
            this.lifeCycle = lifeCycle;
        }

        @Override
        public void run() {
            try (Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
                while (scanner.hasNext()) {
                    String result = scanner.nextLine();
                    if (result == null) {
                        break;
                    }
                    if (result.startsWith("[METRICS]")) {
                        loggerModelMetrics.info(Metric.parse(result.substring(9)));
                        continue;
                    }

                    if ("MXNet worker started.".equals(result)) {
                        lifeCycle.setSuccess(true);
                    } else if (result.startsWith("[PID]")) {
                        lifeCycle.setPid(Integer.parseInt(result.substring("[PID]".length())));
                    }
                    if (error) {
                        logger.warn(result);
                    } else {
                        logger.info(result);
                    }
                }
            } finally {
                lifeCycle.setSuccess(false);
            }
        }
    }
}
