package com.mozilla.bagheera.metrics;

import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.GangliaReporter;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import com.yammer.metrics.reporting.MetricsServlet;
import com.yammer.metrics.reporting.HealthCheckServlet;
import com.yammer.metrics.util.DeadlockHealthCheck;
import com.yammer.metrics.HealthChecks;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.io.InputStream;

import com.yammer.metrics.core.Counter;

public class MetricsManager {
    private static final String METRICS_NAME_PREFIX = "bagheera";
    private static final String GLOBAL_HTTP_METRIC_ID = METRICS_NAME_PREFIX + "." + "global";

    private static final Logger LOG = Logger.getLogger(MetricsManager.class);
    private static MetricsManager instance = null;
    private static final String METRICS_PROPERTIES_RESOURCE_NAME = "/bagheera.metrics.properties";
    private final Properties MetricsConfig;
    private ConcurrentMap<String, HttpMetric> httpMetrics;
    
    private MetricsManager() {
        Properties prop = new Properties();
        InputStream in = getClass().getResourceAsStream(METRICS_PROPERTIES_RESOURCE_NAME);

        try {
            prop.load(in);
            in.close();
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not find the properites file: " + METRICS_PROPERTIES_RESOURCE_NAME);
        }
        
        MetricsConfig = prop;
    }        

    public synchronized static MetricsManager getInstance() {
        if(instance == null) {
            instance = new MetricsManager();
        }
        return instance;
    }

    private void configureHealthChecks() {
        HealthChecks.register(new DeadlockHealthCheck());
    }

    public String getConfigParam(String name) {
        return getConfigParam(name, null);
    }
    
    public String getConfigParam(String name, String defval) {
        return MetricsConfig.getProperty("bagheera.metrics." + name, defval);
    }
    
    private void configureWebServer() throws Exception {
        Integer port = Integer.parseInt(getConfigParam("webserver.port"));
        String host = getConfigParam("webserver.host");
        InetSocketAddress addr;
        
        if (host == null)
            addr = new InetSocketAddress(port);
        else
            addr = new InetSocketAddress(host, port);
        
        Server server = new Server(addr);
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/");
        
        handler.addServlet(MetricsServlet.class, "/metrics");
        handler.addServlet(HealthCheckServlet.class, "/health");

        ServletHolder holder = new ServletHolder(DefaultServlet.class);
        holder.setInitParameter("dirAllowed", "true");
        holder.setInitParameter("resourceBase",getConfigParam("webserver.static.path"));
        holder.setInitParameter("pathInfoOnly", "true");
        handler.addServlet(holder, "/static/*");

        server.setHandler(handler);
        server.start();
    }

    private void configureReporters() {
        if (Boolean.parseBoolean(getConfigParam("rrd.enable")))
            Rrd4jReporter.enable(
                Integer.parseInt(getConfigParam("rrd.db.update.secs")),
                TimeUnit.SECONDS,
                new File(getConfigParam("rrd.db.path")),
                new File(getConfigParam("rrd.graph.path")),
                Integer.parseInt(getConfigParam("rrd.graph.update.secs")));
        
        if (Boolean.parseBoolean(getConfigParam("ganglia.enable")))
            GangliaReporter.enable(Long.parseLong(getConfigParam("ganglia.update.secs")), TimeUnit.SECONDS,
                getConfigParam("ganglia.host"), Integer.parseInt(getConfigParam("ganglia.port")));
    }

    private void configureHttpMetrics() {
        httpMetrics = new ConcurrentHashMap<String, HttpMetric> ();
        HttpMetric h = new HttpMetric(GLOBAL_HTTP_METRIC_ID);
        httpMetrics.put(GLOBAL_HTTP_METRIC_ID, h);
    }

    public void run() throws Exception {
        this.configureHealthChecks();
        this.configureWebServer();
        this.configureReporters();
        this.configureHttpMetrics();
    }

    private String namespaceToId(String namespace) {
        return METRICS_NAME_PREFIX + ".ns." + namespace;
    }

    public HttpMetric getGlobalHttpMetric() {
        return getHttpMetricForId(GLOBAL_HTTP_METRIC_ID);
    }

    private HttpMetric getHttpMetricForId(String id) {
        final HttpMetric metric = httpMetrics.get(id);

        if (metric == null) {
            final HttpMetric newMetric = httpMetrics.putIfAbsent(id, new HttpMetric(id));
            if (newMetric == null) {
                return httpMetrics.get(id);
            }
            else {
                return newMetric;
            }
        }
        else {
            return metric;
        }
    }

    public HttpMetric getHttpMetricForNamespace(String namespace) {
        return getHttpMetricForId(namespaceToId(namespace));
    }
}