package com.g414.st8;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st8.guice.DatabaseModule;
import com.g414.st8.guice.ServerModule;
import com.g414.st8.resource.NoopServlet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;

public class Main {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        ServletContextHandler root = new ServletContextHandler(server, "/",
                ServletContextHandler.NO_SESSIONS);

        Injector parentInjector = Guice.createInjector(new LifecycleModule(),
                new DatabaseModule(), new ServerModule());

        root.addFilter(GuiceFilter.class, "/*", 0);
        root.addServlet(NoopServlet.class, "/*");

        Lifecycle lifecycle = parentInjector.getInstance(Lifecycle.class);
        lifecycle.init();
        lifecycle.start();

        server.start();
    }
}
