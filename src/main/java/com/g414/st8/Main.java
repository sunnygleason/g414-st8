package com.g414.st8;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import com.g414.st8.guice.ServerModule;
import com.g414.st8.resource.NoopServlet;
import com.google.inject.servlet.GuiceFilter;

public class Main {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        Context root = new Context(server, "/", Context.SESSIONS);

        root.addEventListener(new ServerModule());
        root.addFilter(GuiceFilter.class, "/*", 0);
        root.addServlet(NoopServlet.class, "/*");

        server.start();
    }
}
