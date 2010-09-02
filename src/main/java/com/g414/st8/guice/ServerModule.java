package com.g414.st8.guice;

import java.util.Map;

import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.resource.TableAdministrationResource;
import com.g414.st8.resource.TableDataFormResource;
import com.g414.st8.resource.TableDataUriResource;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.internal.ImmutableMap;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class ServerModule extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new DatabaseModule(), new ServletModule() {
            @Override
            protected void configureServlets() {
                bind(TableAdministrationResource.class).in(Scopes.SINGLETON);
                bind(TableDataUriResource.class).in(Scopes.SINGLETON);
                bind(TableDataFormResource.class).in(Scopes.SINGLETON);

                bind(ObjectMapper.class);
                bind(MessageBodyReader.class).to(JacksonJsonProvider.class);
                bind(MessageBodyWriter.class).to(JacksonJsonProvider.class);

                serve("*").with(GuiceContainer.class, getGzipFilters());
            }
        });
    }

    private static Map<String, String> getGzipFilters() {
        return ImmutableMap.of(
                ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
                GZIPContentEncodingFilter.class.getName(),
                ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS,
                GZIPContentEncodingFilter.class.getName());
    }
}
