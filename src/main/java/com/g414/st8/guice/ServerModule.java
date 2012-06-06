package com.g414.st8.guice;

import java.util.Map;

import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import com.g414.st8.resource.ClearDataResource;
import com.g414.st8.resource.CounterServiceResource;
import com.g414.st8.resource.GraphServiceResource;
import com.g414.st8.resource.IndexResource;
import com.g414.st8.resource.TableAdministrationResource;
import com.g414.st8.resource.TableDataFormResource;
import com.g414.st8.resource.TableDataUriResource;
import com.google.inject.internal.ImmutableMap;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class ServerModule extends ServletModule {
    protected void configureServlets() {
        bind(TableAdministrationResource.class).asEagerSingleton();
        bind(TableDataUriResource.class).asEagerSingleton();
        bind(TableDataFormResource.class).asEagerSingleton();
        bind(IndexResource.class).asEagerSingleton();
        bind(CounterServiceResource.class).asEagerSingleton();
        bind(GraphServiceResource.class).asEagerSingleton();
        bind(ClearDataResource.class).asEagerSingleton();

        bind(ObjectMapper.class).asEagerSingleton();
        bind(MessageBodyReader.class).to(JacksonJsonProvider.class)
                .asEagerSingleton();
        bind(MessageBodyWriter.class).to(JacksonJsonProvider.class)
                .asEagerSingleton();

        serve("*").with(GuiceContainer.class, getGzipFilters());
    }

    private static Map<String, String> getGzipFilters() {
        return ImmutableMap.of(
                ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
                GZIPContentEncodingFilter.class.getName(),
                ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS,
                GZIPContentEncodingFilter.class.getName());
    }
}
