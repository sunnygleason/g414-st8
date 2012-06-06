package com.g414.st8.guice;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.g414.haildb.Database;
import com.g414.haildb.DatabaseConfiguration;
import com.g414.haildb.DatabaseConfiguration.LogFlushMode;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.st8.haildb.InternalTableDefinitions;
import com.g414.st8.haildb.TableManager;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;

public class DatabaseModule extends AbstractModule {
    @Override
    protected void configure() {
        final DatabaseConfiguration config = new DatabaseConfiguration();
        config.setBufferPoolSize(1024L * 1024L * 1024L);
        config.setFlushLogAtTrxCommitMode(LogFlushMode.ONCE_PER_SECOND);
        config.setIoCapacityIOPS(500);
        config.setAdaptiveFlushingEnabled(true);
        config.setDoublewriteEnabled(false);

        final Database database = new Database(config);
        String databaseName = System.getProperty("database", "hail");
        database.createDatabase(databaseName);

        bind(Database.class).toInstance(database);
        bind(DatabaseTemplate.class).toInstance(new DatabaseTemplate(database));

        bind(Key.get(String.class, DatabaseName.class))
                .toInstance(databaseName);
        bind(InternalTableDefinitions.class).asEagerSingleton();
        bind(TableManager.class).asEagerSingleton();
    }

    @BindingAnnotation
    @Retention(RetentionPolicy.RUNTIME)
    public @interface DatabaseName {
    }
}
