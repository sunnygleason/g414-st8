package com.g414.st8.guice;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.g414.inno.db.Database;
import com.g414.inno.db.tpl.DatabaseTemplate;
import com.g414.st8.inno.InternalTableDefinitions;
import com.g414.st8.inno.TableManager;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Scopes;

public class DatabaseModule extends AbstractModule {
    @Override
    protected void configure() {
        final Database database = new Database();
        String databaseName = System.getProperty("database", "inno");
        database.createDatabase(databaseName);

        bind(Database.class).toInstance(database);
        bind(DatabaseTemplate.class).toInstance(new DatabaseTemplate(database));

        bind(Key.get(String.class, DatabaseName.class))
                .toInstance(databaseName);
        bind(InternalTableDefinitions.class).in(Scopes.SINGLETON);
        bind(TableManager.class);
    }

    @BindingAnnotation
    @Retention(RetentionPolicy.RUNTIME)
    public @interface DatabaseName {
    }
}
