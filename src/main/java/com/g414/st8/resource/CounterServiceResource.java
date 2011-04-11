package com.g414.st8.resource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleRegistration;
import com.g414.guice.lifecycle.LifecycleSupportBase;
import com.g414.haildb.InnoException;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.Transaction.TransactionLevel;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Mutation;
import com.g414.haildb.tpl.Functional.MutationType;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.Traversal;
import com.g414.haildb.tpl.Functional.TraversalSpec;
import com.g414.hash.LongHash;
import com.g414.hash.impl.MurmurHash;
import com.g414.st8.haildb.InternalTableDefinitions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

@Path("/1.0/c")
@Produces(MediaType.APPLICATION_JSON)
public class CounterServiceResource implements LifecycleRegistration {
    private LongHash hash = new MurmurHash();

    @Inject
    private InternalTableDefinitions definitions;

    @Inject
    private DatabaseTemplate dbt;

    private TableDef def;

    private AtomicLong serial = new AtomicLong();

    @Inject
    public void register(Lifecycle lifecycle) {
        lifecycle.register(new LifecycleSupportBase() {
            public void init() {
                CounterServiceResource.this.initialize();
            }
        });
    }

    public void initialize() {
        this.def = definitions.getCounters();
    }

    @GET
    @Path("{key}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getCounter(final @PathParam("key") String key)
            throws Exception {
        final Long keyHash = hash.getLongHashCode(key);
        final Map<String, Object> firstKey = ImmutableMap.<String, Object> of(
                "key_hash", keyHash, "key", key);

        final Filter primaryFilter = getPrimaryFilter(key, keyHash);

        Number count = dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Number>() {
                    @Override
                    public Number inTransaction(Transaction txn) {
                        Traversal<Number> iter = null;
                        try {
                            iter = Functional.map(txn, new TraversalSpec(
                                    new Target(def, "K"), firstKey,
                                    primaryFilter, null),
                                    new Mapping<Number>() {
                                        public Number map(
                                                Map<String, Object> row) {
                                            return (Number) row.get("count");
                                        }
                                    });

                            if (iter.hasNext()) {
                                return iter.next();
                            }

                            return null;
                        } finally {
                            if (iter != null) {
                                iter.close();
                            }
                        }
                    }
                });

        if (count != null) {
            return Response.status(Status.OK).entity(count.toString()).build();
        }

        return Response.status(Status.NOT_FOUND).entity("").build();
    }

    @POST
    @Path("{key}")
    @Produces(MediaType.TEXT_PLAIN)
    public String updateCounter(final @PathParam("key") String key,
            @QueryParam("i") Long increment) throws Exception {
        final Long i = (increment == null) ? 1L : increment;

        final Long keyHash = hash.getLongHashCode(key);
        final Map<String, Object> firstKey = ImmutableMap.<String, Object> of(
                "key_hash", keyHash, "key", key);

        final Filter primaryFilter = getPrimaryFilter(key, keyHash);

        for (int retries = 0; retries < 20; retries++) {
            try {
                return dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                        new TransactionCallback<String>() {
                            @Override
                            public String inTransaction(Transaction txn) {
                                Traversal<Mutation> iter = null;
                                try {
                                    iter = Functional.apply(txn, dbt,
                                            new TraversalSpec(new Target(def,
                                                    "K"), firstKey,
                                                    primaryFilter, null),
                                            new Mapping<Mutation>() {
                                                public Mutation map(
                                                        Map<String, Object> row) {
                                                    Long newCount = i
                                                            + ((Number) row
                                                                    .get("count"))
                                                                    .longValue();
                                                    Map<String, Object> newRow = new LinkedHashMap<String, Object>();
                                                    newRow.putAll(row);
                                                    newRow.put("count",
                                                            newCount);

                                                    return new Mutation(
                                                            MutationType.INSERT_OR_UPDATE,
                                                            newRow);
                                                }
                                            });

                                    if (iter.hasNext()) {
                                        return ((Number) iter.next()
                                                .getInstance().get("count"))
                                                .toString();
                                    }
                                } finally {
                                    if (iter != null) {
                                        iter.close();
                                    }
                                }

                                Map<String, Object> newRow = new LinkedHashMap<String, Object>();
                                newRow.put("id",
                                        Long.valueOf(serial.getAndIncrement()));
                                newRow.putAll(firstKey);
                                newRow.put("count", i);

                                dbt.insertOrUpdate(txn, def, newRow);

                                return Long.toString(i);
                            }
                        });
            } catch (Exception e) {
                if (e.getCause() instanceof InnoException) {
                    // ignore deadlock
                } else {
                    throw e;
                }
            } finally {
                // this space intentionally left blank
            }

            try {
                Thread.sleep((retries << 6) + 10);
            } catch (Exception e) {
                // this space intentionally left blank
            }
        }

        throw new WebApplicationException(Response.status(
                Status.INTERNAL_SERVER_ERROR).build());
    }

    @DELETE
    @Path("{key}")
    @Produces(MediaType.TEXT_PLAIN)
    public void deleteData(final @PathParam("key") String key) throws Exception {
        final Long keyHash = hash.getLongHashCode(key);
        final Map<String, Object> firstKey = ImmutableMap.<String, Object> of(
                "key_hash", keyHash, "key", key);

        final Filter primaryFilter = getPrimaryFilter(key, keyHash);

        dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.apply(
                                txn,
                                dbt,
                                new TraversalSpec(new Target(def, "K"),
                                        firstKey, primaryFilter, null),
                                new Mapping<Mutation>() {
                                    public Mutation map(Map<String, Object> row) {
                                        return new Mutation(
                                                MutationType.DELETE, row);
                                    }
                                }).traverseAll();

                        return null;
                    }
                });
    }

    private static Filter getPrimaryFilter(final String key, final Long keyHash) {
        final Filter primaryFilter = new Filter() {
            @Override
            public Boolean map(Map<String, Object> row) {
                return ((Number) row.get("key_hash")).longValue() == keyHash
                        && ((String) row.get("key")).equals(key);
            }
        };
        return primaryFilter;
    }
}
