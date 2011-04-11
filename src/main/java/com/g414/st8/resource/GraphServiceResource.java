package com.g414.st8.resource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;

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
import com.g414.haildb.tpl.Functional.Reduction;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.TraversalSpec;
import com.g414.st8.haildb.InternalTableDefinitions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.api.uri.UriComponent;

@Path("/1.0/g")
@Produces(MediaType.APPLICATION_JSON)
public class GraphServiceResource implements LifecycleRegistration {
    @Inject
    private InternalTableDefinitions definitions;

    @Inject
    private DatabaseTemplate dbt;

    private TableDef def;

    @Inject
    private ObjectMapper mapper;

    @Inject
    public void register(Lifecycle lifecycle) {
        lifecycle.register(new LifecycleSupportBase() {
            public void init() {
                GraphServiceResource.this.initialize();
            }
        });
    }

    public void initialize() {
        this.def = definitions.getGraph();
    }

    @GET
    @Path("all")
    @Produces(MediaType.TEXT_PLAIN)
    public String showFullGraph() throws Exception {
        final StringBuilder response = new StringBuilder();
        final AtomicLong count = new AtomicLong();

        dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Functional.reduce(txn, new TraversalSpec(new Target(
                                def, "P"), null, new Filter() {
                            @Override
                            public Boolean map(Map<String, Object> row) {
                                return count.incrementAndGet() < 10000;
                            }
                        }, null), new Reduction<StringBuilder>() {
                            @Override
                            public StringBuilder reduce(
                                    Map<String, Object> row,
                                    StringBuilder initial) {
                                try {
                                    initial.append(mapper
                                            .writeValueAsString(row));
                                    initial.append("\n");

                                    return initial;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }, response);

                        return null;
                    }
                });

        return response.toString();
    }

    @GET
    @Path("bfs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response breadthFirstSearch(@QueryParam("a") final Long root,
            @QueryParam("b") final Long relation,
            @QueryParam("maxDepth") final Long depth) throws Exception {
        if (root == null || relation == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("missing root 'a' or relation 'b'").build();
        }

        final Long theDepth = (depth == null) ? 3L : depth;

        final Map<Long, Integer> nodeDepth = new LinkedHashMap<Long, Integer>();
        final AtomicInteger maxCount = new AtomicInteger(100000);

        dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        final Set<Long> explored = new HashSet<Long>();
                        final LinkedList<Long> frontier = new LinkedList<Long>();
                        frontier.add(root);
                        nodeDepth.put(root, 0);

                        while (!frontier.isEmpty()) {
                            final Long cur = frontier.removeFirst();
                            final Integer cDepth = nodeDepth.get(cur);

                            if (theDepth.longValue() <= cDepth) {
                                continue;
                            }

                            final Map<String, Object> firstKey = ImmutableMap
                                    .<String, Object> of("a", cur, "b",
                                            relation);

                            final Filter primaryFilter = new Filter() {
                                @Override
                                public Boolean map(Map<String, Object> row) {
                                    if (maxCount.decrementAndGet() <= 0) {
                                        throw new WebApplicationException(
                                                Response.status(
                                                        Status.SERVICE_UNAVAILABLE)
                                                        .entity("max graph depth of 100000 exceeded")
                                                        .build());
                                    }

                                    return (((Number) row.get("a")).longValue() == cur
                                            .longValue())
                                            && (((Number) row.get("b"))
                                                    .longValue() == relation
                                                    .longValue());
                                }
                            };

                            final Filter filter = new Filter() {
                                @Override
                                public Boolean map(Map<String, Object> row) {
                                    return !explored.contains(((Number) row
                                            .get("c")).longValue());
                                }
                            };

                            final TraversalSpec traversalSpec = new TraversalSpec(
                                    new Target(def, "P"), firstKey,
                                    primaryFilter, filter);

                            Functional.reduce(txn, traversalSpec,
                                    new Reduction<List<Long>>() {
                                        @Override
                                        public List<Long> reduce(
                                                Map<String, Object> row,
                                                List<Long> initial) {
                                            Long c = ((Number) row.get("c"))
                                                    .longValue();

                                            if (!nodeDepth.containsKey(c)) {
                                                frontier.add(c);
                                                nodeDepth.put(c, cDepth + 1);
                                            }

                                            return frontier;
                                        }
                                    }, frontier);

                            explored.add(cur);
                        }

                        return null;
                    }
                });

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("a", root);
        result.put("b", relation);
        result.put("maxDepth", depth);

        List<Map<String, Object>> hits = new ArrayList<Map<String, Object>>();

        for (Map.Entry<Long, Integer> entry : nodeDepth.entrySet()) {
            Map<String, Object> hit = new LinkedHashMap<String, Object>();
            hit.put("a", entry.getKey());
            hit.put("d", entry.getValue());

            hits.add(hit);
        }

        result.put("result", hits);

        return Response.status(Status.OK)
                .entity(mapper.writeValueAsString(result)).build();
    }

    @GET
    @Path("topo")
    @Produces(MediaType.APPLICATION_JSON)
    public Response topoSort(@QueryParam("a") final List<Long> roots,
            @QueryParam("b") final Long relation) throws Exception {
        if (roots == null || relation == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("missing root 'a' or relation 'b'").build();
        }

        final Set<Long> wanted = new HashSet<Long>();
        final Set<Long> done = new HashSet<Long>();
        final List<Long> order = new ArrayList<Long>();

        wanted.addAll(roots);

        final AtomicInteger maxCount = new AtomicInteger(100000);

        dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        OUTER: while (!wanted.isEmpty()) {
                            boolean foundOne = false;

                            Set<Long> allTodo = new HashSet<Long>();

                            Iterator<Long> iter = wanted.iterator();
                            while (iter.hasNext()) {
                                Long node = iter.next();

                                Set<Long> nodeTodo = getOutDeps(txn, node,
                                        relation, done, maxCount);

                                if (nodeTodo.size() == 0) {
                                    iter.remove();
                                    if (!done.contains(node)) {
                                        order.add(node);
                                    }
                                    done.add(node);
                                    foundOne = true;
                                } else {
                                    allTodo.addAll(nodeTodo);
                                }
                            }

                            boolean foundNew = false;

                            for (Long node : allTodo) {
                                if (!done.contains(node)
                                        && !wanted.contains(node)) {
                                    foundNew = true;
                                    wanted.add(node);
                                }
                            }

                            if (foundNew) {
                                continue OUTER;
                            }

                            if (!foundOne && !wanted.isEmpty()) {
                                throw new WebApplicationException(Response
                                        .status(Status.BAD_REQUEST)
                                        .entity("probable cycle: " + wanted)
                                        .build());
                            }
                        }

                        return null;
                    }
                });

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("a", roots);
        result.put("b", relation);

        result.put("order", order);

        return Response.status(Status.OK)
                .entity(mapper.writeValueAsString(result)).build();
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public Response addToGraph(@Context UriInfo uri) throws Exception {
        final Map<String, Object> toInsert = convertUriToMap(uri.getPath());

        for (int retries = 0; retries < 20; retries++) {
            try {
                dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                        new TransactionCallback<Void>() {
                            @Override
                            public Void inTransaction(Transaction txn) {
                                dbt.insertOrUpdate(txn, def, toInsert);

                                return null;
                            }
                        });

                return Response.status(Status.NO_CONTENT).entity("").build();
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
    @Produces(MediaType.TEXT_PLAIN)
    public Response deleteData(@Context UriInfo uri) throws Exception {
        final Map<String, Object> toDelete = convertUriToMap(uri.getPath());

        for (int retries = 0; retries < 20; retries++) {
            try {
                dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                        new TransactionCallback<Void>() {
                            @Override
                            public Void inTransaction(Transaction txn) {
                                dbt.delete(txn, def, toDelete);

                                return null;
                            }
                        });

                return Response.status(Status.NO_CONTENT).entity("").build();
            } catch (Exception e) {
                if (e.getCause() instanceof InnoException) {
                    e.printStackTrace();

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

    private static Map<String, Object> convertUriToMap(String path) {
        return flattenMap(UriComponent.decodeMatrix(path, true));
    }

    private static Map<String, Object> flattenMap(
            MultivaluedMap<String, String> inMap) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, List<String>> e : inMap.entrySet()) {
            String key = e.getKey();
            List<String> values = e.getValue();
            if (values.size() == 1) {
                result.put(key, values.get(0));
            } else {
                throw new IllegalArgumentException(
                        "Multivalued Parameters not allowed: " + key);
            }
        }

        return result;
    }

    private Set<Long> getOutDeps(final Transaction txn, final Long node,
            final Long relation, final Set<Long> done,
            final AtomicInteger maxCount) {
        final Map<String, Object> firstKey = ImmutableMap.<String, Object> of(
                "a", node, "b", relation);

        final Filter primaryFilter = new Filter() {
            @Override
            public Boolean map(Map<String, Object> row) {
                if (maxCount.decrementAndGet() <= 0) {
                    throw new WebApplicationException(Response
                            .status(Status.SERVICE_UNAVAILABLE)
                            .entity("max graph depth of 100000 exceeded")
                            .build());
                }

                return (((Number) row.get("a")).longValue() == node.longValue())
                        && (((Number) row.get("b")).longValue() == relation
                                .longValue());
            }
        };

        final Filter filter = new Filter() {
            @Override
            public Boolean map(Map<String, Object> row) {
                return !done.contains(((Number) row.get("c")).longValue());
            }
        };

        final TraversalSpec traversalSpec = new TraversalSpec(new Target(def,
                "P"), firstKey, primaryFilter, filter);

        return Functional.reduce(txn, traversalSpec,
                new Reduction<Set<Long>>() {
                    @Override
                    public Set<Long> reduce(Map<String, Object> row,
                            Set<Long> initial) {
                        initial.add(((Number) row.get("c")).longValue());

                        return initial;
                    }
                }, new HashSet<Long>());
    }
}
