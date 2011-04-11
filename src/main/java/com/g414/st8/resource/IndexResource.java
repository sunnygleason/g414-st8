package com.g414.st8.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

import com.g414.haildb.ColumnDef;
import com.g414.haildb.Cursor.CursorDirection;
import com.g414.haildb.Cursor.SearchMode;
import com.g414.haildb.IndexDef;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.Transaction.TransactionLevel;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.Traversal;
import com.g414.haildb.tpl.Functional.TraversalSpec;
import com.g414.st8.haildb.TableManager;
import com.g414.st8.helpers.EncodingHelper;
import com.g414.st8.helpers.OpaquePaginationHelper;
import com.g414.st8.query.QueryEvaluator;
import com.g414.st8.query.QueryLexer;
import com.g414.st8.query.QueryParser;
import com.g414.st8.query.QueryTerm;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does secondary index search
 * operations for KV documents using a "real" db index.
 */
@Path("/1.0/i")
public class IndexResource {
    @Inject
    private DatabaseTemplate dbt;

    @Inject
    private QueryEvaluator eval;

    @Inject
    private TableManager manager;

    @GET
    @Path("{type}.{index}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("type") String type,
            @PathParam("index") String indexName,
            @QueryParam("q") String query, @QueryParam("s") String token,
            @QueryParam("n") Long num) throws Exception {
        return doSearch(type, indexName, query, token, num);
    }

    /**
     * Perform a search against the specified object type, using the specified
     * index name and query string.
     * 
     * @param type
     * @param index
     * @param query
     * @return
     * @throws Exception
     */
    private Response doSearch(final String table, final String index,
            final String query, final String token, Long pageSize)
            throws Exception {
        final List<QueryTerm> queryTerms;
        if (query != null) {
            try {
                queryTerms = parseQuery(query);
            } catch (Exception e) {
                return Response.status(Status.BAD_REQUEST)
                        .entity("Invalid query: " + query).build();
            }
        } else {
            queryTerms = Collections.emptyList();
        }

        final TableDef tableDef = manager.getTableDef(table);
        final IndexDef indexDef = tableDef.getIndexDef(index);

        boolean hasToken = (token != null);

        final SortedQueryTerms sortedQuery = sortQueryTerms(hasToken, eval,
                tableDef, indexDef, queryTerms);

        if (pageSize == null || pageSize > 100 || pageSize < 1) {
            pageSize = OpaquePaginationHelper.DEFAULT_PAGE_SIZE;
        }

        final Map<String, Object> firstKey = !hasToken ? sortedQuery
                .getFirstKey() : OpaquePaginationHelper
                .decodeOpaqueCursor(token);

        final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        final Long num = pageSize + 1;

        dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                new TransactionCallback<Void>() {
                    @Override
                    public Void inTransaction(Transaction txn) {
                        Traversal<Map<String, Object>> iter = Functional.map(
                                txn, new TraversalSpec(new Target(tableDef,
                                        index), CursorDirection.ASC,
                                        sortedQuery.getSearchMode(), firstKey,
                                        sortedQuery.getPrimaryFilter(),
                                        sortedQuery.getFilter()),
                                new Mapping<Map<String, Object>>() {
                                    @Override
                                    public Map<String, Object> map(
                                            Map<String, Object> row) {
                                        return row;
                                    }
                                });

                        try {
                            for (int i = 0; iter.hasNext() && i < num; i++) {
                                results.add(iter.next());
                            }
                        } finally {
                            iter.close();
                        }

                        return null;
                    }
                });

        Map<String, Object> result = new LinkedHashMap<String, Object>();

        Map<String, Object> lastId = null;
        if (results.size() > pageSize) {
            lastId = results.remove(pageSize.intValue());
        }

        result.put("table", table);
        result.put("index", index);
        result.put("query", query);
        result.put("results", results);

        String theNext = (lastId != null) ? OpaquePaginationHelper
                .createOpaqueCursor(lastId) : null;

        result.put("pageSize", pageSize);
        result.put("next", theNext);

        String valueJson = EncodingHelper.convertToJson(result);

        return Response.status(Status.OK).entity(valueJson).build();
    }

    /**
     * Parses the query string using our trusty ANTLR-generated parser.
     * 
     * @param queryString
     * @return a list of QueryTerm instances
     * @throws Exception
     *             if the query is not parseable
     */
    private List<QueryTerm> parseQuery(String queryString) throws Exception {
        QueryLexer lex = new QueryLexer(new ANTLRStringStream(queryString));
        CommonTokenStream tokens = new CommonTokenStream(lex);
        QueryParser parser = new QueryParser(tokens);

        List<QueryTerm> query = new ArrayList<QueryTerm>();
        parser.term_list(query);

        return query;
    }

    private SortedQueryTerms sortQueryTerms(boolean hasToken,
            final QueryEvaluator eval, TableDef tableDef, IndexDef index,
            List<QueryTerm> queryTerms) {
        List<QueryTerm> primaryTerms = new ArrayList<QueryTerm>();
        List<QueryTerm> filterTerms = new ArrayList<QueryTerm>();
        Map<String, Object> firstKey = new LinkedHashMap<String, Object>();
        SearchMode searchMode = SearchMode.GE;

        OUTER: for (QueryTerm term : queryTerms) {
            for (ColumnDef col : index.getColumns()) {
                String colName = col.getName();

                if (colName.equals(term.getField())) {
                    primaryTerms.add(term);

                    switch (term.getOperator()) {
                    case EQ:
                        searchMode = SearchMode.GE;
                        firstKey.put(colName, term.getValue().getValue());
                        break;
                    case GE:
                        searchMode = SearchMode.GE;
                        firstKey.put(colName, term.getValue().getValue());
                        break;
                    case GT:
                        searchMode = (!hasToken) ? SearchMode.G : SearchMode.GE;
                        firstKey.put(colName, term.getValue().getValue());
                        break;
                    case LE:
                        searchMode = SearchMode.LE;
                        break;
                    case LT:
                        searchMode = SearchMode.L;
                        break;
                    }

                    continue OUTER;
                }
            }

            filterTerms.add(term);
        }

        return new SortedQueryTerms(eval, primaryTerms, filterTerms, firstKey,
                searchMode);
    }

    private static class SortedQueryTerms {
        private final Filter primaryFilter;
        private final Filter filter;
        private final Map<String, Object> firstKey;
        private final SearchMode searchMode;

        public SortedQueryTerms(final QueryEvaluator eval,
                final List<QueryTerm> primary, final List<QueryTerm> filter,
                Map<String, Object> firstKey, final SearchMode searchMode) {

            this.primaryFilter = new Filter() {
                private final List<QueryTerm> terms = Collections
                        .unmodifiableList(primary);

                @Override
                public Boolean map(Map<String, Object> row) {
                    return eval.matches(row, terms);
                }
            };

            this.filter = new Filter() {
                private final List<QueryTerm> terms = Collections
                        .unmodifiableList(filter);

                @Override
                public Boolean map(Map<String, Object> row) {
                    return eval.matches(row, terms);
                }
            };

            this.firstKey = Collections.unmodifiableMap(firstKey);
            this.searchMode = searchMode;
        }

        public Filter getPrimaryFilter() {
            return primaryFilter;
        }

        public Filter getFilter() {
            return filter;
        }

        public Map<String, Object> getFirstKey() {
            return firstKey;
        }

        public SearchMode getSearchMode() {
            return searchMode;
        }
    }
}
