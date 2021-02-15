package org.apache.solr.handler.component;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.QUERY_CANCELLATION_UUID;

/**
 * Handles requests for query cancellation for cancellable queries
 */
public class QueryCancellationHandler extends TaskManagementHandler {
    // This can be a parent level member but we keep it here to allow future handlers to have
    // a custom list of components
    private List<SearchComponent> components;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        ResponseBuilder rb = buildResponseBuilder(req, rsp, getComponentsList());
        Map<String, String> extraParams = null;

        rb.setCancellation(true);

        String cancellationUUID = req.getParams().get(QUERY_CANCELLATION_UUID, null);

        if (cancellationUUID == null) {
            throw new IllegalArgumentException("Query cancellation was requested but no query UUID for cancellation was given");
        }

        if (rb.isDistrib) {
            extraParams = new HashMap<>();

            extraParams.put(QUERY_CANCELLATION_UUID, cancellationUUID);
        } else {
            rb.setCancellationUUID(cancellationUUID);
        }

        processRequest(req, rb, extraParams);

        rsp.getValues().add("status", "query with queryID " + rb.getCancellationUUID() + " " + "cancelled");
    }

    @Override
    public String getDescription() {
        return "Cancel queries";
    }

    @Override
    public Category getCategory() {
        return Category.ADMIN;
    }

    @Override
    public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
        return PermissionNameProvider.Name.READ_PERM;
    }

    @Override
    public SolrRequestHandler getSubHandler(String path) {
        if (path.equals("/tasks/cancel")) {
            return this;
        }

        return null;
    }

    @Override
    public Boolean registerV2() {
        return Boolean.TRUE;
    }

    @Override
    public Collection<Api> getApis() {
        return ApiBag.wrapRequestHandlers(this, "core.tasks.cancel");
    }

    private List<SearchComponent> getComponentsList() {
        if (components == null) {
            components = buildComponentsList();
        }

        return components;
    }
}
