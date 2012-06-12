package com.mozilla.bagheera.finagle;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.mozilla.bagheera.nio.codec.http.PathDecoder;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class SubmissionHandler extends Service<HttpRequest, HttpResponse> {

    // REST path indices
    public static final int ENDPOINT_PATH_IDX = 0;
    public static final int NAMESPACE_PATH_IDX = 1;
    public static final int ID_PATH_IDX = 2;
    
    // REST endpoints
    private static final String ENDPOINT_SUBMIT = "submit";
    
    @Override
    public Future<HttpResponse> apply(HttpRequest request) {
        PathDecoder pd = new PathDecoder(request.getUri());
        pd.getPathElement(ENDPOINT_PATH_IDX);
        return null;
    }

}
