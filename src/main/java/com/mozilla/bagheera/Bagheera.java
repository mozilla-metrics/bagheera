package com.mozilla.bagheera;

import java.net.InetSocketAddress;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.mozilla.bagheera.nio.codec.http.PathDecoder;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.http.Http;
import com.twitter.util.Future;

public class Bagheera extends Service<HttpRequest, HttpResponse> {

    // REST path indices
    public static final int ENDPOINT_PATH_IDX = 0;
    public static final int NAMESPACE_PATH_IDX = 1;
    public static final int ID_PATH_IDX = 2;
    
    public Bagheera() {       
    }
    
    @Override
    public Future<HttpResponse> apply(HttpRequest request) {
        // TODO Auto-generated method stub
        PathDecoder pd = new PathDecoder(request.getUri());
        pd.getPathElement(ENDPOINT_PATH_IDX);
        return null;
    }
    
    public static void main(String[] args) {
        Bagheera service = new Bagheera();
        ServerBuilder.safeBuild(service,
                                ServerBuilder.get()
                                .codec(Http.get())
                                .name("bagheera")
                                .bindTo(new InetSocketAddress("localhost", 8080)));
    }
    
}
