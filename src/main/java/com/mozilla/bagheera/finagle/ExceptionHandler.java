package com.mozilla.bagheera.finagle;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import scala.PartialFunction;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.util.Future;

public class ExceptionHandler extends SimpleFilter<HttpRequest, HttpResponse> {

    @Override
    public Future<HttpResponse> apply(HttpRequest request, Service<HttpRequest, HttpResponse> next) {
        // TODO: Somehow handle exceptions here?
        return next.apply(request);
    }

}
