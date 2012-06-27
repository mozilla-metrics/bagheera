package com.mozilla.bagheera.http;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMessage;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class RootResponse extends SimpleChannelUpstreamHandler {

    private static final String ROOT_PATH = "/";
     
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof HttpMessage) {
            HttpRequest request = (HttpRequest) msg;
            if (ROOT_PATH.equals(request.getUri()) || request.getUri().isEmpty()) {
                HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
                ChannelFuture future = e.getChannel().write(response);
                future.addListener(ChannelFutureListener.CLOSE);
            } else {
                Channels.fireMessageReceived(ctx, request, e.getRemoteAddress());
            }
        } else {
            ctx.sendUpstream(e);
        }
    }
    
}
