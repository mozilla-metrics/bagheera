package com.mozilla.bagheera.http;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.jboss.netty.handler.codec.http.HttpMethod.POST;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.FakeChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.junit.Before;
import org.junit.Test;

public class ContentLengthFilterTest {

    private ChannelHandlerContext ctx;
    private InetSocketAddress remoteAddr;
    
    @Before
    public void setup() {
        Channel channel = createMock(Channel.class);
        expect(channel.getCloseFuture()).andReturn(new DefaultChannelFuture(channel, false));
        expect(channel.getRemoteAddress()).andReturn(InetSocketAddress.createUnresolved("192.168.1.1", 51723));
        
        OrderedMemoryAwareThreadPoolExecutor executor = new OrderedMemoryAwareThreadPoolExecutor(10, 0L, 0L);
        final ExecutionHandler handler = new ExecutionHandler(executor, true, true);
        
        ctx = new FakeChannelHandlerContext(channel, handler);
    }
    
    @Test
    public void testNegativeLengthFilter() {
        boolean success = false;
        try {
            ContentLengthFilter filter = new ContentLengthFilter(-1);
        } catch (IllegalArgumentException e) {
            success = true;
        }
        assertTrue(success);
    }
    
    private MessageEvent createMockEvent(Channel channel, HttpVersion protocolVersion, HttpMethod method, String uri, byte[] contentBytes) {    
        MessageEvent event = createMock(UpstreamMessageEvent.class);
        expect(event.getChannel()).andReturn(channel).anyTimes();
        expect(event.getFuture()).andReturn(new DefaultChannelFuture(channel,false)).anyTimes();
        expect(event.getRemoteAddress()).andReturn(remoteAddr);
        HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        req.setChunked(false);
        req.setContent(ChannelBuffers.copiedBuffer(contentBytes));
        expect(event.getMessage()).andReturn(req);
        replay(channel, event);
        
        return event;
    }
    
    @Test
    public void testRequestTooLong() {
        ContentLengthFilter filter = new ContentLengthFilter(1);
        byte[] contentBytes = new String("foo").getBytes();
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, POST, "/", contentBytes));
        } catch (TooLongFrameException e) {
            success = true;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertTrue(success);
    }
    
    @Test
    public void testFilterSuccess() {
        ContentLengthFilter filter = new ContentLengthFilter(4);
        byte[] contentBytes = new String("foo").getBytes();
        boolean success = false;
        try {
            filter.messageReceived(ctx, createMockEvent(ctx.getChannel(), HTTP_1_1, POST, "/", contentBytes));
            success = true;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertTrue(success);
    }
}
