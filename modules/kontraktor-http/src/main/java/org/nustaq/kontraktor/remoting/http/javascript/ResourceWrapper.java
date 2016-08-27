package org.nustaq.kontraktor.remoting.http.javascript;


import io.undertow.io.IoCallback;
import io.undertow.io.Sender;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.resource.Resource;
import io.undertow.util.ETag;
import io.undertow.util.MimeMappings;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;

/**
 * Created by ruedi on 15/06/16.
 */
public class ResourceWrapper implements Resource {

    Resource wrapped;

    public ResourceWrapper(Resource wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getPath() {
        return wrapped.getPath();
    }

    @Override
    public Date getLastModified() {
        return wrapped.getLastModified();
    }

    @Override
    public String getLastModifiedString() {
        return wrapped.getLastModifiedString();
    }

    @Override
    public ETag getETag() {
        return wrapped.getETag();
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public boolean isDirectory() {
        return wrapped.isDirectory();
    }

    @Override
    public List<Resource> list() {
        return wrapped.list();
    }

    @Override
    public String getContentType(MimeMappings mimeMappings) {
        return wrapped.getContentType(mimeMappings);
    }

    @Override
    public void serve(Sender sender, HttpServerExchange exchange, IoCallback completionCallback) {
        wrapped.serve(sender,exchange,completionCallback);
    }

    @Override
    public Long getContentLength() {
        return wrapped.getContentLength();
    }

    @Override
    public String getCacheKey() {
        return wrapped.getCacheKey();
    }

    @Override
    public File getFile() {
        return wrapped.getFile();
    }

    @Override
    public Path getFilePath() {
        return getFile().toPath();
    }

    @Override
    public File getResourceManagerRoot() {
        return wrapped.getResourceManagerRoot();
    }

    @Override
    public Path getResourceManagerRootPath() {
        return wrapped.getResourceManagerRootPath();
    }

    @Override
    public URL getUrl() {
        return wrapped.getUrl();
    }
}
