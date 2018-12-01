package org.apache.hadoop.hdfs.server.statestore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class KVStatStore extends StateStore {
    static final Log LOG = LogFactory.getLog(Balancer.class);
    private static String API_SERVER_HOST = "http://127.0.0.1:8089";
    private HttpClient httpClient = new DefaultHttpClient();
    private String endpoint;

    public KVStatStore(String endpoint) {
      this.endpoint = endpoint;
    }

    public KVStatStore() {
      this(API_SERVER_HOST);
    }

    class INodeChildren {
        INodeMeta[] response;
    }

    private<T> Object request(String url, String method, Object body, Class<T> valueType) throws IOException {
        RequestBuilder builder =  RequestBuilder.create(method);
        builder.setUri(url);
        if (body != null) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValueAsString(body);
        }
        HttpRequest request = builder.build();
        HttpResponse response = httpClient.execute(HttpHost.create(endpoint),request);
        int statusCode = response.getStatusLine().getStatusCode();
        LOG.trace("request api " + url + "response code " + statusCode);
        if (statusCode >= 400) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.getEntity().getContent(), APIResponse.class);
        } else if (valueType != null) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.getEntity().getContent(), valueType);
        } else {
            return null;
        }
    }

    public long tso() {
        long[] ts =tso(1);
        if (ts == null) {
            return -1;
        }
        return ts[0];
    }

    public long[] tso(int size) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/tso?count=");
        builder.append(size);
        TS ts = null;
        try {
            ts = (TS)request(builder.toString(), "GET", null, TS.class);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("call tso api error " + e.getMessage());
        }
        if (ts == null) {
            return null;
        }
        return ts.timestamp;
    }

    public INodeFileMeta createFile(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime, long header) {
       StringBuffer builder = new StringBuffer();
       builder.append("/api/file/").append(id);
       INodeFileMeta meta = new INodeFileMeta(parent, id, name, permission, modificationTime, accessTime, header);
       try {
           request(builder.toString(), "PUT", meta, APIResponse.class);
       } catch (IOException e) {
           e.printStackTrace();
           LOG.error("call create file api error " +  e.getMessage());
       }
       return meta;
    }

    public INodeDirectoryMeta mkdir(long parent, long id,  byte[] name, long permission, long modificationTime, long accessTime) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(id);
        INodeDirectoryMeta meta = new INodeDirectoryMeta(parent, id, name, permission, modificationTime, accessTime);
        try {
            request(builder.toString(), "PUT", meta, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " +  e.getMessage());
        }
        return meta;
    }

    public INodeMeta getDirectoryChild(long directoryId, byte[] name) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId).append("/").append(name.toString());
        try {
            return (INodeMeta)request(builder.toString(), "GET", null, INodeMeta.class);
        }catch (IOException e) {
            e.printStackTrace();
            LOG.error("call get directory child api error "+ e.getMessage());
            return null;
        }
    }



    public INodeMeta[] getDirectoryChildren(long directoryId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory-children/").append(directoryId).append("/");
        try {
            INodeChildren children =  (INodeChildren)request(builder.toString(), "GET", null, INodeChildren.class);
            return children.response;
        }catch (IOException e) {
            e.printStackTrace();
            LOG.error("call get directory child api error "+ e.getMessage());
            return null;
        }
    }

    public INodeFileMeta getFile(long fileId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId);
        try {
            return (INodeFileMeta) request(builder.toString(), "GET", null, INodeFileMeta.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " +  e.getMessage());
        }
        return null;
    }

    public INodeDirectoryMeta getDirectory(long directoryId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId);
        try {
            return (INodeDirectoryMeta) request(builder.toString(), "GET", null, INodeDirectoryMeta.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " +  e.getMessage());
        }
        return null;
    }

    public BlockMeta getFileBlock(long fileId, int index) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId).append("/").append(index);
        try {
            return (BlockMeta) request(builder.toString(), "GET", null, BlockMeta.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " +  e.getMessage());
        }
        return null;
    }

    public BlockMeta addBlock(long fileId, long blockId, long generationTimestamp) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId).append("/").append(blockId);
        try {
            request(builder.toString(), "PUT", null, null);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call add file block api error " + e.getMessage());
        }
        return null;
    }

    @Override
    public BlockMeta[] updateBlocks(long fileId, BlockInfo[] blocks) {
        return new BlockMeta[0];
    }

    @Override
    public void truncateBlocks(long fileId, int size) {

    }

    @Override
    public BlockMeta updateBlock(long fileId, int atIndex, BlockInfo block) {
        return null;
    }

    @Override
    public void setParent(long newParentId, long oldParentId, long id) {
    }

    @Override
    public void removeDirectoryChild(long directoryId, byte[] name) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId).append("/").append(name.toString());
        try {
            request(builder.toString(), "DELETE", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call delete directory api error " +  e.getMessage());
        }
    }

    @Override
    public void removeDirectory(long directoryId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId);
        try {
            request(builder.toString(), "DELETE", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call delete directory api error " +  e.getMessage());
        }
    }

    @Override
    public void removeFile(long fileId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId);
        try {
            request(builder.toString(), "DELETE", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call delete file api error " +  e.getMessage());
        }
    }

    @Override
    public void update(INodeFileMeta meta) {
    }

    @Override
    public void update(INodeDirectoryMeta meta) {
    }
}
