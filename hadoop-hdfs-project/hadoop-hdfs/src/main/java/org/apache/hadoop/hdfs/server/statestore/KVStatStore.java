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
import org.apache.http.entity.ByteArrayEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;

import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;

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

    public static class INodeChildren {
        public INodeMeta[] response;
    }

    private <T> Object request(String url, String method, Object body, Class<T> valueType) throws IOException {
        RequestBuilder builder = RequestBuilder.create(method);
        builder.setUri(url);
        if (body != null) {
            builder.setEntity(new ByteArrayEntity(new ObjectMapper().setPropertyNamingStrategy(
                    PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES).writeValueAsBytes(body)));
        }
        HttpRequest request = builder.build();
        HttpResponse response = httpClient.execute(HttpHost.create(endpoint),request);
        int statusCode = response.getStatusLine().getStatusCode();
        LOG.trace("request api " + url + " response code " + statusCode);
        if (statusCode == 404) {
            response.getEntity().consumeContent();
            return null;
        } else if (statusCode >= 400) {
            ObjectMapper mapper = new ObjectMapper().setPropertyNamingStrategy(
                    PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
            return mapper.readValue(response.getEntity().getContent(), APIResponse.class);
        } else if (valueType != null && valueType != APIResponse.class) {
            ObjectMapper mapper = new ObjectMapper().setPropertyNamingStrategy(
                    PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
            return mapper.readValue(response.getEntity().getContent(), valueType);
        } else {
            response.getEntity().consumeContent();
            return null;
        }
    }

    public long tso() {
        long[] ts = tso(1);
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
            ts = (TS) request(builder.toString(), "GET", null, TS.class);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("call tso api error " + e.getMessage());
        }
        if (ts == null) {
            return null;
        }
        return ts.timestamp;
    }

    public INodeFileMeta createFile(INodeFileMeta meta) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(meta.id);
        try {
            request(builder.toString(), "PUT", meta, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " + e.getMessage());
        }
        return meta;
    }

    public INodeFileMeta createFile(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime, long header, String clientName, String clientMachine) {
        return createFile(new INodeFileMeta(parent, id, name, permission, modificationTime, accessTime, header, clientName, clientMachine));
    }

    public INodeDirectoryMeta mkdir(long parent, long id, byte[] name, long permission, long modificationTime, long accessTime) {
        return mkdir(new INodeDirectoryMeta(parent, id, name, permission, modificationTime, accessTime));
    }

    public INodeDirectoryMeta mkdir(INodeDirectoryMeta meta) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(meta.id);
        try {
            request(builder.toString(), "PUT", meta, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call create file api error " + e.getMessage());
        }
        return meta;
    }

    public INodeMeta getDirectoryChild(long directoryId, byte[] name) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId).append("/").append(new String(name, UTF_8));
        try {
            return (INodeMeta) request(builder.toString(), "GET", null, INodeMeta.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call get directory child api error " + e.getMessage());
            return null;
        }
    }
    
    public INodeMeta[] getDirectoryChildren(long directoryId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory-children/").append(directoryId);
        try {
            INodeChildren children = (INodeChildren) request(builder.toString(), "GET", null, INodeChildren.class);
            return children.response;
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call get directory child api error " + e.getMessage());
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
            LOG.error("call create file api error " + e.getMessage());
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
            LOG.error("call create file api error " + e.getMessage());
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
            LOG.error("call create file api error " + e.getMessage());
        }
        return null;
    }

    public BlockMeta addBlock(long fileId, long blockId, long generationTimestamp) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId).append("/").append(blockId).append("?generation_time=").append(generationTimestamp);
        try {
            request(builder.toString(), "PUT", null, null);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call add file block api error " + e.getMessage());
        }
        return null;
    }

    @Override
    public BlockMeta[] updateBlocks(long fileId, BlockMeta[] blocks) {
        BlockMeta[] blks = new BlockMeta[blocks.length];
        for (int i =0; i < blocks.length; i++) {
            blks[i] = updateBlock(fileId, blocks[i]);
        }
        return  blks;
    }

    @Override
    public void truncateBlocks(long fileId, int size) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file-truncate/").append(fileId).append("/").append(size);
        try {
            request(builder.toString(), "PUT", null, null);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call add file block api error " + e.getMessage());
        }
    }

    @Override
    public BlockMeta updateBlock(long fileId, BlockMeta block) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(fileId).append("/").append(block.id);
        try {
            return (BlockMeta)request(builder.toString(), "POST", block, BlockMeta.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call add file block api error " + e.getMessage());
        }
        return  null;
    }

    @Override
    public void rename(long oldParentId, INodeMeta node) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(node.id).append("/").append(oldParentId).append("/").append(node.parentId);
        try {
            request(builder.toString(), "PUT", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call delete directory api error " + e.getMessage());
        }
    }

    @Override
    public void removeDirectoryChild(long directoryId, byte[] name) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/directory/").append(directoryId).append("/").append(new String(name, UTF_8));
        try {
            request(builder.toString(), "DELETE", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call delete directory api error " + e.getMessage());
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
            LOG.error("call delete directory api error " + e.getMessage());
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
            LOG.error("call delete file api error " + e.getMessage());
        }
    }

    @Override
    public void update(INodeFileMeta meta) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/file/").append(meta.id);
        try {
            request(builder.toString(), "POST", meta, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call update file api error " + e.getMessage());
        }
    }

    @Override
    public void update(INodeDirectoryMeta meta) {
    }

    @Override
    public void addBlockStorage(String id, String dataNodeId, String storageId) {
        StringBuffer builder = new StringBuffer();
        builder.append("/api/block/storage/").append(dataNodeId).append("/").append(storageId);
        try {
            request(builder.toString(), "PUT", null, APIResponse.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("call update file api error " + e.getMessage());
        }
    }
}
