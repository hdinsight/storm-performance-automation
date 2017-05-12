package org.apache.storm.hdfs;

import java.lang.String;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.RetryPolicy;
import org.apache.storm.shade.com.google.common.primitives.Longs;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class ZookeeperClient {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);
	static CuratorFramework client;
	static RetryPolicy retryPolicy;
	String zkCounterPath;
    String zkLockPath;
	String zkConnStr;
    InterProcessSemaphoreMutex lock;

	public ZookeeperClient(String zkConnStr)
	{
		this.zkConnStr = zkConnStr;
		this.zkCounterPath = "/stormsyncpath";
        this.zkLockPath = "/stormlockpath";
        initializeZookeeperClient();
	}

    protected String getZkSyncPath()
    {
        return this.zkCounterPath;
    }

	public void initializeZookeeperClient()
	{
        this.retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient(this.zkConnStr, this.retryPolicy);
        this.client.start();
	}

    public boolean updatespoutstatistics(Long elapsedtimeinmillis, String path)
    {
        boolean updateSuccess = false;
        try
        {
            System.out.println("elapsedtimeinmillis"+elapsedtimeinmillis);
            if(client.checkExists().forPath(path) == null)
            {
                this.client.create().withMode(CreateMode.PERSISTENT).forPath(path, Longs.toByteArray(elapsedtimeinmillis));
                updateSuccess = true;
            }
            this.client.setData().forPath(path, valueToBytes(elapsedtimeinmillis));
        }
        catch(Exception e)
        {
            log("Exception encountered :" + e.getMessage());
        }
        return updateSuccess;
    }

    public boolean updatespoutstatistics(byte[] input_data, String path)
    {
        boolean updateSuccess = false;
        try
        {
            if(client.checkExists().forPath(path) == null)
            {
                this.client.create().withMode(CreateMode.PERSISTENT).forPath(path, input_data);
                updateSuccess = true;
            }
            this.client.setData().forPath(path, input_data);
        }
        catch(Exception e)
        {
            log("Exception encountered :" + e.getMessage());
        }
        return updateSuccess;
    }

    public boolean updatedSharedWorkProgressCounter()
    {       
            boolean updateSuccess = false;
            lock = new InterProcessSemaphoreMutex(this.client, this.zkLockPath);

            try
            {
                lock.acquire(30, TimeUnit.SECONDS);
            }
            catch(Exception e)
            {
                log("Exception in acquiring exclusive Lock on path (" + this.zkLockPath + ") : " + e.getMessage());
                return false;
            }

            try
            {
                if(client.checkExists().forPath(this.zkCounterPath) == null)
                {
                    this.client.create().withMode(CreateMode.PERSISTENT).forPath(zkCounterPath, valueToBytes(0L));
                }
                Long prevValue = getSharedCounterValue();
                if (prevValue != null)
                {
                    long newValue = prevValue + 1L;
                    log("Previous value :" + prevValue + ", New Value: " + newValue);
                    this.client.setData().forPath(zkCounterPath, valueToBytes(newValue));
                    updateSuccess = true;
                }
                lock.release();
            }
            catch(Exception e)
            {
                log("Exception encountered :" + e.getMessage());
            }

            return updateSuccess;
    }

    public Long getSharedCounterValue()
    {
        try
        {
            return bytesToValue(this.client.getData().forPath(this.zkCounterPath));
        }
        catch (Exception e)
        {
            log("Exception encountered while retrieving data from Zookeeper: " + e.getMessage());
            return null;
        }
    }

    public Long getData(String Path){
        try{
            return bytesToValue(this.client.getData().forPath(Path));
        }
        catch (Exception e)
        {
            log("Exception encountered while retrieving data from Zookeeper: " + e.getMessage());
            return null;
        }
    }
    public Map<Long,Long> getdata(String Path){
        try{
            ByteArrayInputStream data = new ByteArrayInputStream(this.client.getData().forPath(Path));
            ObjectInputStream in = new ObjectInputStream(data);
            Map<Long, Long> ackstats = (Map<Long, Long>) in.readObject();
            return ackstats;
        }
        catch (Exception e)
        {
            log("Exception encountered while retrieving data from Zookeeper: " + e.getMessage());
            return null;
        }
    }

	public byte[] valueToBytes(Long newValue)
    {
        byte[] newData = new byte[8];
        ByteBuffer wrapper = ByteBuffer.wrap(newData);
        wrapper.putLong(newValue);
        return newData;
    }

    public long bytesToValue(byte[] bytes)
    {
        long value = 0;
        for (int i = 0; i < bytes.length; i++)
        {
            value = (value << 8) + (bytes[i] & 0xff);
        }
        return value;
    }

    protected long getContextId()
	{
		return Thread.currentThread().getId();
	}

    protected void log(String msg)
	{
		LOG.info(getContextId() + " - " + msg);
	}

    protected void close()
    {
        this.client.close();
    }

    protected void deletePaths()
    {
        try
        {
            this.client.delete().forPath(this.zkCounterPath);
            this.client.delete().forPath(this.zkLockPath + "/locks");
            this.client.delete().forPath(this.zkLockPath + "/leases");
            this.client.delete().forPath(this.zkLockPath);
        }
        catch (Exception e)
        {
            log("Exception encountered when deleting ZK Paths: " + e.getMessage());
        }
    }
}
