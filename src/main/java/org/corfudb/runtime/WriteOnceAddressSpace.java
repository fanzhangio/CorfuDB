/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.runtime;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.UnwrittenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * This is the write-once address space providing storage for the shared log.
 */
public interface WriteOnceAddressSpace
{
    /**
     * Writes an entry at a particular position. Throws an exception if
     * the entry is already written to.
     *
     * @param pos
     * @param bs
     */
    void write(long pos, BufferStack bs); //todo: throw exception

    /**
     * Reads the entry at a particular position. Throws exceptions if the entry
     * is unwritten or trimmed.
     *
     * @param pos
     */
    BufferStack read(long pos); //todo: throw exception

    /**
     * Trims the prefix of the address space before the passed in position.
     *
     * @param pos position before which all entries are trimmed
     */
    void prefixTrim(long pos);

    int getID();
}

class CacheMap<K,V> extends LinkedHashMap<K,V>
{
    int numentries;
    public CacheMap(int tnumentries)
    {
        super();
        numentries = tnumentries;
    }
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
    {
        if(size()>numentries)
            return true;
        return false;
    }
}

/**
 * Implements the write-once address space over the default Corfu shared log implementation.
 */
class CorfuLogAddressSpace implements WriteOnceAddressSpace
{
    Logger dbglog = LoggerFactory.getLogger(CorfuLogAddressSpace.class);

    int ID;

    final boolean caching = true;
    final int cachesize = 10000;
    CacheMap<Long, byte[]> cache = new CacheMap(cachesize);

    CorfuDBClient cl;
    org.corfudb.client.view.WriteOnceAddressSpace cwoas;

    public CorfuLogAddressSpace(CorfuDBClient tcl, int ID)
    {
        this.ID = ID;
        cl = tcl;
        cwoas = new org.corfudb.client.view.WriteOnceAddressSpace(cl);
    }

    public void write(long pos, BufferStack bs)
    {
        try
        {
            cwoas.write(pos, bs.flatten());
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }




    public BufferStack read(long pos)
    {
        dbglog.debug("Reading {}", pos);
        if(caching)
            synchronized(cache)
            {
                if(cache.containsKey(pos))
                    return new BufferStack(cache.get(pos));
            }
        byte[] ret = null;
        int retrycounter = 0;
        final int retrymax = 12;
        while(true)
        {
            try
            {
                long difftime = -1;

                long startts = System.currentTimeMillis();

                ret = cwoas.read(pos);
                long stopts = System.currentTimeMillis();
                difftime = stopts-startts;

                //for now, copy to a byte array and return
                dbglog.debug("read back {} bytes, took {} ms", ret.length, difftime);
                break;
            }
            //reactivate this code block once michael throws exceptions on unwritten
            catch (UnwrittenException uce)
            {
                //encountered a hole -- try again
//                System.out.println("Hole..." + pos);
                retrycounter++;
                if(retrycounter==retrymax) throw new RuntimeException("Encountered non-transient hole at " + pos + "...");
                try
                {
                    int sleepms = (int)Math.pow(2, retrycounter);
                    dbglog.debug("Encountered hole; sleeping for {} ms...", sleepms);
                    //exponential backoff
                    Thread.sleep(sleepms);
                }
                catch(InterruptedException e)
                {
                    //ignore
                }
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }

        }
        dbglog.debug("Done Reading {}", pos);
        if(caching)
            synchronized(cache)
            {
                cache.put(pos, ret);
            }
        return new BufferStack(ret);

    }

    @Override
    public void prefixTrim(long pos)
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public int getID()
    {
        return ID;
    }
}