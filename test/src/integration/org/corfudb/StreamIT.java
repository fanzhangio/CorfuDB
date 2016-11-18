package org.corfudb;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.StreamView;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {
    static String layoutServers;

    @BeforeClass
    static public void getLayoutServers() throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("CorfuDB.properties");
        Properties properties = new Properties();
        properties.load(input);

        layoutServers = (String) properties.get("layoutServers");
    }

    @Test
    public void SimpleStreamTest() throws Exception {

        CorfuRuntime rt = new CorfuRuntime(layoutServers).connect();
        rt.setCacheDisabled(true);

        UUID streamId = UUID.nameUUIDFromBytes("stream1".getBytes());

        StreamView s1 = rt.getStreamsView().get(streamId);

        // Verify that the stream is empty
        assertThat(s1.check()).isEqualTo(-1);


        // Generate and write random data. Write 100 entries, each entry is 2mb
        int entrySize = 2000;
        int numEntries = 100;
        byte[][] data = new byte[numEntries][entrySize];

        Random rand = new Random();

        for(int x = 0; x < numEntries; x++) {
            rand.nextBytes(data[x]);
            s1.write(data[x]);
        }

        // Read back the data and verify it is correct
        for(int x = 0; x < numEntries; x++) {
            LogData entry = s1.read(x);
            byte[] tmp = (byte[]) entry.getPayload(rt);

            Thread.sleep(100);
            assertThat(tmp).isEqualTo(data[x]);
        }
    }
}