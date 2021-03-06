package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.SequencerServer;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class SequencerClientTest extends AbstractClientTest {

    SequencerClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new SequencerServer(defaultServerContext()))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new SequencerClient();
        return new ImmutableSet.Builder<IClient>()
                .add(client)
                .build();
    }

    @Test
    public void canGetAToken()
            throws Exception {
        client.nextToken(Collections.<UUID>emptySet(), 1).get();
    }

    @Test
    public void tokensAreIncrementing()
            throws Exception {
        long token = client.nextToken(Collections.<UUID>emptySet(), 1).get().getToken();
        long token2 = client.nextToken(Collections.<UUID>emptySet(), 1).get().getToken();
        assertThat(token2)
                .isGreaterThan(token);
    }

    @Test
    public void checkTokenPositionWorks()
            throws Exception {
        long token = client.nextToken(Collections.<UUID>emptySet(), 1).get().getToken();
        long token2 = client.nextToken(Collections.<UUID>emptySet(), 0).get().getToken();
        assertThat(token)
                .isEqualTo(token2);
    }

    @Test
    public void perStreamTokensWork()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        client.nextToken(Collections.singleton(streamA), 1).get();
        long tokenA = client.nextToken(Collections.singleton(streamA), 1).get().getToken();
        long tokenA2 = client.nextToken(Collections.singleton(streamA), 0).get().getToken();
        assertThat(tokenA)
                .isEqualTo(tokenA2);
        long tokenB = client.nextToken(Collections.singleton(streamB), 0).get().getToken();
        assertThat(tokenB)
                .isNotEqualTo(tokenA2);
        long tokenB2 = client.nextToken(Collections.singleton(streamB), 1).get().getToken();
        long tokenB3 = client.nextToken(Collections.singleton(streamB), 0).get().getToken();
        assertThat(tokenB2)
                .isEqualTo(tokenB3);
        long tokenA3 = client.nextToken(Collections.singleton(streamA), 0).get().getToken();
        assertThat(tokenA3)
                .isEqualTo(tokenA2);
    }
}
