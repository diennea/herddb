package herddb.mem;

import java.io.IOException;
import java.nio.file.Path;

import herddb.server.LocalNodeIdManager;

/**
 * In memory LocalNodeIdManager, for tests
 *
 * @author diego.salvi
 */
public class MemoryLocalNodeIdManager extends LocalNodeIdManager {

    public MemoryLocalNodeIdManager(Path dataPath) {
        super(dataPath);
    }

    @Override
    public void persistLocalNodeId(String nodeId) throws IOException {
        /* NOP */
    }

}
