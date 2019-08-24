package herddb.mem;

import herddb.server.LocalNodeIdManager;
import java.io.IOException;
import java.nio.file.Path;

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
