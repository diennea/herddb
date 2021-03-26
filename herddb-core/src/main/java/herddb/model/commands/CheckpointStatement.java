package herddb.model.commands;

import herddb.model.Statement;

/**
 * TableSpace consistency check statement
 * @author lorenzobalzani
 */
public class CheckpointStatement extends Statement{

    public CheckpointStatement(String tableSpace) {
        super(tableSpace);
    }
}
