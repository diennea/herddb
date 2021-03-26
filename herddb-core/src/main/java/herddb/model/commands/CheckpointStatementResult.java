package herddb.model;

/**
 * Statement result for a checkpoint operation
 * @author lorenzobalzani
 */
public class CheckpointStatementResult extends StatementExecutionResult {

    private final String message;
    private final boolean ok;

    public CheckpointStatementResult(boolean ok) {
        super(0);
        this.ok = ok;
        this.message = null;
    }

    public CheckpointStatementResult(boolean ok, String message) {
        super(0);
        this.ok = ok;
        this.message = message;
    }

    public String getMessage() {
        return this.message;
    }

    public boolean getOk() {
        return this.ok;
    }
}

