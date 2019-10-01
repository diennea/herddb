package herddb.sql;

import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.rel.core.JoinRelType;

public class CalciteEnumUtils {

    public static JoinType toLinq4jJoinType(JoinRelType joinRelType) {
        switch (joinRelType) {
            case INNER:
                return JoinType.INNER;
            case LEFT:
                return JoinType.LEFT;
            case RIGHT:
                return JoinType.RIGHT;
            case FULL:
                return JoinType.FULL;
            case SEMI:
                return JoinType.SEMI;
            case ANTI:
                return JoinType.ANTI;
            default:
                throw new IllegalStateException(
                        "Unable to convert " + joinRelType + " to Linq4j JoinType");
        }
    }
}
