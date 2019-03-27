package herddb.index.blink;

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import herddb.core.RandomPageReplacementPolicy;
import herddb.index.blink.BLink.SizeEvaluator;
import herddb.index.blink.BLinkTest.DummyBLinkIndexDataStorage;

/**
 * Tests on {@link BLink} about constant size retrieved from {@link SizeEvaluator}
 *
 * @author diego.salvi
 */
public class BLinkConstantSizeTest {

    /**
     * Dynamic sizing
     */
    private static class DummySizeEvaluator implements SizeEvaluator<Long,Long> {

        static final long KEY_CONSTANT_SIZE = 1;
        static final long VALUE_CONSTANT_SIZE = 1;

        @Override
        public long evaluateKey(Long key) {
            return KEY_CONSTANT_SIZE;
        }

        @Override
        public long evaluateValue(Long value) {
            return VALUE_CONSTANT_SIZE;
        }

        @Override
        public long evaluateAll(Long key, Long value) {
            return KEY_CONSTANT_SIZE + VALUE_CONSTANT_SIZE;
        }

        private static final Long POSITIVE_INF = Long.MAX_VALUE;

        @Override
        public Long getPosiviveInfinityKey() {
            return POSITIVE_INF;
        }

    }

    /**
     * Constant sizing
     */
    private static final class DummyConstantSizeEvaluator extends DummySizeEvaluator {

        @Override
        public long evaluateKey(Long key) {
            Assert.fail("Method evaluateKey souldn't be invoked");
            return super.evaluateKey(key);
        }

        @Override
        public long evaluateValue(Long value) {
            Assert.fail("Method evaluateValue souldn't be invoked");
            return super.evaluateKey(value);
        }

        @Override
        public boolean isKeySizeConstant() {
            return true;
        }

        @Override
        public long constantKeySize() {
            return KEY_CONSTANT_SIZE;
        }

        @Override
        public boolean isValueSizeConstant() {
            return true;
        }

        @Override
        public long constantValueSize() {
            return VALUE_CONSTANT_SIZE;
        }

    }

    /**
     * Check that 2 BLink generated with 2 different but equivalent {@link SizeEvaluator}s (one enforce
     * constant sizing the other one enforce dynamic sizing) report the same size.
     */
    @Test
    public void dynamicAndConstantSizeCheck() {

        long maxSize = 2048L;

        Function<SizeEvaluator<Long,Long>,Long> sizeEvaluation = (evaluator) -> {
            try (BLink<Long, Long> blink = new BLink<>(maxSize, evaluator, new RandomPageReplacementPolicy(3), new DummyBLinkIndexDataStorage<>())) {
                long size;
                long data = 0;
                while ((size = blink.getUsedMemory()) < maxSize) {
                    final Long v = Long.valueOf(data++);
                    blink.insert(v, v);
                }

                return size;
            }
        };


        long dynamicSize = sizeEvaluation.apply(new DummySizeEvaluator());
        long constantSize = sizeEvaluation.apply(new DummyConstantSizeEvaluator());

        Assert.assertEquals(dynamicSize, constantSize);
    }

}
