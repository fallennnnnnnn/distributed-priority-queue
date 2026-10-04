package com.queue.distributedpriorityqueue.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class MessageIdTest {

    @Test
    void writesAsShardDashOffset() {
        assertThat(new MessageId(3, 1234)).hasToString("3-1234");
    }

    @Test
    void parsesWhatItWrites() {
        MessageId id = new MessageId(12, 9_000_000_000L);

        assertThat(MessageId.parse(id.toString())).isEqualTo(id);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "7", "-7", "3-", "a-1", "1-b", "1-2-3", "1--2"})
    void rejectsMalformedText(String text) {
        assertThatIllegalArgumentException().isThrownBy(() -> MessageId.parse(text));
    }

    @Test
    void rejectsNegativeParts() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MessageId(-1, 0));
        assertThatIllegalArgumentException().isThrownBy(() -> new MessageId(0, -1));
    }
}
