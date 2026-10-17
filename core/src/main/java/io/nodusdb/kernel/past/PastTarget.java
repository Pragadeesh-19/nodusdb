package io.nodusdb.kernel.past;

import io.nodusdb.kernel.Token;

import java.util.Objects;

public sealed interface PastTarget permits PastTarget.AtToken, PastTarget.AtTime {

    record AtToken(Token token) implements PastTarget {

        public AtToken {
            Objects.requireNonNull(token, "token");
        }
    }

    record AtTime(long micros) implements PastTarget {

        public AtTime {
            if (micros < 0) {
                throw new IllegalArgumentException("a point in time is not before 1970: " + micros);
            }
        }
    }
}
