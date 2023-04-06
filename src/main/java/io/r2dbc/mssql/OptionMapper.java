/*
 * Copyright 2020-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.io.File;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An utility data parser for {@link Option}.
 *
 * @author Mark Paluch
 * @since 0.8.3
 */
final class OptionMapper {

    private final ConnectionFactoryOptions options;

    private OptionMapper(ConnectionFactoryOptions options) {
        this.options = options;
    }

    /**
     * Construct a new {@link OptionMapper} given {@link ConnectionFactoryOptions}.
     *
     * @param options must not be {@code null}.
     * @return the option mapper.
     */
    public static OptionMapper create(ConnectionFactoryOptions options) {
        return new OptionMapper(options);
    }

    /**
     * Construct a new {@link Source} for a {@link Option}. Options without a value are not bound or mapped in the later stages of {@link Source}.
     *
     * @param option the option to apply.
     * @return the source object.
     */
    public Source<Object> from(Option<?> option) {

        if (this.options.hasOption(option)) {

            return new AvailableSource<>(() -> {
                return this.options.getRequiredValue(option);
            }, option.name());
        }

        return NullSource.instance();
    }

    /**
     * Construct a new {@link Source} for a {@link Option} using type inference. Options without a value are not bound or mapped in the later stages of {@link Source}.
     *
     * @param option the option to apply.
     * @param <T>    inferred option type.
     * @return the source object.
     */
    @SuppressWarnings("unchecked")
    public <T> Source<T> fromTyped(Option<T> option) {

        if (this.options.hasOption(option)) {

            return new AvailableSource<>(() -> {
                return (T) this.options.getRequiredValue(option);
            }, option.name());
        }

        return NullSource.instance();
    }

    /**
     * Parse an {@link Option} to boolean.
     */
    static boolean toBoolean(Object value) {

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }

        if (value instanceof String) {
            return Boolean.parseBoolean(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to boolean", value));
    }

    /**
     * Parse an ISO-8601 formatted {@link Option} to {@link Duration}.
     */
    static Duration toDuration(Object value) {

        if (value instanceof Duration) {
            return ((Duration) value);
        }

        if (value instanceof String) {
            return Duration.parse(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to Duration", value));
    }

    /**
     * Parse an {@link Option} to {@link File}.
     */
    static File toFile(Object value) {

        if (value instanceof File) {
            return (File) value;
        }

        if (value instanceof String) {
            return new File(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to File", value));
    }

    /**
     * Parse an {@link Option} to int.
     */
    static int toInteger(Object value) {

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to integer", value));
    }

    /**
     * Parse an {@link Option} to {@link Predicate}.
     */
    @SuppressWarnings("unchecked")
    static Predicate<String> toStringPredicate(Object value) {

        if (value instanceof Predicate) {
            return (Predicate) value;
        }

        if (value instanceof Boolean) {
            boolean choice = (boolean) value;
            return s -> choice;
        }

        if (value instanceof String) {
            String stringValue = value.toString();
            if ("true".equalsIgnoreCase(stringValue) || "false".equalsIgnoreCase(stringValue)) {
                return toStringPredicate(Boolean.parseBoolean(stringValue));
            } else {

                try {
                    Object predicate = Class.forName(stringValue).getDeclaredConstructor().newInstance();
                    if (predicate instanceof Predicate) {
                        return toStringPredicate(predicate);
                    } else {
                        throw new IllegalArgumentException("Value '" + value + "' must be an instance of Predicate");
                    }
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
                }
            }
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to Predicate", value));
    }

    /**
     * Parse an {@link Option} to a {@link PreparedStatementCache}.
     */
    static PreparedStatementCache toPreparedStatementCache(Object value) {
        if (value instanceof PreparedStatementCache) {
            return (PreparedStatementCache) value;
        }

        if (value instanceof Integer) {
            return toPreparedStatementCache((Integer) value);
        }

        if (value instanceof String) {
            return toPreparedStatementCache((String) value);
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to PreparedStatementCache", value));
    }

    /**
     * Parse an {@link Option} to {@link UUID}.
     */
    static UUID toUuid(Object value) {

        if (value instanceof UUID) {
            return (UUID) value;
        }

        if (value instanceof String) {
            return UUID.fromString(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s to UUID", value));
    }

    private static PreparedStatementCache toPreparedStatementCache(Integer value) {
        if (value < 0) {
            return new IndefinitePreparedStatementCache();
        } else if (value == 0) {
            return new NoPreparedStatementCache();
        } else {
            return new LRUPreparedStatementCache(value);
        }
    }

    private static PreparedStatementCache toPreparedStatementCache(String value) {
        try {
            Integer number = Integer.parseInt(value);
            return toPreparedStatementCache(number);
        } catch (NumberFormatException ignore) {
            // ignore - value is not a number
        }

        try {
            Object cache = Class.forName(value).getDeclaredConstructor().newInstance();
            if (cache instanceof PreparedStatementCache) {
                return (PreparedStatementCache) cache;
            }
            throw new IllegalArgumentException("Value '" + value + "' must be an instance of PreparedStatementCache");
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
        }
    }

    public interface Source<T> {

        /**
         * Return an mapped version of the source changed via the given mapping function.
         *
         * @param <R>             the resulting type
         * @param mappingFunction the mapping function to apply
         * @return a new adapted source instance
         */
        <R> Source<R> map(Function<Object, R> mappingFunction);

        /**
         * Complete the mapping by passing any non-filtered value to the specified
         * consumer.
         *
         * @param consumer the consumer that should accept the value if it's not been
         *                 filtered
         */
        Otherwise to(Consumer<T> consumer);

        /**
         * Complete the mapping by passing any non-filtered value to the specified
         * consumer.
         *
         * @param consumer the runnable that should be invoked.
         */
        Otherwise to(Runnable consumer);

    }

    public interface Otherwise {

        /**
         * Invoked if the previous {@link Source} outcome did not match.
         *
         * @param consumer the runnable that should be invoked.
         */
        void otherwise(Runnable consumer);

    }

    private enum Otherwises implements Otherwise {

        NONE {
            @Override
            public void otherwise(Runnable consumer) {
                // no-op
            }
        }, FALLBACK {
            @Override
            public void otherwise(Runnable consumer) {
                consumer.run();
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private enum NullSource implements Source<Object> {

        INSTANCE;

        public static <T> Source<T> instance() {
            return (Source) INSTANCE;
        }

        @Override
        public <R> Source<R> map(Function<Object, R> mappingFunction) {
            return (Source) this;
        }

        @Override
        public Otherwise to(Consumer<Object> consumer) {
            return Otherwises.FALLBACK;
        }

        @Override
        public Otherwise to(Runnable consumer) {
            return Otherwises.FALLBACK;
        }
    }

    private static class AvailableSource<T> implements Source<T> {

        private final Supplier<T> supplier;

        private final String optionName;

        private AvailableSource(Supplier<T> supplier, String optionName) {
            this.supplier = supplier;
            this.optionName = optionName;
        }

        /**
         * Return an mapped version of the source changed via the given mapping function.
         *
         * @param <R>             the resulting type.
         * @param mappingFunction the mapping function to apply.
         * @return a new mapped source instance.
         */
        @Override
        public <R> Source<R> map(Function<Object, R> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "Mapping function must not be null");

            Supplier<R> supplier = () -> mappingFunction.apply(this.supplier.get());

            return new AvailableSource<>(supplier, this.optionName);
        }

        /**
         * Complete the mapping by passing any non-filtered value to the specified
         * consumer.
         *
         * @param consumer the consumer that should accept the value.
         */
        @Override
        public Otherwise to(Consumer<T> consumer) {
            Assert.requireNonNull(consumer, "Consumer must not be null");
            try {
                T value = this.supplier.get();

                if (value != null) {
                    consumer.accept(value);
                    return Otherwises.NONE;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Cannot assign option %s", this.optionName), e);
            }

            return Otherwises.FALLBACK;
        }

        @Override
        public Otherwise to(Runnable consumer) {
            return to(ignore -> consumer.run());
        }

    }

}
