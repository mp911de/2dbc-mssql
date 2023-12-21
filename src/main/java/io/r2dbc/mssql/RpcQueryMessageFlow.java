/*
 * Copyright 2018-2023 the original author or authors.
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

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.mssql.RpcQueryMessageFlow.CursorState.Phase;
import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.RpcDirection;
import io.r2dbc.mssql.message.ClientMessage;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.TransactionDescriptor;
import io.r2dbc.mssql.message.token.*;
import io.r2dbc.mssql.message.type.Collation;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.mssql.util.Operators;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.processing.Completion;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.r2dbc.mssql.util.PredicateUtils.or;

/**
 * Query message flow using cursors. The cursored query message flow uses {@link RpcRequest RPC} calls to open, fetch and close cursors.
 * <p>
 * Commands require deferred creation because {@link Client} can be used concurrently and we must fetch the latest state (e.g. {@link TransactionDescriptor}) to issue a command with the appropriate
 * state.
 *
 * @author Mark Paluch
 * @see RpcRequest
 */
final class RpcQueryMessageFlow {

    private static final Predicate<Message> FILTER_PREDICATE = or(RowToken.class::isInstance,
        ColumnMetadataToken.class::isInstance,
        ReturnValue.class::isInstance,
        DoneInProcToken.class::isInstance,
        IntermediateCount.class::isInstance,
        AbstractInfoToken.class::isInstance,
        Completion.class::isInstance,
        AbstractDoneToken::isAttentionAck);

    private static final Logger logger = Loggers.getLogger(RpcQueryMessageFlow.class);

    static final RpcRequest.OptionFlags NO_METADATA = RpcRequest.OptionFlags.empty().disableMetadata();

    // Constants for server-cursored result sets.
    // See the Engine Cursors Functional Specification for details.
    static final int FETCH_FIRST = 1;

    static final int FETCH_NEXT = 2;

    static final int FETCH_PREV = 4;

    static final int FETCH_LAST = 8;

    static final int FETCH_ABSOLUTE = 16;

    static final int FETCH_RELATIVE = 32;

    static final int FETCH_REFRESH = 128;

    static final int FETCH_INFO = 256;

    static final int FETCH_PREV_NOADJUST = 512;

    // Scroll options and concurrency options lifted out
    // of the the Yukon cursors spec for sp_cursoropen.
    final static int SCROLLOPT_KEYSET = 1;

    final static int SCROLLOPT_DYNAMIC = 2;

    final static int SCROLLOPT_FORWARD_ONLY = 4;

    final static int SCROLLOPT_STATIC = 8;

    final static int SCROLLOPT_FAST_FORWARD = 16;

    static final int SCROLLOPT_PARAMETERIZED_STMT = 4096;

    static final int CCOPT_READ_ONLY = 1;

    static final int CCOPT_ALLOW_DIRECT = 8192;

    /**
     * Execute a direct query with parameters.
     *
     * @param client the {@link Client} to exchange messages with.
     * @param query  the query to execute.
     * @return the messages received in response to this exchange.
     */
    static Flux<Message> exchange(Client client, String query, Binding binding) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");


        CursorState state = new CursorState();
        state.directMode = true;

        Flux<Message> exchange = client.exchange(Mono.fromSupplier(() -> spExecuteSql(query, binding, client.getRequiredCollation(), client.getTransactionDescriptor())), DoneProcToken::isDone);
        OnCursorComplete cursorComplete = new OnCursorComplete();

        Flux<Message> messages = exchange //
            .<Message>handle((message, sink) -> {

                state.update(message);

                handleMessage(client, 0, state, message, sink, cursorComplete, true);
            })
            .filter(FILTER_PREDICATE)
            .doOnCancel(cursorComplete);

        return messages.doOnSubscribe(subscription -> {
            QueryLogger.logQuery(client.getContext(), query);
        })
            .transform(it -> Operators.discardOnCancel(it, state::cancel).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)).takeUntilOther(cursorComplete.takeUntil());
    }

    /**
     * Execute a cursored query.
     *
     * @param client    the {@link Client} to exchange messages with.
     * @param codecs    the codecs to decode {@link ReturnValue}s from RPC calls.
     * @param query     the query to execute.
     * @param fetchSize the number of rows to fetch. TODO: Try to determine fetch size from current demand and apply demand function.
     * @return the messages received in response to this exchange.
     */
    static Flux<Message> exchange(Client client, Codecs codecs, String query, int fetchSize) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        Sinks.Many<ClientMessage> outbound = Sinks.many().unicast().onBackpressureBuffer();

        CursorState state = new CursorState();

        Flux<Message> exchange = client.exchange(Flux.defer(() -> {
            outbound.emitNext(spCursorOpen(query, client.getRequiredCollation(), client.getTransactionDescriptor()), Sinks.EmitFailureHandler.FAIL_FAST);
            return outbound.asFlux();
        }), isFinalToken(state));

        OnCursorComplete cursorComplete = new OnCursorComplete();

        Flux<Message> messages = exchange //
            .<Message>handle((message, sink) -> {

                boolean emit = true;
                if (message.getClass() == ReturnValue.class) {

                    ReturnValue returnValue = (ReturnValue) message;

                    // cursor Id
                    if (returnValue.getOrdinal() == 0) {
                        state.cursorId = parseCursorId(codecs, state, returnValue);
                    }

                    // skip spCursorOpen OUT
                    if (returnValue.getOrdinal() < 5) {
                        returnValue.release();
                        emit = false;
                    }
                }

                state.update(message);

                handleMessage(client, fetchSize, outbound, state, message, sink, cursorComplete, emit);
            })
            .filter(FILTER_PREDICATE);

        return messages.doOnSubscribe(subscription -> {
            QueryLogger.logQuery(client.getContext(), query);
        })
            .transform(it -> Operators.discardOnCancel(it, state::cancel).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)).takeUntilOther(cursorComplete.takeUntil());
    }

    /**
     * Execute a cursored query with RPC parameters.
     *
     * @param statementCache the {@link PreparedStatementCache} to keep track of prepared statement handles.
     * @param client         the {@link Client} to exchange messages with.
     * @param codecs         the codecs to decode {@link ReturnValue}s from RPC calls.
     * @param query          the query to execute.
     * @param binding        parameter bindings.
     * @param fetchSize      the number of rows to fetch. TODO: Try to determine fetch size from current demand and apply demand function.
     * @return the messages received in response to this exchange.
     * @throws IllegalArgumentException when {@link Client} or {@code query} is {@code null}.
     */
    static Flux<Message> exchange(PreparedStatementCache statementCache, Client client, Codecs codecs, String query, Binding binding, int fetchSize) {

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(query, "Query must not be null");

        Sinks.Many<ClientMessage> outbound = Sinks.many().unicast().onBackpressureBuffer();
        int handle = statementCache.getHandle(query, binding);

        AtomicBoolean retryReprepare = new AtomicBoolean(true);
        AtomicBoolean needsPrepare = new AtomicBoolean(false);

        Flux<ClientMessage> messageProducer;

        if (handle == PreparedStatementCache.UNPREPARED) {
            messageProducer = Flux.defer(() -> {
                outbound.emitNext(spCursorPrepExec(PreparedStatementCache.UNPREPARED, query, binding, client.getRequiredCollation(),
                        client.getTransactionDescriptor()), Sinks.EmitFailureHandler.FAIL_FAST);
                return outbound.asFlux();
            });

            needsPrepare.set(true);
        } else {
            messageProducer = Flux.defer(() -> {
                outbound.emitNext(spCursorExec(handle, binding, client.getTransactionDescriptor()), Sinks.EmitFailureHandler.FAIL_FAST);
                return outbound.asFlux();
            });
            needsPrepare.set(false);
        }

        CursorState state = new CursorState();
        Flux<Message> exchange = client.exchange(messageProducer, isFinalToken(state));
        OnCursorComplete cursorComplete = new OnCursorComplete();

        Flux<Message> messages = exchange //
            .<Message>handle((message, sink) -> {

                boolean emit = true;
                if (message.getClass() == ReturnValue.class) {

                    ReturnValue returnValue = (ReturnValue) message;

                    emit = handleSpCursorReturnValue(statementCache, codecs, query, binding, state, needsPrepare.get(), returnValue);

                    if (!emit) {
                        returnValue.release();
                    }
                }

                state.update(message);

                if (message instanceof ErrorToken) {
                    if (retryReprepare.compareAndSet(true, false)) {
                        logger.debug("Prepared statement no longer valid: {}", handle);
                        state.update(Phase.PREPARE_RETRY);
                    }
                }

                if (state.phase == Phase.PREPARE_RETRY) {
                    emit = false;
                }

                if (DoneProcToken.isDone(message) && state.phase == Phase.PREPARE_RETRY) {

                    logger.debug("Attempting to re-prepare statement: {}", query);
                    needsPrepare.set(true);
                    state.update(Phase.NONE);
                    outbound.emitNext(spCursorPrepExec(PreparedStatementCache.UNPREPARED, query, binding, client.getRequiredCollation(),
                            client.getTransactionDescriptor()), Sinks.EmitFailureHandler.FAIL_FAST);
                    return;
                }

                handleMessage(client, fetchSize, outbound, state, message, sink, cursorComplete, emit);
            })
            .filter(FILTER_PREDICATE);

        return messages.doOnSubscribe(subscription -> {
            QueryLogger.logQuery(client.getContext(), query);
        })
            .transform(it -> Operators.discardOnCancel(it, state::cancel).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)).takeUntilOther(cursorComplete.takeUntil());
    }

    /**
     * Check whether the error indicates a prepared statement requiring reprepare.
     * <p>
     * <ul><li>586: The prepared statement handle %d is not valid in this context. Please verify that current database, user
     * default schema ANSI_NULLS and QUOTED_IDENTIFIER set options are not changed since the handle is prepared.</li>
     * <li>8179: Could not find prepared statement with handle %d.</li>
     * </ul>
     *
     * @param errorNumber
     * @return
     */
    private static boolean isPreparedStatementNotFound(long errorNumber) {
        return errorNumber == 8179 || errorNumber == 586 || errorNumber == 8144  || errorNumber == 8178;
    }

    private static boolean handleSpCursorReturnValue(PreparedStatementCache statementCache, Codecs codecs, String query, Binding binding, CursorState state, boolean needsPrepare,
                                                     ReturnValue returnValue) {

        // cursor Id
        if (returnValue.getOrdinal() == 1) {
            state.cursorId = parseCursorId(codecs, state, returnValue);
        }

        if (needsPrepare) {

            // prepared statement handle
            if (returnValue.getOrdinal() == 0) {

                int preparedStatementHandle = codecs.decode(returnValue.getValue(), returnValue.asDecodable(), Integer.class);
                logger.debug("Prepared statement with handle: {}", preparedStatementHandle);
                statementCache.putHandle(preparedStatementHandle, query, binding);
            }

            // skip spCursorPrepExec OUT
            return returnValue.getOrdinal() >= 7;
        } else {
            // skip spCursorExec OUT
            return returnValue.getOrdinal() >= 5;
        }
    }

    private static int parseCursorId(Codecs codecs, CursorState state, ReturnValue returnValue) {

        Integer cursorId = codecs.decode(returnValue.getValue(), returnValue.asDecodable(), Integer.class);
        logger.debug("CursorId: {}", cursorId);
        return cursorId;
    }

    private static void handleMessage(Client client, int fetchSize, CursorState state, Message message, SynchronousSink<Message> sink,
                                      Runnable onCursorComplete, boolean emit) {
        handleMessage(client, fetchSize, it -> {
            throw new UnsupportedOperationException("Cannot accept subsequent messages");
        }, state, message, sink, onCursorComplete, emit);
    }

    private static void handleMessage(Client client, int fetchSize, Sinks.Many<ClientMessage> requests, CursorState state, Message message, SynchronousSink<Message> sink,
                                      Runnable onCursorComplete, boolean emit) {
        handleMessage(client, fetchSize, t -> requests.emitNext(t, Sinks.EmitFailureHandler.FAIL_FAST), state, message, sink, onCursorComplete, emit);
    }

    private static void handleMessage(Client client, int fetchSize, Consumer<ClientMessage> requests, CursorState state, Message message, SynchronousSink<Message> sink,
                                      Runnable onCursorComplete, boolean emit) {

        if (message instanceof ColumnMetadataToken && !((ColumnMetadataToken) message).hasColumns()) {
            return;
        }

        if (message instanceof AbstractInfoToken) {

            // direct mode
            if (((AbstractInfoToken) message).getNumber() == 16954) {
                state.directMode = true;
            }
        }

        if (message instanceof DoneInProcToken) {

            DoneInProcToken doneToken = (DoneInProcToken) message;
            state.hasMore = doneToken.hasMore();

            if (!state.directMode) {

                if (state.phase == Phase.FETCHING && doneToken.hasCount()) {
                    sink.next(new IntermediateCount(doneToken));
                }
                return;
            }

            sink.next(doneToken);
            return;
        }

        if (AbstractDoneToken.isAttentionAck(message)) {

            state.update(Phase.CLOSED);
            sink.next(message);
            return;
        }

        if (!(message instanceof DoneProcToken)) {

            if (emit) {
                sink.next(message);
            }
            return;
        }

        if (state.hasSeenError) {
            state.update(Phase.ERROR);
        }

        if (DoneProcToken.isDone(message)) {
            onDone(client, fetchSize, requests, state, onCursorComplete);
        }
    }

    static void onDone(Client client, int fetchSize, Consumer<ClientMessage> requests, CursorState state, Runnable completion) {

        Phase phase = state.phase;

        if (isFinalState(state)) {

            completion.run();

            state.update(Phase.CLOSED);
            return;
        }

        if (phase == Phase.NONE || phase == Phase.FETCHING) {

            if (((state.hasMore && phase == Phase.NONE) || state.hasSeenRows) && state.wantsMore()) {
                if (phase == Phase.NONE) {
                    state.update(Phase.FETCHING);
                }
                requests.accept(spCursorFetch(state.cursorId, FETCH_NEXT, fetchSize, client.getTransactionDescriptor()));
            } else {
                state.update(Phase.CLOSING);
                // TODO: spCursorClose should happen also if a subscriber cancels its subscription.
                requests.accept(spCursorClose(state.cursorId, client.getTransactionDescriptor()));
            }

            state.hasSeenRows = false;
        }
    }

    private static Predicate<Message> isFinalToken(CursorState state) {

        return message -> {

            if (!DoneProcToken.isDone(message)) {
                return false;
            }

            return isFinalState(state);
        };
    }

    private static boolean isFinalState(CursorState state) {

        Phase phase = state.phase;

        if (phase == Phase.NONE || phase == Phase.FETCHING) {

            if (state.cursorId == 0) {
                return true;
            }
        }

        return phase == Phase.ERROR || phase == Phase.CLOSING || phase == Phase.CLOSED;
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_ExecuteSql} to execute a SQL statement that returns directly results.
     *
     * @param query                 the query to execute.
     * @param binding               bound parameters
     * @param collation             the database collation.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen}.
     * @throws IllegalArgumentException when {@code query}, {@link Collation}, or {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spExecuteSql(String query, Binding binding, Collation collation, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(query, "Query must not be null");
        Assert.requireNonNull(collation, "Collation must not be null");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_ExecuteSql) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.IN, collation, query) //
            .withParameter(RpcDirection.IN, collation, binding.getFormalParameters()); // formal parameter defn

        binding.forEach((name, parameter) -> {
            builder.withNamedParameter(parameter.rpcDirection, name, parameter.encoded);
        });

        return builder.build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen} to execute a SQL statement that returns a cursor.
     *
     * @param query                 the query to execute.
     * @param collation             the database collation.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorOpen}.
     * @throws IllegalArgumentException when {@code query}, {@link Collation}, or {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spCursorOpen(String query, Collation collation, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(query, "Query must not be null");
        Assert.requireNonNull(collation, "Collation must not be null");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        int resultSetScrollOpt = SCROLLOPT_FORWARD_ONLY;
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorOpen) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, collation, query)
            .withParameter(RpcDirection.IN, resultSetScrollOpt)  // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0) // rowcount
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch} to fetch {@code rowCount} from the given {@literal cursor}.
     *
     * @param cursor                the cursor Id.
     * @param fetchType             the type of fetch operation (first, next, …).
     * @param rowCount              number of rows to fetch
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     * @throws IllegalArgumentException when {@code rowCount} is less than zero.
     */
    static RpcRequest spCursorFetch(int cursor, int fetchType, int rowCount, TransactionDescriptor transactionDescriptor) {

        Assert.isTrue(rowCount >= 0, "Row count must be greater or equal to zero");
        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorFetch) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withOptionFlags(NO_METADATA) //
            .withParameter(RpcDirection.IN, cursor) // cursor
            .withParameter(RpcDirection.IN, fetchType) // fetch type
            .withParameter(RpcDirection.IN, 0)  // startRow
            .withParameter(RpcDirection.IN, rowCount) // numRows
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorClose} release server resources.
     *
     * @param cursor                the cursor Id.
     * @param transactionDescriptor transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     * @throws IllegalArgumentException when {@link TransactionDescriptor} is {@code null}.
     */
    static RpcRequest spCursorClose(int cursor, TransactionDescriptor transactionDescriptor) {

        Assert.requireNonNull(transactionDescriptor, "TransactionDescriptor must not be null");

        return RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorClose) //
            .withTransactionDescriptor(transactionDescriptor) //
            .withParameter(RpcDirection.IN, cursor) // cursor
            .build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorPrepare} to prepare and execute a {@code query}.
     *
     * @param preparedStatementHandle handle to a previously prepared statement. This call un-prepares a previously prepared statement.
     * @param query                   the query to execute.
     * @param binding                 bound parameters
     * @param collation               the database collation.
     * @param transactionDescriptor   transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorPrepExec(int preparedStatementHandle, String query, Binding binding, Collation collation, TransactionDescriptor transactionDescriptor) {

        int resultSetScrollOpt = SCROLLOPT_FORWARD_ONLY | (binding.isEmpty() ? 0 : SCROLLOPT_PARAMETERIZED_STMT);
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorPrepExec) //
            .withTransactionDescriptor(transactionDescriptor) //

            // <prepared handle>
            // IN (reprepare): Old handle to unprepare before repreparing
            // OUT: The newly prepared handle
            .withParameter(RpcDirection.OUT, preparedStatementHandle)
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, collation, binding.getFormalParameters()) // formal parameter defn
            .withParameter(RpcDirection.IN, collation, query) // statement
            .withParameter(RpcDirection.IN, resultSetScrollOpt) // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0);// rowcount

        binding.forEach((name, parameter) -> {
            builder.withNamedParameter(parameter.rpcDirection, name, parameter.encoded);
        });

        return builder.build();
    }

    /**
     * Creates a {@link RpcRequest} for {@link RpcRequest#Sp_CursorExecute} to and execute prepared statement.
     *
     * @param preparedStatementHandle handle to a previously prepared statement.
     * @param binding                 bound parameters
     * @param transactionDescriptor   transaction descriptor.
     * @return {@link RpcRequest} for {@link RpcRequest#Sp_CursorFetch}.
     */
    static RpcRequest spCursorExec(int preparedStatementHandle, Binding binding, TransactionDescriptor transactionDescriptor) {

        Assert.isTrue(preparedStatementHandle != PreparedStatementCache.UNPREPARED, "Invalid PreparedStatement handle");

        int resultSetScrollOpt = SCROLLOPT_FORWARD_ONLY;
        int resultSetCCOpt = CCOPT_READ_ONLY | CCOPT_ALLOW_DIRECT;

        RpcRequest.Builder builder = RpcRequest.builder() //
            .withProcId(RpcRequest.Sp_CursorExecute) //
            .withTransactionDescriptor(transactionDescriptor) //

            // <prepared handle>
            // IN (reprepare): Old handle to unprepare before repreparing
            // OUT: The newly prepared handle
            .withParameter(RpcDirection.IN, preparedStatementHandle)
            .withParameter(RpcDirection.OUT, 0) // cursor
            .withParameter(RpcDirection.IN, resultSetScrollOpt) // scrollopt
            .withParameter(RpcDirection.IN, resultSetCCOpt) // ccopt
            .withParameter(RpcDirection.OUT, 0);// rowcount

        binding.forEach((name, parameter) -> {
            builder.withNamedParameter(parameter.rpcDirection, name, parameter.encoded);
        });

        return builder.build();
    }

    /**
     * Cursoring state.
     */
    static class CursorState {

        volatile int cursorId;

        // hasMore flag from the DoneInProc token
        volatile boolean hasMore;

        // hasMore typically reports true, but we need to check whether we've seen rows to determine whether to end cursoring.
        volatile boolean hasSeenRows;

        volatile boolean hasSeenError;

        volatile boolean directMode;

        volatile boolean cancelRequested;

        volatile ErrorToken errorToken;

        Phase phase = Phase.NONE;

        boolean wantsMore() {
            return !this.cancelRequested;
        }

        void cancel() {
            this.cancelRequested = true;
        }

        void update(Message it) {
            if (it instanceof RowToken) {
                this.hasSeenRows = true;
            }

            if (it instanceof ErrorToken) {
                this.errorToken = (ErrorToken) it;
                this.hasSeenError = true;
            }
        }

        public void update(Phase newPhase) {

            this.phase = newPhase;

            if (newPhase == Phase.PREPARE_RETRY) {
                errorToken = null;
                hasSeenError = false;
            }
        }

        enum Phase {
            NONE, FETCHING, PREPARE_RETRY, CLOSING, CLOSED, ERROR
        }

    }

    static class IntermediateCount extends AbstractDoneToken {

        public IntermediateCount(DoneInProcToken token) {
            super(token.getType(), token.getStatus(), token.getCurrentCommand(), token.getRowCount());
        }

        @Override
        public String getName() {
            return "INTERMEDIATE_COUNT";
        }

    }

    static class OnCursorComplete implements Runnable {

        private static final int STATE_ACTIVE = 0;

        private static final int STATE_CANCELLED = 1;

        private static final AtomicIntegerFieldUpdater<OnCursorComplete> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(OnCursorComplete.class, "state");

        private final Sinks.Empty<Void> trigger = Sinks.empty();

        private volatile int state = STATE_ACTIVE;

        @Override
        public void run() {

            if (STATE_UPDATER.compareAndSet(this, STATE_ACTIVE, STATE_CANCELLED)) {
                this.trigger.tryEmitEmpty();
            }
        }

        public Publisher<Void> takeUntil() {
            return this.trigger.asMono();
        }

    }

}
