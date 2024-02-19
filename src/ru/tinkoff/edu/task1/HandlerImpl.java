package ru.tinkoff.edu.task1;

import ru.tinkoff.edu.task1.interfaces.ApplicationStatusResponse;
import ru.tinkoff.edu.task1.interfaces.Client;
import ru.tinkoff.edu.task1.interfaces.Handler;
import ru.tinkoff.edu.task1.interfaces.Response;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class HandlerImpl implements Handler {
    public static final int OPERATION_TIMEOUT_S = 15;

    private final Client client;
    private final ExecutorService executor;

    public HandlerImpl(Client client) {
        this.client = client;
        this.executor = Executors.newFixedThreadPool(2);
    }

    @Override
    public ApplicationStatusResponse performOperation(String id) {
        CompletionService<Response> completionService = new ExecutorCompletionService<>(executor);

        AtomicReference<Duration> lastFailureDuration = new AtomicReference<>(Duration.ZERO);
        AtomicInteger retriesCount = new AtomicInteger();
        Instant deadline = Instant.now().plusSeconds(OPERATION_TIMEOUT_S);

        executor.submit(() -> receiveApplicationStatus(
                id,
                client::getApplicationStatus1,
                completionService,
                deadline,
                retriesCount,
                lastFailureDuration));

        executor.submit(() -> receiveApplicationStatus(
                id,
                client::getApplicationStatus2,
                completionService,
                deadline,
                retriesCount,
                lastFailureDuration));

        try {
            Response response = completionService.poll(OPERATION_TIMEOUT_S, TimeUnit.SECONDS).get();
            if (response instanceof Response.Success success) {
                return new ApplicationStatusResponse.Success(success.applicationId(), success.applicationStatus());
            }

            if (response instanceof Response.Failure) {
                return makeFailureResp(lastFailureDuration.get(), retriesCount);
            }
        } catch (Exception e) {
            return makeFailureResp(lastFailureDuration.get(), retriesCount);
        } finally {
            executor.shutdown();
        }
        return makeFailureResp(lastFailureDuration.get(), retriesCount);
    }

    private ApplicationStatusResponse.Failure makeFailureResp(Duration lastFailureDuration, AtomicInteger retriesCount) {
        return new ApplicationStatusResponse.Failure(lastFailureDuration, retriesCount.get());
    }

    private void receiveApplicationStatus(String id,
                                          Function<String, Response> statusRequest,
                                          CompletionService<Response> completionService,
                                          Instant deadline,
                                          AtomicInteger retriesCount,
                                          AtomicReference<Duration> lastFailureDuration) {
        Instant requestStart = Instant.now();
        if (Instant.now().isAfter(deadline)) {
            return;
        }

        Response response = statusRequest.apply(id);
        if (response instanceof Response.RetryAfter) {
            Duration delay = ((Response.RetryAfter) response).delay();
            retriesCount.incrementAndGet();
            CompletableFuture
                    .delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)
                    .execute(() ->
                            receiveApplicationStatus(
                                    id,
                                    statusRequest,
                                    completionService,
                                    deadline,
                                    retriesCount,
                                    lastFailureDuration)
                    );
        } else if (response instanceof Response.Failure) {
            lastFailureDuration.set(Duration.between(requestStart, Instant.now()));
            completionService.submit(() -> response);
        } else {
            completionService.submit(() -> response);
        }
    }
}