/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.extensions.helloclientmetric;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the main class of the extension.
 * <p>
 * It periodically fetches the message queue size for a specific client ({@code client1})
 * via the HiveMQ REST API and exposes it as a HiveMQ metric named
 * {@code com.hivemq.client1.message-queue-size}.
 *
 * @author Dasha Samkova
 * @since 4.46.0
 */
public class HelloClientMetricSimpleMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(HelloClientMetricSimpleMain.class);
	private static final @NotNull String CLIENT_ID = "client1";
	private static final @NotNull String API_URL = "http://localhost:8888/api/v1/mqtt/clients/" + CLIENT_ID;
	private static final @NotNull String METRIC_NAME = "com.hivemq." + CLIENT_ID + ".message-queue-size";

	private final @NotNull OkHttpClient httpClient = new OkHttpClient();
	private final @NotNull Gson gson = new Gson();
	private final @NotNull AtomicLong messageQueueSize = new AtomicLong(0);


	@Override
    public void extensionStart(
            final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {
			Services.metricRegistry().gauge(METRIC_NAME, () -> messageQueueSize::get);

			Services.extensionExecutorService().scheduleAtFixedRate(this::fetchAndUpdateMetric, 120, 15, TimeUnit.SECONDS);


			final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

        } catch (final Exception e) {
            log.error("Exception thrown at extension start: ", e);
        }
    }

    @Override
    public void extensionStop(
            final @NotNull ExtensionStopInput extensionStopInput,
            final @NotNull ExtensionStopOutput extensionStopOutput) {

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());
    }

	private void fetchAndUpdateMetric() {
		final Request request = new Request.Builder().url(API_URL).get().build();
		try (final Response response = httpClient.newCall(request).execute()) {
			if (!response.isSuccessful()) {
				log.warn("Failed to fetch client data from API. URL: {}, Response: {}", API_URL, response);
				return;
			}

			final ResponseBody body = response.body();
			if (body == null) {
				log.warn("Response body from API was null. URL: {}", API_URL);
				return;
			}

			final JsonObject json = gson.fromJson(body.string(), JsonObject.class);
			if (json != null && json.has("client") && json.get("client").isJsonObject()) {
				final JsonObject clientObject = json.getAsJsonObject("client");
				if (clientObject.has("messageQueueSize")) {
					final JsonElement messageQueueSizeElement = clientObject.get("messageQueueSize");
					final long queueSize = messageQueueSizeElement.getAsLong();
					messageQueueSize.set(queueSize);
					log.debug("Successfully updated message queue size for client '{}' to {}", CLIENT_ID, queueSize);
				} else {
					log.warn("JSON response did not contain 'messageQueueSize'. Response: {}", json);
				}
			} else {
				log.warn("JSON response did not contain a 'client' object. Response: {}", json);
			}
		} catch (final IOException e) {
			log.error("Error while fetching client data from API for client '{}'", CLIENT_ID, e);
		} catch (final Exception e) {
			log.error("An unexpected error occurred while processing the API response for client '{}'", CLIENT_ID, e);
		}
	}

}