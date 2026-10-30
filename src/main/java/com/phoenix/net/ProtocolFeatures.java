// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

/**
 * Optional protocol features a node agent can announce in
 * {@code POST /register} under {@code "protocolFeatures"}.
 *
 * <p>Deliberately separate from {@code capabilities}: those are scheduling
 * tags matched against task requirements, these describe how the agent speaks
 * to Core. Unknown feature names are ignored, so agents can announce features
 * a newer Core understands without breaking an older one.
 */
public final class ProtocolFeatures {

    private ProtocolFeatures() {
    }

    /**
     * The agent echoes the {@code attemptId} it receives with a task in
     * {@code /tasks/started} and {@code /results}.
     */
    public static final String ATTEMPT_ID = "attempt-id";
}
