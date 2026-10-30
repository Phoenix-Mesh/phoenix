// SPDX-License-Identifier: Apache-2.0

package com.phoenix.core;

/**
 * How strongly late or duplicate reports for one attempt can be rejected.
 *
 * <p>Chosen once, when the attempt is created, from what the node announced
 * at registration; it never changes during the attempt. Because the mode is a
 * property of the <em>current attempt</em>, a legacy node's stale report can
 * never be accepted for an {@link #ENFORCED} retry: the retry demands its own
 * id, which the stale report cannot have.
 */
public enum AttemptFencing {

    /**
     * The node advertised the {@code attempt-id} protocol feature. Core sends
     * it the attempt id and requires that every report echoes it. A report
     * with a different id, or with no id at all, is rejected as stale.
     */
    ENFORCED,

    /**
     * The node cannot echo an attempt id. Reports without one are accepted
     * (backwards compatibility) but are <b>not fenced at the attempt level</b>:
     * Core cannot tell a late report from an abandoned attempt apart from a
     * report for the current one. A report that does carry a wrong id is still
     * rejected.
     */
    LEGACY
}
