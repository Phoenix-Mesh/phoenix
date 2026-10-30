// SPDX-License-Identifier: Apache-2.0

package com.phoenix.net;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

/** Marks the NodeAgentClient binding for devices with no direct transport
 * (e.g. ESP32 — reachable only through the message broker, so a node that
 * did not register over MQTT cannot run tasks at all). */
@BindingAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({FIELD, PARAMETER, METHOD})
public @interface BrokerTransport {}
