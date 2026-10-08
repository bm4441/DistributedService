package com.zebra.ai.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The service policy: the conditions the device must meet for the Distributed Service to run. The rules, today:
 *  1. Battery: more than MIN_BATTERY_PERCENT percent.
 *  2. Temperature: the battery's temperature below MAX_TEMPERATURE_C degrees Celsius.
 * This class only judges a reading of the device (DeviceStats): what the service does when the policy is not met is
 * decided elsewhere. A rule whose reading is not available is not met -- it cannot be confirmed -- except the battery rule
 * on a device without a battery.
 *
 * Another rule is added by writing a Rule and adding it in the constructor; the verdict, the log line and the status
 * message sent to the dashboard are all built from the list of rules.
 *
 * No Android dependencies, so it runs in plain unit tests.
 */
final class ServicePolicy {

    /** The battery must have MORE than this much charge (percent). */
    static final int MIN_BATTERY_PERCENT = 20;
    /** The battery must be LESS than this warm (degrees Celsius). */
    static final float MAX_TEMPERATURE_C = 50.0f;

    /** The verdict of one rule on one reading. */
    static final class RuleResult {
        final String id;
        final String name;
        final boolean ok;
        /** What was measured, e.g. "54%". */
        final String value;
        /** What the rule asks for, e.g. "more than 20%". */
        final String required;
        /** Extra context that doesn't change the verdict, e.g. "charging". */
        final String detail;
        /** Why it failed, as a clause for a sentence ("battery 18% is not more than 20%"); empty when ok. */
        final String reason;

        RuleResult(String id, String name, boolean ok, String value, String required, String detail, String reason) {
            this.id = id;
            this.name = name;
            this.ok = ok;
            this.value = value;
            this.required = required;
            this.detail = detail;
            this.reason = reason;
        }
    }

    /** The verdict of the whole policy on one reading. */
    static final class Decision {
        /** Every rule is met. */
        final boolean met;
        final List<RuleResult> rules;
        /** "All conditions met", or what is wrong ("Battery 18% is not more than 20%; ..."). */
        final String summary;
        /** Which rules are met -- changes only when a verdict changes, not with every new reading. */
        final String key;

        Decision(List<RuleResult> rules) {
            this.rules = rules;
            boolean allMet = true;
            StringBuilder reasons = new StringBuilder();
            StringBuilder keyBuilder = new StringBuilder();
            for (RuleResult rule : rules) {
                keyBuilder.append(rule.id).append('=').append(rule.ok).append(';');
                if (!rule.ok) {
                    allMet = false;
                    if (reasons.length() > 0) {
                        reasons.append("; ");
                    }
                    reasons.append(rule.reason);
                }
            }
            this.met = allMet;
            this.key = keyBuilder.toString();
            if (allMet) {
                this.summary = "All conditions met";
            } else {
                String text = reasons.toString();
                this.summary = text.isEmpty() ? "Conditions not met"
                        : Character.toUpperCase(text.charAt(0)) + text.substring(1);
            }
        }
    }

    /** One condition of the policy. */
    interface Rule {
        /** Short name for the log and the status message, e.g. "Battery". */
        String name();

        /** What the rule asks for, e.g. "more than 20%". */
        String requirement();

        RuleResult evaluate(DeviceStats stats);
    }

    private final List<Rule> mRules = new ArrayList<>();

    ServicePolicy() {
        mRules.add(new BatteryRule());
        mRules.add(new TemperatureRule());
    }

    /** Judges one reading against every rule. */
    Decision evaluate(DeviceStats stats) {
        List<RuleResult> results = new ArrayList<>(mRules.size());
        for (Rule rule : mRules) {
            results.add(rule.evaluate(stats));
        }
        return new Decision(results);
    }

    /** The policy in words: "Battery: more than 20%; Temperature: less than 50 °C". */
    String describe() {
        StringBuilder text = new StringBuilder();
        for (Rule rule : mRules) {
            if (text.length() > 0) {
                text.append("; ");
            }
            text.append(rule.name()).append(": ").append(rule.requirement());
        }
        return text.toString();
    }

    /* ================= The rules ================= */

    /** Rule 1: more than MIN_BATTERY_PERCENT percent charge. */
    private static final class BatteryRule implements Rule {
        @Override
        public String name() {
            return "Battery";
        }

        @Override
        public String requirement() {
            return "more than " + MIN_BATTERY_PERCENT + "%";
        }

        @Override
        public RuleResult evaluate(DeviceStats s) {
            String required = requirement();
            if (!s.batteryPresent) {
                // Nothing to run flat: a device without a battery meets this rule.
                return new RuleResult("battery", name(), true, "no battery", required, "", "");
            }
            if (s.batteryPercent < 0) {
                return new RuleResult("battery", name(), false, "unknown", required, "", "battery level is not available");
            }
            boolean ok = s.batteryPercent > MIN_BATTERY_PERCENT;
            String value = s.batteryPercent + "%";
            return new RuleResult("battery", name(), ok, value, required, s.charging ? "charging" : "on battery",
                    ok ? "" : "battery " + value + " is not " + required);
        }
    }

    /** Rule 2: the battery's temperature below MAX_TEMPERATURE_C degrees. */
    private static final class TemperatureRule implements Rule {
        @Override
        public String name() {
            return "Temperature";
        }

        @Override
        public String requirement() {
            return String.format(Locale.US, "less than %.0f °C", MAX_TEMPERATURE_C);
        }

        @Override
        public RuleResult evaluate(DeviceStats s) {
            String required = requirement();
            if (Float.isNaN(s.batteryTempC)) {
                return new RuleResult("temperature", name(), false, "unknown", required, "",
                        "temperature is not available");
            }
            boolean ok = s.batteryTempC < MAX_TEMPERATURE_C;
            String value = String.format(Locale.US, "%.1f °C", s.batteryTempC);
            return new RuleResult("temperature", name(), ok, value, required, "battery",
                    ok ? "" : "temperature " + value + " is not " + required);
        }
    }
}
