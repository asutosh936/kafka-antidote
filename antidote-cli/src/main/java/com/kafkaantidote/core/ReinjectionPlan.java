package com.kafkaantidote.core;

import java.util.Objects;

/**
 * The output of {@link MessageSource#planReinjection}: a reviewable re-injection artifact
 * (Spec R4). Immutable value type (Spec §4.3).
 *
 * <p>The plan carries the exact original {@link #target} (R4.2 — the script must hit that exact
 * partition, never re-partition by key hash) and the generated {@link #scriptContent} a human
 * reviews before running. It intentionally holds no ability to execute anything: generating the
 * plan is read-only; running it is a separate human act (R4.6).
 */
public record ReinjectionPlan(TopicPartitionOffset target, String language, String scriptContent) {

    public ReinjectionPlan {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(language, "language");
        Objects.requireNonNull(scriptContent, "scriptContent");
    }

    public String toHuman() {
        return "re-injection plan (" + language + ") targeting exact partition "
                + target.topic() + "-" + target.partition();
    }

    public String toJson() {
        return "{\"target\":" + target.toJson()
                + ",\"language\":" + Json.quote(language)
                + ",\"script\":" + Json.quote(scriptContent) + "}";
    }
}
