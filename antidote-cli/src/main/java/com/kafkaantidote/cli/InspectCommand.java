package com.kafkaantidote.cli;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.MessageFetchException;
import com.kafkaantidote.core.MessageSource;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.TopicPartitionOffset;
import com.kafkaantidote.payload.FailureClassifier;
import com.kafkaantidote.payload.PayloadPresenter;
import java.io.PrintWriter;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * {@code inspect} — dump + classify one message (Spec R5.2). Fetches raw bytes without the failing
 * deserializer (R2.1) and shows them as hex, best-effort UTF-8, metadata, and a labeled heuristic
 * (R2.2, R2.3, R3). Read-only.
 */
@Command(
        name = "inspect",
        description = "Dump and classify a single message at a topic/partition/offset (read-only).",
        mixinStandardHelpOptions = true,
        footer = {
                "",
                "Example:",
                "  antidote inspect --bootstrap localhost:9092 --topic orders --partition 3 --offset 84213"
        })
public final class InspectCommand implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Option(names = {"-b", "--bootstrap"}, required = true, paramLabel = "<servers>",
            description = "Kafka bootstrap servers, e.g. localhost:9092")
    String bootstrap;

    @Option(names = {"-t", "--topic"}, required = true, paramLabel = "<t>", description = "Topic.")
    String topic;

    @Option(names = {"-p", "--partition"}, required = true, paramLabel = "<p>", description = "Partition.")
    int partition;

    @Option(names = {"-o", "--offset"}, required = true, paramLabel = "<o>", description = "Offset to inspect.")
    long offset;

    @Option(names = "--json", description = "Emit machine-readable JSON instead of human-readable output. (Phase 4)")
    boolean json;

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();

        final TopicPartitionOffset position;
        try {
            position = new TopicPartitionOffset(topic, partition, offset);
        } catch (IllegalArgumentException e) {
            err.println("Invalid coordinates: " + e.getMessage());
            return ExitCodes.USAGE;
        }

        MessageSource source = MessageSources.forConsumer(bootstrap);

        final RawMessage raw;
        try {
            raw = source.fetchRaw(position);
        } catch (MessageFetchException e) {
            // R2.4: a clear, actionable message — never a raw stack trace as the primary output.
            err.println(e.getMessage());
            return ExitCodes.FETCH_FAILED;
        } catch (RuntimeException e) {
            err.println("Could not inspect " + position.toHuman() + ": " + e.getMessage());
            return ExitCodes.CONNECTION;
        }

        FailureClassification classification = FailureClassifier.classify(raw);
        if (json) {
            out.println("{\"message\":" + raw.toJson()
                    + ",\"classification\":" + classification.toJson() + "}");
        } else {
            out.println(PayloadPresenter.toHuman(raw, classification));
        }
        out.flush();
        return ExitCodes.OK;
    }
}
