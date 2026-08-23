package com.kafkaantidote.cli;

import com.kafkaantidote.core.MessageFetchException;
import com.kafkaantidote.core.MessageSource;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.ReinjectionPlan;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * {@code gen-reinject} — produce a reviewable re-injection script (Spec R5.3, R4).
 *
 * <p>Reads the original message (read-only, to carry over its key/headers) and the operator-supplied
 * corrected payload, then generates a script that re-injects the corrected bytes to the EXACT
 * original partition. The tool does NOT repair messages and does NOT run the script — re-injection
 * preserves data but does not unstick the group (R4.6).
 */
@Command(
        name = "gen-reinject",
        description = "Generate a safe, reviewable re-injection script for a corrected payload.",
        mixinStandardHelpOptions = true,
        footer = {
                "",
                "Example:",
                "  antidote gen-reinject --bootstrap localhost:9092 --topic orders --partition 3 \\",
                "      --offset 84213 --corrected-payload ./fixed-order.bin --out ./Reinject.java"
        })
public final class GenReinjectCommand implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Option(names = {"-b", "--bootstrap"}, required = true, paramLabel = "<servers>",
            description = "Kafka bootstrap servers, e.g. localhost:9092")
    String bootstrap;

    @Option(names = {"-t", "--topic"}, required = true, paramLabel = "<t>",
            description = "Original topic.")
    String topic;

    @Option(names = {"-p", "--partition"}, required = true, paramLabel = "<p>",
            description = "Original partition (the script targets this EXACT partition — no key-hash re-partitioning).")
    int partition;

    @Option(names = {"-o", "--offset"}, required = true, paramLabel = "<o>",
            description = "Original offset (for coordinates and to carry over key/headers).")
    long offset;

    @Option(names = "--corrected-payload", required = true, paramLabel = "<file>",
            description = "File containing the operator-corrected message bytes to re-inject.")
    String correctedPayload;

    @Option(names = "--out", paramLabel = "<file>",
            description = "Where to write the generated script (default: stdout).")
    String out;

    @Option(names = "--json", description = "Emit the plan (target + script) as JSON instead of raw script text.")
    boolean json;

    @Override
    public Integer call() {
        PrintWriter stdout = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();

        final TopicPartitionOffset position;
        try {
            position = new TopicPartitionOffset(topic, partition, offset);
        } catch (IllegalArgumentException e) {
            err.println("Invalid coordinates: " + e.getMessage());
            return ExitCodes.USAGE;
        }

        final byte[] corrected;
        try {
            corrected = Files.readAllBytes(Path.of(correctedPayload));
        } catch (IOException e) {
            err.println("Could not read --corrected-payload '" + correctedPayload + "': " + e.getMessage());
            return ExitCodes.USAGE;
        }

        MessageSource source = MessageSources.forConsumer(bootstrap);

        final RawMessage original;
        try {
            original = source.fetchRaw(position); // read-only: needed to carry over key + headers
        } catch (MessageFetchException e) {
            err.println(e.getMessage());
            return ExitCodes.FETCH_FAILED;
        } catch (RuntimeException e) {
            err.println("Could not read original message at " + position.toHuman() + ": " + e.getMessage());
            return ExitCodes.CONNECTION;
        }

        ReinjectionPlan plan = source.planReinjection(original, corrected);

        if (out != null) {
            try {
                Files.writeString(Path.of(out), plan.scriptContent());
            } catch (IOException e) {
                err.println("Could not write --out '" + out + "': " + e.getMessage());
                return ExitCodes.USAGE;
            }
        }

        if (json) {
            stdout.println(plan.toJson());
        } else if (out != null) {
            stdout.println("Wrote re-injection script to " + out);
            stdout.println("REVIEW IT before running. It defaults to a dry run; use --produce --yes to send.");
            stdout.println("Re-injection preserves the message but does NOT unstick the group (a separate step).");
        } else {
            stdout.println(plan.scriptContent());
        }
        stdout.flush();
        return ExitCodes.OK;
    }
}
