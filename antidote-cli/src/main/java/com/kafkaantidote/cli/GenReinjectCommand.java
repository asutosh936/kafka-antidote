package com.kafkaantidote.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code gen-reinject} — produce a reviewable re-injection script (Spec R5.3, R4).
 *
 * <p>The original topic/partition/offset identify the message (and carry over its key, headers, and
 * timestamp); {@code --corrected-payload} is the operator-supplied fixed bytes. The tool does NOT
 * repair messages and does NOT run the script — re-injection preserves data but does not unstick the
 * group (R4.6).
 *
 * <p>Phase 0: option surface and {@code --help} only; the generator lands in Phase 3 (human review gate).
 */
@Command(
        name = "gen-reinject",
        description = "Generate a safe, reviewable re-injection script for a corrected payload.",
        mixinStandardHelpOptions = true,
        footer = {
                "",
                "Example:",
                "  antidote gen-reinject --bootstrap localhost:9092 --topic orders --partition 3 \\",
                "      --offset 84213 --corrected-payload ./fixed-order.bin --out ./reinject-orders-3.java"
        })
public final class GenReinjectCommand implements Callable<Integer> {

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
            description = "Original offset (for coordinates and to carry over key/headers/timestamp).")
    long offset;

    @Option(names = "--corrected-payload", required = true, paramLabel = "<file>",
            description = "File containing the operator-corrected message bytes to re-inject.")
    String correctedPayload;

    @Option(names = "--out", paramLabel = "<file>",
            description = "Where to write the generated script (default: stdout).")
    String out;

    @Override
    public Integer call() {
        System.err.println("gen-reinject: not implemented in this build phase (Phase 3).");
        return ExitCodes.NOT_IMPLEMENTED;
    }
}
