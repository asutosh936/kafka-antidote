package com.kafkaantidote.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code inspect} — dump + classify one message (Spec R5.2). Fetches raw bytes without the failing
 * deserializer (R2.1) and shows them as hex, best-effort UTF-8, and a labeled heuristic (R2.2, R3).
 *
 * <p>Phase 0: option surface and {@code --help} only; the fetch/classify logic lands in Phase 2.
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

    @Option(names = {"-b", "--bootstrap"}, required = true, paramLabel = "<servers>",
            description = "Kafka bootstrap servers, e.g. localhost:9092")
    String bootstrap;

    @Option(names = {"-t", "--topic"}, required = true, paramLabel = "<t>", description = "Topic.")
    String topic;

    @Option(names = {"-p", "--partition"}, required = true, paramLabel = "<p>", description = "Partition.")
    int partition;

    @Option(names = {"-o", "--offset"}, required = true, paramLabel = "<o>", description = "Offset to inspect.")
    long offset;

    @Option(names = "--json", description = "Emit machine-readable JSON instead of human-readable output.")
    boolean json;

    @Override
    public Integer call() {
        System.err.println("inspect: not implemented in this build phase (Phase 2).");
        return ExitCodes.NOT_IMPLEMENTED;
    }
}
