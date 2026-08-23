package com.kafkaantidote.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code diagnose} — find and report stuck offsets (Spec R5.1). Detection is multi-sample (R1.1):
 * a partition is only reported stuck when its committed offset stays put across {@code --samples}
 * reads separated by {@code --poll-interval} while lag stays non-zero.
 *
 * <p>Phase 0: the option surface and {@code --help} are wired; the detection logic lands in Phase 1.
 */
@Command(
        name = "diagnose",
        description = "Find and report partitions stuck on a poison pill (read-only).",
        mixinStandardHelpOptions = true,
        footer = {
                "",
                "Example:",
                "  antidote diagnose --bootstrap localhost:9092 --group orders-consumer",
                "  antidote diagnose --bootstrap localhost:9092 --group orders-consumer --poll-interval 5s --samples 3"
        })
public final class DiagnoseCommand implements Callable<Integer> {

    @Option(names = {"-b", "--bootstrap"}, required = true, paramLabel = "<servers>",
            description = "Kafka bootstrap servers, e.g. localhost:9092")
    String bootstrap;

    @Option(names = {"-g", "--group"}, required = true, paramLabel = "<group>",
            description = "Consumer group id to diagnose.")
    String group;

    @Option(names = "--poll-interval", paramLabel = "<duration>", defaultValue = "5s",
            description = "Gap between offset samples (default: ${DEFAULT-VALUE}).")
    String pollInterval;

    @Option(names = "--samples", paramLabel = "<n>", defaultValue = "2",
            description = "Number of samples the committed offset must stay stationary to be called stuck (default: ${DEFAULT-VALUE}).")
    int samples;

    @Option(names = "--json", description = "Emit machine-readable JSON instead of human-readable output.")
    boolean json;

    @Override
    public Integer call() {
        System.err.println("diagnose: not implemented in this build phase (Phase 1).");
        return ExitCodes.NOT_IMPLEMENTED;
    }
}
