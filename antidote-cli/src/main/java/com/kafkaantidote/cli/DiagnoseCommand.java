package com.kafkaantidote.cli;

import com.kafkaantidote.core.GroupCoordinates;
import com.kafkaantidote.core.MessageSource;
import com.kafkaantidote.core.StuckPosition;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * {@code diagnose} — find and report stuck offsets (Spec R5.1). Detection is multi-sample (R1.1):
 * a partition is only reported stuck when its committed offset stays put across {@code --samples}
 * reads separated by {@code --poll-interval} while lag stays non-zero. The path is strictly
 * read-only (R1.4).
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

    @Spec
    CommandSpec spec;

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

    @Option(names = "--json", description = "Emit machine-readable JSON instead of human-readable output. (Phase 4)")
    boolean json;

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();

        final Duration interval;
        try {
            interval = DurationParser.parse(pollInterval);
        } catch (IllegalArgumentException e) {
            err.println("Invalid --poll-interval: " + e.getMessage());
            return ExitCodes.USAGE;
        }

        MessageSource source = MessageSources.forConsumer(bootstrap, samples, interval);
        GroupCoordinates coordinates = new GroupCoordinates(bootstrap, group);

        final List<StuckPosition> stuck;
        try {
            stuck = source.findStuckPositions(coordinates);
        } catch (RuntimeException e) {
            err.println("Could not diagnose " + coordinates.toHuman() + ": " + e.getMessage());
            return ExitCodes.CONNECTION;
        }

        if (stuck.isEmpty()) {
            out.println("No poison pill detected — " + coordinates.toHuman()
                    + " has no stuck partitions.");
            out.flush();
            return ExitCodes.OK;
        }

        out.println("Poison pill(s) detected in " + coordinates.toHuman() + ":");
        out.println();
        for (StuckPosition p : stuck) {
            out.println("  " + p.topic() + " partition " + p.partition());
            out.println("    committed offset : " + p.committedOffset());
            out.println("    log-end offset   : " + p.logEndOffset());
            out.println("    lag              : " + p.lag());
            out.println();
        }
        StuckPosition firstStuck = stuck.get(0);
        out.println("Next: inspect the payload —");
        out.println("  antidote inspect --bootstrap " + bootstrap
                + " --topic " + firstStuck.topic()
                + " --partition " + firstStuck.partition()
                + " --offset " + firstStuck.committedOffset());
        out.flush();
        return ExitCodes.POISON_DETECTED;
    }
}
