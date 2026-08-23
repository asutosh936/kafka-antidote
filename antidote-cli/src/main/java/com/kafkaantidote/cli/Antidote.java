package com.kafkaantidote.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Kafka Antidote — poison-pill recovery CLI (Spec R5).
 *
 * <p>Top-level entry point. Wires the three subcommands (diagnose, inspect, gen-reinject). Running
 * with no subcommand prints usage.
 */
@Command(
        name = "antidote",
        header = "Kafka Antidote — find, inspect, and safely recover from Kafka poison pills.",
        description = "Diagnose a stalled consumer group, inspect the un-deserializable payload, "
                + "and generate a safe re-injection script. Diagnose and inspect are read-only.",
        mixinStandardHelpOptions = true,
        versionProvider = Antidote.ManifestVersionProvider.class,
        subcommands = {
                DiagnoseCommand.class,
                InspectCommand.class,
                GenReinjectCommand.class
        })
public final class Antidote implements Runnable {

    @Override
    public void run() {
        // No subcommand given: show usage. picocli returns USAGE (2) for this via the run below.
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Antidote()).execute(args);
        System.exit(exitCode);
    }

    /** Reads the version from the jar manifest so it stays in sync with the build. */
    static final class ManifestVersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            String v = Antidote.class.getPackage().getImplementationVersion();
            return new String[] {"antidote " + (v == null ? "0.1.0-SNAPSHOT (dev)" : v)};
        }
    }
}
