package com.kafkaantidote.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Phase-0 Definition of Done: {@code --help} runs (Spec R5.4).
 */
class HelpTest {

    private CommandLine newCli(StringWriter out) {
        return new CommandLine(new Antidote()).setOut(new PrintWriter(out));
    }

    @Test
    void topLevelHelpRunsAndListsAllSubcommands() {
        StringWriter out = new StringWriter();
        int code = newCli(out).execute("--help");

        assertEquals(ExitCodes.OK, code, "--help must exit 0");
        String text = out.toString();
        assertTrue(text.contains("diagnose"), "help lists diagnose");
        assertTrue(text.contains("inspect"), "help lists inspect");
        assertTrue(text.contains("gen-reinject"), "help lists gen-reinject");
    }

    @Test
    void eachSubcommandSupportsHelpWithAnExample() {
        for (String sub : new String[] {"diagnose", "inspect", "gen-reinject"}) {
            StringWriter out = new StringWriter();
            int code = newCli(out).execute(sub, "--help");
            assertEquals(ExitCodes.OK, code, sub + " --help must exit 0");
            assertTrue(out.toString().contains("Example:"), sub + " --help shows an example");
        }
    }

    @Test
    void noArgsPrintsUsageWithoutError() {
        StringWriter out = new StringWriter();
        int code = newCli(out).execute();
        // Running the top-level command with no subcommand prints usage via run(); exit is clean.
        assertEquals(ExitCodes.OK, code);
    }
}
