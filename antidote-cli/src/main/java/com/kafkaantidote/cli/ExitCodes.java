package com.kafkaantidote.cli;

/**
 * Meaningful, documented exit codes (Spec R5.5): 0 = clean, distinct non-zero per failure class.
 */
public final class ExitCodes {

    /** Clean run — includes "no poison pill detected" (R1.3). */
    public static final int OK = 0;

    /** Diagnosis succeeded and at least one stuck partition was found (grep-style signal). */
    public static final int POISON_DETECTED = 1;

    /** Bad or missing arguments / usage error (picocli default). */
    public static final int USAGE = 2;

    /** Could not reach or authenticate to the cluster. */
    public static final int CONNECTION = 3;

    /** The requested message/offset could not be fetched (R2.4). */
    public static final int FETCH_FAILED = 4;

    /** Behavior not yet implemented in the current build phase. */
    public static final int NOT_IMPLEMENTED = 64;

    private ExitCodes() {
    }
}
