package com.aeroncookbook.archive.learning;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

/**
 * First learning step: construct the minimum set of objects owned by an Archive Host.
 */
public final class MinimalArchiveHost {
    private static final String RECORDING_CHANNEL = "aeron:ipc";
    private static final String CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:8101";
    private static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8101";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8102";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int RECORDING_STREAM_ID = 100;

    private MinimalArchiveHost() {
    }

    /**
     * Launch the minimum Archive Host object graph.
     *
     * @param args unused command-line arguments.
     */
    public static void main(final String[] args) {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .dirDeleteOnStart(true);
        final Archive.Context archiveContext = new Archive.Context()
                .deleteArchiveOnStart(true);

        try (
                ArchivingMediaDriver archivingMediaDriver =
                        launchArchivingMediaDriver(mediaDriverContext, archiveContext);
                Aeron aeron = connectAeron(archivingMediaDriver);
                AeronArchive archiveClient = connectArchiveClient(aeron);
                ExclusivePublication publication = createPublication(aeron)) {
            System.out.printf("ArchivingMediaDriver directory: %s%n",
                    archivingMediaDriver.mediaDriver().aeronDirectoryName());
            System.out.printf("Aeron clientId: %d%n", aeron.clientId());
            System.out.printf("Archive controlSessionId: %d%n", archiveClient.controlSessionId());
            System.out.printf("Publication streamId: %d%n", publication.streamId());
            final long recordingSubscriptionId = archiveClient.startRecording(
                    RECORDING_CHANNEL,
                    RECORDING_STREAM_ID,
                    SourceLocation.LOCAL);

            final long recordingId = awaitRecordingId(
                    aeron,
                    archiveClient,
                    publication,
                    new SleepingMillisIdleStrategy());
            System.out.printf("Recording id: %d%n", recordingId);

            final long publishedPosition = publishMessages(publication);
//            System.out.println("finish publish, position " + publishedPosition);
            awaitRecordingPosition(
                    aeron,
                    archiveClient,
                    publication,
                    publishedPosition,
                    new SleepingMillisIdleStrategy());
            System.out.printf("Recording caught up to position: %d%n", publishedPosition);
            IdleStrategy strategy = new SleepingMillisIdleStrategy(100);
            archiveClient.stopRecording(recordingSubscriptionId);
            while (true){
                strategy.idle();
            }
        }
    }

    private static ArchivingMediaDriver launchArchivingMediaDriver(
            final MediaDriver.Context mediaDriverContext,
            final Archive.Context archiveContext) {
        return ArchivingMediaDriver.launch(
                mediaDriverContext,
                archiveContext
                        .controlChannel(CONTROL_CHANNEL)
                        .replicationChannel(REPLICATION_CHANNEL));
        // TODO(learning): use both contexts to launch the combined Media Driver and Archive service.
//        throw new UnsupportedOperationException("TODO: launch ArchivingMediaDriver");
    }

    private static Aeron connectAeron(final ArchivingMediaDriver archivingMediaDriver) {
        return Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName()));
        // TODO(learning): connect Aeron to the directory of the Media Driver launched above.
//        throw new UnsupportedOperationException("TODO: connect Aeron");
    }

    private static AeronArchive connectArchiveClient(final Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));
        // TODO(learning): create an AeronArchive client that reuses this Aeron client.
//        throw new UnsupportedOperationException("TODO: connect AeronArchive client");
    }

    private static ExclusivePublication createPublication(final Aeron aeron) {
        return aeron.addExclusivePublication(RECORDING_CHANNEL, RECORDING_STREAM_ID);
        // TODO(learning): create an exclusive publication with RECORDING_CHANNEL and RECORDING_STREAM_ID.
//        throw new UnsupportedOperationException("TODO: create ExclusivePublication");
    }

    private static long awaitRecordingId(
            final Aeron aeron,
            final AeronArchive archiveClient,
            final ExclusivePublication publication,
            final SleepingMillisIdleStrategy idleStrategy) {

        CountersReader countersReader = aeron.countersReader();
        int counterIdBySession = RecordingPos.findCounterIdBySession(countersReader, publication.sessionId(), archiveClient.archiveId());
        while (counterIdBySession == CountersReader.NULL_COUNTER_ID) {
            idleStrategy.idle();
            counterIdBySession = RecordingPos.findCounterIdBySession(countersReader, publication.sessionId(), archiveClient.archiveId());
        }

        return RecordingPos.getRecordingId(countersReader, counterIdBySession);

        // TODO(learning): use aeron.countersReader() to find the RecordingPos counter.
        // Match publication.sessionId() and archiveClient.archiveId().
        // While the counter is NULL_COUNTER_ID, idle and search again.
        // Finally return RecordingPos.getRecordingId(counters, counterId).
        //        throw new UnsupportedOperationException("TODO: await RecordingPos counter");
    }

    private static long publishMessages(final ExclusivePublication publication) {
        while (!publication.isConnected()) {
            Thread.yield();
        }

        MutableDirectBuffer directBuffer = new UnsafeBuffer(new byte[128]);

        for (int i = 0; i < 3; i++) {
            int length = directBuffer.putStringAscii(0, "test" + i);
            long position = publication.offer(directBuffer, 0, length);
            while (position < 0) {
                Thread.yield();
                position = publication.offer(directBuffer, 0, length);
            }
        }
        return publication.position();

        // TODO(learning): publish a small, known set of messages and return the final publication.position().
//        throw new UnsupportedOperationException("TODO: publish messages");
    }

    private static void awaitRecordingPosition(
            final Aeron aeron,
            final AeronArchive archiveClient,
            final ExclusivePublication publication,
            final long expectedPosition,
            final IdleStrategy idleStrategy) {
        CountersReader countersReader = aeron.countersReader();
        int counterIdBySession = RecordingPos.findCounterIdBySession(countersReader, publication.sessionId(), archiveClient.archiveId());

//        long recordingId = RecordingPos.getRecordingId(countersReader, counterIdBySession);
//        while (recordingId == RecordingPos.NULL_RECORDING_ID) {
//            idleStrategy.idle();
//            recordingId = RecordingPos.getRecordingId(countersReader, counterIdBySession);
//        }

        long counterValue = countersReader.getCounterValue(counterIdBySession);
        while (counterValue < expectedPosition) {
            idleStrategy.idle();
            counterValue = countersReader.getCounterValue(counterIdBySession);
        }
        return;
        // TODO(learning): find the RecordingPos counter for this publication.
        // TODO(learning): idle until countersReader.getCounterValue(counterId) >= expectedPosition.
//        throw new UnsupportedOperationException("TODO: await recording position");
    }
}
