package com.aeroncookbook.archive.learning;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

/**
 * Learning skeleton for the Client side of an Archive Replay.
 */
public final class MinimalArchiveClient {
    private static final String HOST_CONTROL_REQUEST_CHANNEL =
            "aeron:udp?endpoint=localhost:8101";
    private static final String CLIENT_CONTROL_RESPONSE_CHANNEL =
            "aeron:udp?endpoint=localhost:8103";
    private static final String ORIGINAL_RECORDING_CHANNEL = "aeron:ipc";
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int ORIGINAL_RECORDING_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;

    private MinimalArchiveClient() {
    }

    /**
     * Client-side learning flow. The Host must be running in another process.
     *
     * @param args unused command-line arguments.
     */
    public static void main(final String[] args) {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .dirDeleteOnStart(true);

        try (
                MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
                Aeron aeron = connectAeron(mediaDriver);
                AeronArchive archiveClient = connectArchiveClient(aeron)) {
            final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
            final long recordingId = findRecordingId(archiveClient, idleStrategy);


            try (Subscription replaySubscription = createReplaySubscription(aeron)) {
                final String replayChannel = resolveReplayChannel(replaySubscription, idleStrategy);
                long stopPosition = awaitStopPosition(archiveClient, recordingId, idleStrategy);
                long startPosition = startReplay(stopPosition, archiveClient, recordingId, replayChannel);
                pollReplay(startPosition, stopPosition, replaySubscription, idleStrategy);
            }
        }
    }

    private static Aeron connectAeron(final MediaDriver mediaDriver) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        // TODO(learning): connect Aeron to this MediaDriver's directory.
//        throw new UnsupportedOperationException("TODO: connect Client Aeron");
    }

    private static AeronArchive connectArchiveClient(final Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(HOST_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(CLIENT_CONTROL_RESPONSE_CHANNEL));
        // TODO(learning): connect to the Host control request channel and
        // configure the Client control response channel.
//        throw new UnsupportedOperationException("TODO: connect remote AeronArchive");
    }

    private static long findRecordingId(
            final AeronArchive archiveClient,
            final IdleStrategy idleStrategy) {
        final MutableLong lastRecordingId =
                new MutableLong(Long.MIN_VALUE);
        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> lastRecordingId.set(recordingId);
        while (lastRecordingId.get() == Long.MIN_VALUE) {
            archiveClient.listRecordingsForUri(0, 100, ORIGINAL_RECORDING_CHANNEL, ORIGINAL_RECORDING_STREAM_ID, consumer);
            if (lastRecordingId.get() == Long.MIN_VALUE) {
                idleStrategy.idle();
            }
        }

        return lastRecordingId.get();
        // TODO(learning): call listRecordingsForUri from recordingId 0,
        // filter ORIGINAL_RECORDING_CHANNEL + ORIGINAL_RECORDING_STREAM_ID,
        // and return the matching recordingId.
//        throw new UnsupportedOperationException("TODO: find recordingId");
    }

    private static Subscription createReplaySubscription(final Aeron aeron) {
        return aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);
        // TODO(learning): create a Subscription on the Client's own UDP endpoint
        // with REPLAY_STREAM_ID. Start with endpoint=localhost:0.
//        throw new UnsupportedOperationException("TODO: create Replay Subscription");
    }

    private static String resolveReplayChannel(
            final Subscription replaySubscription,
            final IdleStrategy idleStrategy) {
        String resolveChannelEndpointPort = replaySubscription.tryResolveChannelEndpointPort();
        while (resolveChannelEndpointPort == null) {
            idleStrategy.idle();
            resolveChannelEndpointPort = replaySubscription.tryResolveChannelEndpointPort();
        }
        return resolveChannelEndpointPort;
        // TODO(learning): repeatedly call tryResolveChannelEndpointPort()
        // until the actual Client endpoint is available.
//        throw new UnsupportedOperationException("TODO: resolve replay channel");
    }

    private static long startReplay(
            final long stopPosition,
            final AeronArchive archiveClient,
            final long recordingId,
            final String replayChannel) {

        long startPosition = archiveClient.getStartPosition(recordingId);
        long length = stopPosition - startPosition;
        System.out.println("replay length " + length);
        archiveClient.startReplay(recordingId, startPosition, length, replayChannel, REPLAY_STREAM_ID);
        return startPosition;
        // TODO(learning): call startReplay(recordingId, position, length,
        // replayChannel, REPLAY_STREAM_ID).
//        throw new UnsupportedOperationException("TODO: start replay");
    }

    private static long awaitStopPosition(
            final AeronArchive archiveClient,
            final long recordingId,
            final IdleStrategy idleStrategy) {
        long stopPosition = archiveClient.getStopPosition(recordingId);
        while (stopPosition == AeronArchive.NULL_POSITION) {
            idleStrategy.idle();
            stopPosition = archiveClient.getStopPosition(recordingId);
        }
        return stopPosition;
    }

    private static void pollReplay(
            final long startPosition,
            final long stopPosition,
            final Subscription replaySubscription,
            final IdleStrategy idleStrategy) {

        final MutableLong consumedPosition = new MutableLong(startPosition);

        while (!replaySubscription.isConnected()) {
            idleStrategy.idle();
        }
        FragmentHandler fragmentHandler = new FragmentHandler() {
            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                String stringAscii = buffer.getStringAscii(offset);
                consumedPosition.set(header.position());
                System.out.println("receive " + stringAscii);
            }
        };


        while (true) {
            final int fragmentsRead = replaySubscription.poll(fragmentHandler, 100);
            idleStrategy.idle(fragmentsRead);

            if (consumedPosition.get() >= stopPosition) {
                System.out.println("stop replay");
                break;
            }
        }


        // TODO(learning): poll the local Subscription and print/decode messages.
//        throw new UnsupportedOperationException("TODO: poll replay");
    }
}
