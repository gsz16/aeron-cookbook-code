package com.aeroncookbook.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveTest {

    private final String channel = "aeron:ipc";
    private final int streamCapture = 1;
    private final int streamReplay = 2;
    private final int sendCount = 10_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveTest.class);
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8011";
    private final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8012";
    private final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8013";

    private Aeron aeron;
    private AeronArchive aeronArchive;

    private ArchivingMediaDriver mediaDriver;

    public static void main(String[] args) {
        System.out.println("Test");
        final ArchiveTest archiveTest = new ArchiveTest();
        try {
            archiveTest.setup();
            archiveTest.write();
            archiveTest.read();
        } finally {
            archiveTest.cleanUp();
        }
    }

    private void setup() {


        mediaDriver = ArchivingMediaDriver.launch(
                new MediaDriver.Context().spiesSimulateConnection(true).dirDeleteOnStart(true),
                new Archive.Context()
                        .deleteArchiveOnStart(true)
                        .controlChannel(CONTROL_REQUEST_CHANNEL)
                        .replicationChannel(REPLICATION_CHANNEL)
                        .archiveDir(Utils.createTempDir()));

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL));

    }

    private void write() {
        aeronArchive.startRecording(channel, streamCapture, SourceLocation.LOCAL);
        ExclusivePublication exclusivePublication = aeron.addExclusivePublication(channel, streamCapture);
        try {
            while (!exclusivePublication.isConnected()) {
                idleStrategy.idle();
            }

            for (int i = 0; i < sendCount; i++) {
                int length = buffer.putStringAscii(0, "Test " + i);
                while (exclusivePublication.offer(buffer, 0, length) < 0) {
                    idleStrategy.idle();
                }
            }

            long stopPosition = exclusivePublication.position();
            CountersReader countersReader = aeron.countersReader();
            // 问题：一个 publication 可能对应多个 sessionId？
            // 解答：一个已经创建的 Publication 在生命周期内只有一个固定的 sessionId。
            // 但同一个 channel + streamId 上可以存在多个 Publication，因此可能同时出现多个 sessionId。
            // Archive 使用 sessionId 区分当前要查询的是哪一个发布会话的录制进度。
            int counterIdBySession = RecordingPos.findCounterIdBySession(countersReader, exclusivePublication.sessionId(), aeronArchive.archiveId());
            // 问题：counterId 是什么？
            // 解答：counterId 是该计数器在 Aeron Counters 共享区域中的槽位编号，不是 recordingId。
            // 这里找到的是 RecordingPos counter；读取它的值可以得到 Archive 当前已经录制到的 position。
            while (CountersReader.NULL_COUNTER_ID == counterIdBySession) {
                // 问题：这里循环的目的是什么？
                // 解答：startRecording、Publication 建连以及 RecordingPos counter 的创建是异步完成的。
                // 如果 counter 尚未创建，findCounterIdBySession 会返回 NULL_COUNTER_ID；这里持续等待它出现。
                idleStrategy.idle();
                counterIdBySession = RecordingPos.findCounterIdBySession(countersReader, exclusivePublication.sessionId(), aeronArchive.archiveId());
            }
            // Publication 的 position 可能暂时领先于 Archive 的录制 position。
            // 等待 Archive 追上发送结束时的 stopPosition，确保已成功发布的数据全部完成归档后再关闭 Publication。
            while (countersReader.getCounterValue(counterIdBySession) < stopPosition) {
                idleStrategy.idle();
            }
        } finally {
            exclusivePublication.close();
        }

    }

    private void read() {
        AeronArchive reader = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL)
                .aeron(aeron));
        try {
            long recordingId = findLatestRecording(reader, channel, streamCapture);
            long position = AeronArchive.NULL_POSITION;
            long length = Long.MAX_VALUE;

            long replaySesionId = reader.startReplay(recordingId, position, length, channel, streamReplay);
            String channelRead = ChannelUri.addSessionId(channel, (int) replaySesionId);

            Subscription subscription = reader.context().aeron().addSubscription(channelRead, streamReplay);
            while (!subscription.isConnected()) {
                idleStrategy.idle();
            }
            final boolean[] complete = {false};
            while (!complete[0]) {
                subscription.poll(new FragmentHandler() {
                    @Override
                    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                        String stringAscii = buffer.getStringAscii(offset);
                        LOGGER.info("receive {}", stringAscii);
                        if (stringAscii.contains("9999")) {
                            complete[0] = true;
                        }
                    }
                }, 1);
            }
        } finally {
            reader.close();
        }

    }

    private long findLatestRecording(AeronArchive archive, String channel, int streamId) {
        MutableLong lastRecodingId = new MutableLong();
        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId1, strippedChannel, originalChannel, sourceIdentity)
                -> lastRecodingId.set(recordingId);

        long fromRecodingId = 0;
        int recordCount = 100;

        int foundCount = archive.listRecordingsForUri(fromRecodingId, recordCount, channel, streamId, consumer);
        if (foundCount == 0) {
            throw new IllegalStateException("no recordings found");
        }
        return lastRecodingId.get();
    }

    private void cleanUp() {
        CloseHelper.quietClose(aeronArchive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }
}
