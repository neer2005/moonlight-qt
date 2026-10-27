// micstream.cpp - Client-side microphone passthrough sender.
//
// Captures the default microphone with Qt, encodes Opus (VOIP, 64 kbps,
// 10 ms frames), and sends each frame to the host via
// LiSendMicrophoneOpusData(), the Apollo UDP mic protocol (separate UDP
// stream on port 48001, Opus AES-128-CBC when encryption is negotiated).
// The host renders the stream into the Steam Streaming Microphone input.
//
// This is adapted from qiin2333's Moonlight V+ MicStream (Qt capture +
// Opus worker design, bounded queues that drop frames instead of building
// latency), rewired to the common-c Apollo mic send path. Mic negotiation
// happens in the RTSP handshake (common-c sets enableMic and negotiates
// the mic stream); this sender simply transmits while a session is active
// and the user enabled the microphone.
// Capture prefers Float samples (as in Nightjar's SDL capture path) with an
// Int16 fallback; MicStream::probe() lets the session warn before launch
// when no usable microphone is available.
//
// NOTE: LiSendMicrophoneOpusData() does not exist in upstream
// moonlight-common-c. It lives in neer2005/moonlight-common-c (Apollo port)
// and moonlight-qt's submodule must point there before this builds.

#include "micstream.h"
#include "macpermissions.h"

#include <cstdint>
#include <cstring>

#include <Limelight.h>
#include <opus.h>
#include <QAudio>
#include <QAudioDevice>
#include <QAudioFormat>
#include <QAudioSource>
#include <QDebug>
#include <QElapsedTimer>
#include <QIODevice>
#include <QMediaDevices>
#include <QQueue>
#include <QThread>
#include <QTimer>

static const int PCM_FRAME_SAMPLES = 480; // 10 ms at 48 kHz (Nightjar's desktop capture uses 10 ms frames)
static const int MAX_OPUS_SIZE = 4000;
static const int MAX_PENDING_PCM_FRAMES = 10;
static const int MAX_PENDING_OPUS_FRAMES = 10;
static const int MAX_ENCODE_FRAMES_PER_PASS = 4;
static const int MAX_SEND_FRAMES_PER_PASS = 4;

// Max Opus payload per packet. LiSendMicrophoneOpusData() accepts at most
// MAX_MIC_PACKET_SIZE (1400 bytes) minus its 12-byte header; 64 kbps at
// 10 ms is ~80 bytes/frame, comfortably under this cap. Frames that
// encode larger are dropped rather than split.
static const int MAX_OPUS_SEND_SIZE = 1400 - 12;

class MicStreamWorker : public QObject
{
public:
    MicStreamWorker()
        : m_audioInput(nullptr),
          m_audioDevice(nullptr),
          m_encoder(nullptr),
          m_sendTimer(nullptr),
          m_logTimer(nullptr),
          m_audioContinuationPending(false),
          m_useFloat(false),
          m_pcmFrameSize(0),
          m_pcmBytes(0),
          m_opusBytes(0),
          m_sentBytes(0),
          m_sentPackets(0),
          m_sendErrors(0),
          m_idleLoops(0),
          m_droppedPcmFrames(0),
          m_droppedOpusFrames(0),
          m_droppedOversizedFrames(0),
          m_maxAudioCallbackMs(0)
    {
    }

    bool start()
    {
        Q_ASSERT(QThread::currentThread() == thread());
        if (m_audioInput)
            return false;

        int err;
        m_encoder = opus_encoder_create(48000, 1, OPUS_APPLICATION_VOIP, &err);
        if (err != OPUS_OK) {
            m_encoder = nullptr;
            return false;
        }
        opus_encoder_ctl(m_encoder, OPUS_SET_BITRATE(64000));

        QAudioFormat fmt;
        fmt.setSampleRate(48000);
        fmt.setChannelCount(1);
        // Prefer float capture (what CoreAudio produces natively, and what
        // Nightjar's SDL capture requests); fall back to Int16 when the
        // device does not offer float. Opus encodes float directly, so no
        // conversion step is needed either way.
        fmt.setSampleFormat(QAudioFormat::Float);

        QAudioDevice device = QMediaDevices::defaultAudioInput();
        if (device.isNull()) {
            qWarning() << "[MicStream] No default audio input device available";
            cleanupAudioResources();
            return false;
        }

        m_useFloat = device.isFormatSupported(fmt);
        if (!m_useFloat) {
            fmt.setSampleFormat(QAudioFormat::Int16);
            if (!device.isFormatSupported(fmt)) {
                const QAudioFormat preferredFormat = device.preferredFormat();
                qWarning() << "[MicStream] Default audio input does not support required format: 48000 Hz mono Float or Int16"
                           << "device=" << device.description()
                           << "preferred=" << preferredFormat.sampleRate() << "Hz"
                           << preferredFormat.channelCount() << "channels"
                           << "sampleFormat=" << preferredFormat.sampleFormat();
                cleanupAudioResources();
                return false;
            }
        }
        m_pcmFrameSize = PCM_FRAME_SAMPLES * (m_useFloat ? 4 : 2);

        qInfo() << "[MicStream] Using audio input device:" << device.description()
                << "format=" << fmt.sampleRate() << "Hz" << fmt.channelCount() << "channels" << "sampleFormat=" << fmt.sampleFormat();

        m_audioInput = new QAudioSource(device, fmt, this);
        m_audioInput->setBufferSize(m_pcmFrameSize * 4);
        connect(m_audioInput, &QAudioSource::stateChanged, this, [this](QAudio::State state) {
            qInfo() << "[MicStream] Audio state changed state=" << state
                    << "error=" << m_audioInput->error();
        });

        m_audioDevice = m_audioInput->start();
        if (!m_audioDevice || m_audioInput->error() != QAudio::NoError) {
            qWarning() << "[MicStream] Failed to start audio device error=" << m_audioInput->error();
            cleanupAudioResources();
            return false;
        }

        qInfo() << "[MicStream] Audio device initialized successfully";
        connect(m_audioDevice, &QIODevice::readyRead, this, &MicStreamWorker::onAudio);

        resetStatistics();
        m_partialBuffer.clear();
        m_queue.clear();
        m_audioContinuationPending = false;

        m_sendTimer = new QTimer(this);
        m_sendTimer->setInterval(10);
        connect(m_sendTimer, &QTimer::timeout, this, &MicStreamWorker::sendLoop);
        m_sendTimer->start();

        m_logTimer = new QTimer(this);
        m_logTimer->setInterval(5000);
        connect(m_logTimer, &QTimer::timeout, this, &MicStreamWorker::logSummary);
        m_logTimer->start();

        qInfo() << "[MicStream] start thread=" << QThread::currentThread()->objectName();
        return true;
    }

    void stop()
    {
        Q_ASSERT(QThread::currentThread() == thread());
        if (!m_audioInput && !m_encoder)
            return;

        if (m_sendTimer)
            m_sendTimer->stop();
        if (m_logTimer)
            m_logTimer->stop();
        logSummary();
        cleanupAudioResources();

        m_partialBuffer.clear();
        m_queue.clear();
        m_audioContinuationPending = false;
        qInfo() << "[MicStream] stop";
    }

private:
    void cleanupAudioResources()
    {
        if (m_audioInput) {
            m_audioInput->stop();
            delete m_audioInput;
            m_audioInput = nullptr;
            m_audioDevice = nullptr;
        }
        if (m_encoder) {
            opus_encoder_destroy(m_encoder);
            m_encoder = nullptr;
        }
    }

    void onAudio()
    {
        if (!m_audioDevice || !m_encoder)
            return;

        QElapsedTimer callbackTimer;
        callbackTimer.start();

        QByteArray chunk = m_audioDevice->readAll();
        if (!chunk.isEmpty()) {
            m_partialBuffer.append(chunk);
            trimPcmBacklog();
            processPendingAudio();
        }

        m_maxAudioCallbackMs = qMax(m_maxAudioCallbackMs, callbackTimer.elapsed());
    }

    void trimPcmBacklog()
    {
        const int maxBytes = MAX_PENDING_PCM_FRAMES * m_pcmFrameSize;
        if (m_partialBuffer.size() <= maxBytes)
            return;

        const int excessBytes = m_partialBuffer.size() - maxBytes;
        const int framesToDrop = (excessBytes + m_pcmFrameSize - 1) / m_pcmFrameSize;
        const int completeBytes = m_partialBuffer.size() - (m_partialBuffer.size() % m_pcmFrameSize);
        const int bytesToDrop = qMin(framesToDrop * m_pcmFrameSize, completeBytes);
        m_partialBuffer.remove(0, bytesToDrop);
        m_droppedPcmFrames += bytesToDrop / m_pcmFrameSize;
    }

    void processPendingAudio()
    {
        if (!m_encoder)
            return;

        int processedFrames = 0;
        while (m_partialBuffer.size() >= m_pcmFrameSize &&
               processedFrames < MAX_ENCODE_FRAMES_PER_PASS) {
            QByteArray pcm = m_partialBuffer.left(m_pcmFrameSize);
            m_partialBuffer.remove(0, m_pcmFrameSize);
            m_pcmBytes += pcm.size();

            unsigned char encoded[MAX_OPUS_SIZE];
            int len;
            if (m_useFloat) {
                len = opus_encode_float(m_encoder,
                                        reinterpret_cast<const float*>(pcm.constData()),
                                        PCM_FRAME_SAMPLES,
                                        encoded,
                                        MAX_OPUS_SIZE);
            } else {
                len = opus_encode(m_encoder,
                                  reinterpret_cast<const opus_int16*>(pcm.constData()),
                                  PCM_FRAME_SAMPLES,
                                  encoded,
                                  MAX_OPUS_SIZE);
            }
            if (len > 0) {
                while (m_queue.size() >= MAX_PENDING_OPUS_FRAMES) {
                    m_queue.dequeue();
                    m_droppedOpusFrames++;
                }
                m_queue.enqueue(QByteArray(reinterpret_cast<char*>(encoded), len));
                m_opusBytes += len;
            } else {
                qWarning() << "[MicStream] Opus encode failed len=" << len;
            }
            processedFrames++;
        }

        if (m_partialBuffer.size() >= m_pcmFrameSize && !m_audioContinuationPending) {
            m_audioContinuationPending = true;
            QTimer::singleShot(0, this, [this]() {
                m_audioContinuationPending = false;
                processPendingAudio();
            });
        }
    }

    void sendLoop()
    {
        if (m_queue.isEmpty()) {
            m_idleLoops++;
            return;
        }

        int processedFrames = 0;
        while (!m_queue.isEmpty() && processedFrames < MAX_SEND_FRAMES_PER_PASS) {
            QByteArray opus = m_queue.dequeue();
            int opusLen = opus.size();

            // The Apollo mic datagram must stay under the common-c cap.
            // Drop oversized frames rather than splitting them; at
            // 64 kbps this should effectively never trigger
            // (~160 bytes/frame typical).
            if (opusLen > MAX_OPUS_SEND_SIZE) {
                m_droppedOversizedFrames++;
                processedFrames++;
                continue;
            }

            // common-c adds the 12-byte Apollo header, sequences the
            // packet, and AES-encrypts it when mic encryption was
            // negotiated.
            int rc = LiSendMicrophoneOpusData(reinterpret_cast<const unsigned char*>(opus.constData()),
                                              opusLen);
            if (rc != 0) {
                // Don't flood the log at 100 packets/sec; the 5s summary
                // reports the running error count.
                if (m_sendErrors == 0) {
                    qWarning() << "[MicStream] LiSendMicrophoneOpusData failed rc=" << rc;
                }
                m_sendErrors++;
            } else {
                m_sentPackets++;
                m_sentBytes += opusLen;
            }
            processedFrames++;
        }
    }

    void logSummary()
    {
        const int state = m_audioInput ? static_cast<int>(m_audioInput->state()) : -1;
        const int error = m_audioInput ? static_cast<int>(m_audioInput->error()) : -1;
        qInfo() << "[MicStream] 5s summary pcm=" << m_pcmBytes
                << "B opus=" << m_opusBytes
                << "B sent=" << m_sentPackets << "/" << m_sentBytes
                << "B sendErr=" << m_sendErrors
                << "B idle=" << m_idleLoops
                << "queue=" << m_queue.size()
                << "pcmDrop=" << m_droppedPcmFrames
                << "opusDrop=" << m_droppedOpusFrames
                << "oversizeDrop=" << m_droppedOversizedFrames
                << "maxAudioMs=" << m_maxAudioCallbackMs
                << "state=" << state
                << "error=" << error;
        resetStatistics();
    }

    void resetStatistics()
    {
        m_pcmBytes = 0;
        m_opusBytes = 0;
        m_sentBytes = 0;
        m_sentPackets = 0;
        m_sendErrors = 0;
        m_idleLoops = 0;
        m_droppedPcmFrames = 0;
        m_droppedOpusFrames = 0;
        m_droppedOversizedFrames = 0;
        m_maxAudioCallbackMs = 0;
    }

    QAudioSource *m_audioInput;
    QIODevice *m_audioDevice;
    OpusEncoder *m_encoder;
    QTimer *m_sendTimer;
    QTimer *m_logTimer;
    QQueue<QByteArray> m_queue;
    bool m_audioContinuationPending;
    bool m_useFloat; // true when capturing Float (preferred), false for the Int16 fallback
    int m_pcmFrameSize; // bytes per frame for the active capture format
    quint64 m_pcmBytes;
    quint64 m_opusBytes;
    quint64 m_sentBytes;
    int m_sentPackets;
    int m_sendErrors;
    int m_idleLoops;
    int m_droppedPcmFrames;
    int m_droppedOpusFrames;
    int m_droppedOversizedFrames;
    qint64 m_maxAudioCallbackMs;
    QByteArray m_partialBuffer;
};

MicStream::MicStream(QObject *parent)
    : QObject(parent),
      m_thread(nullptr),
      m_worker(nullptr)
{
}

MicStream::~MicStream()
{
    stop();
}

bool MicStream::start()
{
    if (m_thread)
        return false;

    // Keep the macOS permission request on the application thread. Touching
    // QMediaDevices and creating QAudioSource happens in the worker thread.
    if (!checkAndRequestMicrophonePermission()) {
        qWarning() << "[MicStream] Microphone permission denied";
        return false;
    }

    QThread *thread = new QThread(this);
    thread->setObjectName("Microphone Streaming Thread");
    MicStreamWorker *worker = new MicStreamWorker();
    worker->moveToThread(thread);
    connect(thread, &QThread::finished, worker, &QObject::deleteLater);
    thread->start();

    bool started = false;
    const bool invoked = QMetaObject::invokeMethod(worker, [worker, &started]() {
        started = worker->start();
    }, Qt::BlockingQueuedConnection);

    if (!invoked || !started) {
        qWarning() << "[MicStream] Failed to start microphone worker";
        thread->quit();
        thread->wait();
        delete thread;
        return false;
    }

    m_thread = thread;
    m_worker = worker;
    return true;
}

void MicStream::stop()
{
    if (!m_thread)
        return;

    Q_ASSERT(QThread::currentThread() != m_thread);
    if (m_worker && m_thread->isRunning()) {
        QMetaObject::invokeMethod(m_worker, [worker = m_worker]() {
            worker->stop();
        }, Qt::BlockingQueuedConnection);
    }

    m_thread->quit();
    m_thread->wait();
    delete m_thread;
    m_thread = nullptr;
    m_worker = nullptr;
}

bool MicStream::probe(QString* errorMessage)
{
    // Keep the macOS permission request on the calling thread (initialize
    // runs on the GUI thread, the same thread the real start uses).
    if (!checkAndRequestMicrophonePermission()) {
        *errorMessage = "microphone permission was denied";
        return false;
    }

    QAudioDevice device = QMediaDevices::defaultAudioInput();
    if (device.isNull()) {
        *errorMessage = "no audio input device is available";
        return false;
    }

    QAudioFormat fmt;
    fmt.setSampleRate(48000);
    fmt.setChannelCount(1);
    fmt.setSampleFormat(QAudioFormat::Float);
    if (device.isFormatSupported(fmt)) {
        return true;
    }
    fmt.setSampleFormat(QAudioFormat::Int16);
    if (device.isFormatSupported(fmt)) {
        return true;
    }
    *errorMessage = "the default microphone does not support 48000 Hz mono audio";
    return false;
}
