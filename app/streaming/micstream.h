#pragma once

#include <QObject>
#include <QString>
class QThread;
class MicStreamWorker;

class MicStream : public QObject
{
    Q_OBJECT

public:
    explicit MicStream(QObject *parent = nullptr);
    ~MicStream();

    bool start();
    void stop();

    // Pre-launch check used for the StreamSegue toast: permission, a
    // default device, and a usable capture format. Warning-only; it never
    // blocks streaming. Qt6-only file.
    static bool probe(QString* errorMessage);

private:
    QThread *m_thread;
    MicStreamWorker *m_worker;
};
