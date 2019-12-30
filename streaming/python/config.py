class Config:
    '''
    WARNING:
        Do not modify this file easily.
        Please make sure every config is correct according to the config file
        under 'java/streaming-runtime/config'.
    '''
    # common
    STREAMING_JOB_NAME = "streaming.job.name"
    STREAMING_OP_NAME = "streaming.worker.operator.name"
    TASK_JOB_ID = "streaming.job.id"
    STREAMING_WORKER_NAME = "streaming.worker.name"

    # channel
    CHANNEL_TYPE = "streaming.transfer.channel.type"
    MEMORY_CHANNEL = "memory_channel"
    NATIVE_CHANNEL = "native_channel"

    CHANNEL_SIZE = "streaming.transfer.channel.size"
    CHANNEL_SIZE_DEFAULT = 10**8

    IS_RECREATE = "streaming.transfer.channel.is-recreate"

    # return from StreamingReader.getBundle if only empty message read in this
    # interval.
    TIMER_INTERVAL_MS = "streaming.transfer.channel.timer.interval.ms"

    STREAMING_RING_BUFFER_CAPACITY = "streaming.transfer.ring-buffer.capacity"

    # write an empty message if there is no data to be written in this
    # interval.
    STREAMING_EMPTY_MESSAGE_INTERVAL = "streaming.transfer.empty-message.interval"

    # operator type
    OPERATOR_TYPE = "streaming.worker.type"
