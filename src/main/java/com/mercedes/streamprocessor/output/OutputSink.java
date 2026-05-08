package com.mercedes.streamprocessor.output;

import com.mercedes.streamprocessor.model.AttributedPageView;

/**
 * Durable sink for attributed page view records.
 * flush() must complete before offsets are committed.
 */
public interface OutputSink {

    void write(AttributedPageView record);

    void flush();
}
