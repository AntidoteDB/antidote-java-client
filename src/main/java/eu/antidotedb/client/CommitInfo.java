package eu.antidotedb.client;

import com.google.protobuf.ByteString;

public class CommitInfo {
    private final ByteString commitTime;

    public CommitInfo(ByteString commitTime) {
        this.commitTime = commitTime;
    }

    public ByteString getCommitTime() {
        return commitTime;
    }
}
