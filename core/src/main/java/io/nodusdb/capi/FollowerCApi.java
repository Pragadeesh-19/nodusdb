package io.nodusdb.capi;

import io.nodusdb.replica.FollowerConfig;
import io.nodusdb.replica.Restore;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class FollowerCApi {

    private FollowerCApi() {
    }

    @CEntryPoint(name = "nodus_open_follower")
    public static VoidPointer openFollower(IsolateThread thread, CCharPointer configUtf8, int configLength,
                                           long maxMemoryBytes, CIntPointer status) {
        try {
            FollowerConfig config = FollowerConfig.parse(
                    new String(NodusCApi.readBytes(configUtf8, configLength), StandardCharsets.UTF_8));
            long handle = NodusCApi.SESSIONS.openFollower(config, maxMemoryBytes);
            NodusCApi.report(status, NodusCApi.OK);
            return WordFactory.pointer(handle);
        } catch (IOException | RuntimeException e) {
            NodusCApi.report(status, Failures.codeOf(e));
        }
        return WordFactory.nullPointer();
    }

    @CEntryPoint(name = "nodus_restore")
    public static int restore(IsolateThread thread, CCharPointer configUtf8, int configLength, CCharPointer target,
                              CLongPointer reportOut) {
        try {
            FollowerConfig config = FollowerConfig.parse(
                    new String(NodusCApi.readBytes(configUtf8, configLength), StandardCharsets.UTF_8));
            Restore.Restored restored = Restore.restore(config, Path.of(NodusCApi.cString(target)));
            if (reportOut.isNonNull()) {
                reportOut.write(0, restored.appliedLsn());
                reportOut.write(1, restored.epoch());
                reportOut.write(2, restored.snapshotLsn());
            }
            return NodusCApi.OK;
        } catch (IOException | RuntimeException e) {
            return Failures.codeOf(e);
        }
    }
}
