package io.nodusdb.capi;

import io.nodusdb.chain.KeyFiles;
import io.nodusdb.kernel.Token;
import io.nodusdb.log.LogConfig;
import io.nodusdb.log.SyncMode;
import io.nodusdb.ship.ShippingConfig;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;
import org.graalvm.nativeimage.c.type.VoidPointer;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

public final class ShippingCApi {

    private static final long MAX_WAIT_MILLIS = Duration.ofDays(3_650).toMillis();

    private ShippingCApi() {
    }

    @CEntryPoint(name = "nodus_open_durable_shipping")
    public static VoidPointer openDurableShipping(IsolateThread thread, CCharPointer directory, int syncMode,
                                                  long maxMemoryBytes, CCharPointer configUtf8, int configLength,
                                                  CIntPointer status) {
        try {
            LogConfig config = LogConfig.withSyncMode(SyncMode.fromCode(syncMode));
            ShippingConfig shipping = ShippingConfig.parse(
                    new String(NodusCApi.readBytes(configUtf8, configLength), StandardCharsets.UTF_8));
            long handle = NodusCApi.SESSIONS.openDurable(Path.of(NodusCApi.cString(directory)), config,
                    maxMemoryBytes, shipping);
            NodusCApi.report(status, NodusCApi.OK);
            return WordFactory.pointer(handle);
        } catch (IOException | RuntimeException e) {
            NodusCApi.report(status, Failures.codeOf(e));
        }
        return WordFactory.nullPointer();
    }

    @CEntryPoint(name = "nodus_await_shipped")
    public static int awaitShipped(IsolateThread thread, VoidPointer handle, long epoch, long lsn, long timeoutMillis) {
        try {
            long bounded = Math.min(timeoutMillis, MAX_WAIT_MILLIS);
            NodusCApi.session(handle).awaitShipped(new Token(epoch, lsn), Duration.ofMillis(bounded));
            return NodusCApi.OK;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_stats_json")
    public static int statsJson(IsolateThread thread, VoidPointer handle, CCharPointer outBuf, int outCap) {
        try {
            byte[] json = NodusCApi.session(handle).statsJson().getBytes(StandardCharsets.UTF_8);
            int count = Math.min(json.length, Math.max(outCap, 0));
            for (int i = 0; i < count; i++) {
                outBuf.write(i, json[i]);
            }
            return json.length;
        } catch (RuntimeException e) {
            return Failures.codeOf(e);
        }
    }

    @CEntryPoint(name = "nodus_signing_key_generate")
    public static int signingKeyGenerate(IsolateThread thread, CCharPointer privatePath, CCharPointer publicPath) {
        try {
            KeyFiles.generateTo(Path.of(NodusCApi.cString(privatePath)), Path.of(NodusCApi.cString(publicPath)));
            return NodusCApi.OK;
        } catch (IOException | RuntimeException e) {
            return Failures.codeOf(e);
        }
    }
}
