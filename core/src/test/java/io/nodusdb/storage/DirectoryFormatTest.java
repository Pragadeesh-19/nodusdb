package io.nodusdb.storage;

import io.nodusdb.error.UnsupportedFeatureException;
import io.nodusdb.storage.DirectoryFormat.Layout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirectoryFormatTest {

    @TempDir
    Path directory;

    private void tripwire(int version) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(DirectoryFormat.TRIPWIRE_BYTES);
        header.putInt(0x4E4F4455).putShort((short) version).putShort((short) 0).putLong(0L);
        Files.write(directory.resolve(GraphFiles.TRIPWIRE), header.array());
    }

    private void format(String text) throws IOException {
        Files.writeString(directory.resolve(GraphFiles.FORMAT), text, StandardCharsets.US_ASCII);
    }

    @Test
    void anEmptyDirectoryIsNew() throws IOException {
        assertEquals(Layout.NEW, DirectoryFormat.detect(directory));
    }

    @Test
    void theFormatMarkerNamesTheCurrentVersion() throws IOException {
        format("nodus-format 2\n");

        assertEquals(Layout.CURRENT, DirectoryFormat.detect(directory));
    }

    @Test
    void aLaterFormatIsRefusedAndAnUnreadableMarkerIsAnError() throws IOException {
        format("nodus-format 3\n");
        assertThrows(UnsupportedFeatureException.class, () -> DirectoryFormat.detect(directory));

        format("something else\n");
        assertThrows(IOException.class, () -> DirectoryFormat.detect(directory));

        format("nodus-format two\n");
        assertThrows(IOException.class, () -> DirectoryFormat.detect(directory));
    }

    @Test
    void anEarlierLogMeansALegacyDirectory() throws IOException {
        tripwire(1);

        assertEquals(Layout.LEGACY, DirectoryFormat.detect(directory));
    }

    @Test
    void aSnapshotOrSymbolFileAloneMeansALegacyDirectory() throws IOException {
        Files.write(directory.resolve(GraphFiles.SNAPSHOT), new byte[] {1});
        assertEquals(Layout.LEGACY, DirectoryFormat.detect(directory));

        Files.delete(directory.resolve(GraphFiles.SNAPSHOT));
        Files.write(directory.resolve(GraphFiles.LEGACY_SYMBOLS), new byte[] {1});
        assertEquals(Layout.LEGACY, DirectoryFormat.detect(directory));
    }

    @Test
    void aCurrentTripwireWithoutAFormatMarkerIsACreationThatDidNotFinish() throws IOException {
        tripwire(2);

        assertEquals(Layout.NEW, DirectoryFormat.detect(directory));
    }

    @Test
    void aBackupOrStagingDirectoryMeansAnUpgradeWasInterrupted() throws IOException {
        tripwire(2);
        Files.createDirectory(directory.resolve(GraphFiles.BACKUP_DIRECTORY));
        assertEquals(Layout.INTERRUPTED_UPGRADE, DirectoryFormat.detect(directory));

        Files.delete(directory.resolve(GraphFiles.BACKUP_DIRECTORY));
        Files.createDirectory(directory.resolve(GraphFiles.STAGING_DIRECTORY));
        assertEquals(Layout.INTERRUPTED_UPGRADE, DirectoryFormat.detect(directory));
    }

    @Test
    void aPartialBackupAloneLeavesTheDirectoryLegacy() throws IOException {
        Files.write(directory.resolve(GraphFiles.SNAPSHOT), new byte[] {1});
        Files.createDirectory(directory.resolve(GraphFiles.BACKUP_DIRECTORY + ".partial"));

        assertEquals(Layout.LEGACY, DirectoryFormat.detect(directory));
    }

    @Test
    void theFormatMarkerWinsOverLeftoversOfAnUpgrade() throws IOException {
        format("nodus-format 2\n");
        Files.createDirectory(directory.resolve(GraphFiles.BACKUP_DIRECTORY));
        Files.createDirectory(directory.resolve(GraphFiles.STAGING_DIRECTORY));

        assertEquals(Layout.CURRENT, DirectoryFormat.detect(directory));
    }

    @Test
    void aLogFromALaterVersionIsRefusedAndAForeignFileIsAnError() throws IOException {
        tripwire(3);
        assertThrows(UnsupportedFeatureException.class, () -> DirectoryFormat.detect(directory));

        Files.write(directory.resolve(GraphFiles.TRIPWIRE), new byte[DirectoryFormat.TRIPWIRE_BYTES]);
        assertThrows(IOException.class, () -> DirectoryFormat.detect(directory));
    }

    @Test
    void writingTheMarkerAndTheTripwireRoundTrips() throws IOException {
        DirectoryFormat.writeTripwire(directory);
        assertEquals(DirectoryFormat.CURRENT_VERSION,
                DirectoryFormat.readTripwireVersion(directory.resolve(GraphFiles.TRIPWIRE)));
        assertEquals(Layout.NEW, DirectoryFormat.detect(directory));

        DirectoryFormat.writeFormat(directory);
        assertEquals(Layout.CURRENT, DirectoryFormat.detect(directory));
    }
}
