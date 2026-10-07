package io.nodusdb.log.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class DirectoryLogFileSystem implements LogFileSystem {

    private final Path directory;

    public DirectoryLogFileSystem(Path directory) {
        this.directory = directory;
    }

    @Override
    public List<String> list() throws IOException {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(directory)) {
            List<String> names = new ArrayList<>();
            entries.filter(Files::isRegularFile).forEach(path -> names.add(path.getFileName().toString()));
            return names;
        }
    }

    @Override
    public boolean exists(String name) {
        return Files.isRegularFile(directory.resolve(name));
    }

    @Override
    public LogChannel create(String name) throws IOException {
        Files.createDirectories(directory);
        return new FileLogChannel(FileChannel.open(directory.resolve(name), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE));
    }

    @Override
    public LogChannel open(String name) throws IOException {
        return new FileLogChannel(FileChannel.open(directory.resolve(name), StandardOpenOption.READ,
                StandardOpenOption.WRITE));
    }

    @Override
    public void delete(String name) throws IOException {
        Files.deleteIfExists(directory.resolve(name));
    }

    @Override
    public void trySyncDirectory() {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            return;
        }
    }

    private record FileLogChannel(FileChannel channel) implements LogChannel {

        @Override
        public int read(ByteBuffer destination, long position) throws IOException {
            return channel.read(destination, position);
        }

        @Override
        public int write(ByteBuffer source, long position) throws IOException {
            int total = 0;
            while (source.hasRemaining()) {
                total += channel.write(source, position + total);
            }
            return total;
        }

        @Override
        public long size() throws IOException {
            return channel.size();
        }

        @Override
        public void truncate(long size) throws IOException {
            channel.truncate(size);
        }

        @Override
        public void force() throws IOException {
            channel.force(false);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
