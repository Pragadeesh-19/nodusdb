package io.nodusdb.objectstore.s3;

import io.nodusdb.objectstore.ObjectStore;
import io.nodusdb.objectstore.ObjectStoreContractTest;
import org.junit.jupiter.api.AfterEach;

import java.time.Clock;

class S3ObjectStoreContractTest extends ObjectStoreContractTest {

    private FakeS3Server server;

    @Override
    protected ObjectStore create() {
        server = FakeS3Server.start();
        return new S3ObjectStore(server.config(), new StaticCredentialsProvider(FakeS3Server.CREDENTIALS),
                Clock.systemUTC());
    }

    @AfterEach
    void stopServer() {
        server.close();
    }
}
