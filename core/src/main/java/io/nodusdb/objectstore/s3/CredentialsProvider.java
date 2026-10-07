package io.nodusdb.objectstore.s3;

public interface CredentialsProvider {

    Credentials current();

    Credentials refresh();
}
