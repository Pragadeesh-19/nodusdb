package io.nodusdb.objectstore.s3;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

final class TrustStores {

    private TrustStores() {
    }

    static SSLContext fromBundle(Path pemFile) {
        try (InputStream in = Files.newInputStream(pemFile)) {
            Collection<? extends Certificate> certificates =
                    CertificateFactory.getInstance("X.509").generateCertificates(in);
            if (certificates.isEmpty()) {
                throw new IllegalArgumentException("the CA bundle holds no certificate: " + pemFile.getFileName());
            }
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int index = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("ca-" + index++, certificate);
            }
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("the CA bundle cannot be used: " + pemFile.getFileName(), e);
        }
    }
}
