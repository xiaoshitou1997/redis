package io.github.redis.security;

import io.github.redis.config.RedisConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import javax.net.ssl.SSLException;
import java.io.File;
import java.security.cert.CertificateException;

/**
 * Builds Netty {@link SslContext}s from {@link RedisConfig}'s TLS directives.
 *
 * <p>If no cert/key files are configured but TLS is enabled, we fall back to a
 * generated self-signed certificate. This is suitable for development and
 * smoke tests; production deployments should always provide explicit certs.
 */
public final class TlsFactory {

    private TlsFactory() {}

    /** Build a server-side SSL context. Returns null if TLS is disabled. */
    public static SslContext buildServerContext(RedisConfig config) throws SSLException {
        if (config == null || !config.isTlsEnabled()) return null;

        File cert = config.getTlsCertFile() == null ? null : new File(config.getTlsCertFile());
        File key  = config.getTlsKeyFile()  == null ? null : new File(config.getTlsKeyFile());

        if (cert == null || key == null || !cert.exists() || !key.exists()) {
            // Self-signed fallback for development.
            try {
                SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
                cert = ssc.certificate();
                key  = ssc.privateKey();
            } catch (CertificateException e) {
                throw new SSLException("Failed to generate self-signed certificate", e);
            }
        }

        SslContextBuilder builder = SslContextBuilder.forServer(cert, key)
                .sslProvider(SslProvider.OPENSSL)
                .clientAuth(config.isTlsAuthClients() ? ClientAuth.REQUIRE : ClientAuth.NONE);

        File caCert = config.getTlsCaCertFile() == null ? null : new File(config.getTlsCaCertFile());
        if (caCert != null && caCert.exists()) {
            builder.trustManager(caCert);
        }

        return builder.build();
    }
}
