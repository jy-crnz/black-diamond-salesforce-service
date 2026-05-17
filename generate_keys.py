import datetime
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.x509.oid import NameOID
from cryptography import x509


def generate_salesforce_keys():
    print("🔐 Generating 2048-bit RSA Private Key...")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Write private key to private.pem
    with open("private.pem", "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    print("📜 Generating X.509 Public Certificate...")
    # Create a dummy identity for the dev certificate
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "GlynacDevIntegration"),
        ]
    )

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365)
        )
        .sign(private_key, hashes.SHA256())
    )

    # Write public cert to public.crt
    with open("public.crt", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print(
        "✅ SUCCESS: 'private.pem' and 'public.crt' have been created in your folder!"
    )


if __name__ == "__main__":
    generate_salesforce_keys()
