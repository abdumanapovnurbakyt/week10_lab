"""Выпуск учебных сертификатов для TLS на участке шлюз -> облачный брокер.

  python -m tools.gen_certs /certs          # создаёт ca.crt, broker.crt, broker.key (если их ещё нет)

CA самоподписанный; сертификат брокера действителен для имён broker-wan, wan-link, localhost, 127.0.0.1.
Шлюз проверяет сертификат по ca.crt (с проверкой имени), поэтому TLS-соединение настоящее, а не «insecure».
"""
import datetime as dt
import ipaddress
import os
import sys

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Lab7 IoT Gateway"),
                      x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def generate(out: str, force: bool = False) -> bool:
    os.makedirs(out, exist_ok=True)
    paths = [os.path.join(out, n) for n in ("ca.crt", "broker.crt", "broker.key")]
    if all(os.path.exists(p) for p in paths) and not force:
        return False
    now = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (x509.CertificateBuilder().subject_name(_name("Lab7 Root CA")).issuer_name(_name("Lab7 Root CA"))
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now).not_valid_after(now + dt.timedelta(days=3650))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
                                       key_encipherment=False, data_encipherment=False, key_agreement=False,
                                       encipher_only=False, decipher_only=False), critical=True)
          .sign(ca_key, hashes.SHA256()))
    key = ec.generate_private_key(ec.SECP256R1())
    sans = [x509.DNSName("broker-wan"), x509.DNSName("wan-link"), x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    cert = (x509.CertificateBuilder().subject_name(_name("broker-wan")).issuer_name(ca.subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now).not_valid_after(now + dt.timedelta(days=825))
            .add_extension(x509.SubjectAlternativeName(sans), critical=False)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    with open(paths[0], "wb") as f:
        f.write(ca.public_bytes(serialization.Encoding.PEM))
    with open(paths[1], "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(paths[2], "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                                  serialization.NoEncryption()))
    for p in paths:
        os.chmod(p, 0o644)          # учебный стенд: ключ должен читаться пользователем mosquitto внутри контейнера
    return True


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/certs"
    print(("сертификаты созданы в " if generate(target, "--force" in sys.argv) else "сертификаты уже есть в ") + target)
