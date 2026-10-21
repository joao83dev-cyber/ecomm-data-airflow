import base64
import hashlib
import unittest

from app.connection_producer import PBKDF2_ITERATIONS, generate_connection_batch, hash_mock_password


class ConnectionProducerTests(unittest.TestCase):
    def test_generates_requested_count_with_hashed_passwords(self) -> None:
        batch = generate_connection_batch(2)

        self.assertEqual(len(batch.records), 2)
        self.assertNotEqual(batch.records[0].userid, batch.records[1].userid)
        self.assertNotEqual(batch.records[0].login, batch.records[1].login)
        for record in batch.records:
            self.assertTrue(record.login)
            self.assertEqual(record.date_access.tzinfo is not None, True)
            self.assertTrue(record.hashpass.startswith("pbkdf2_sha256$"))

    def test_password_hash_can_be_verified_without_storing_plaintext(self) -> None:
        password = "mock-password-only"
        algorithm, iterations, salt, expected = hash_mock_password(password).split("$")
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            base64.urlsafe_b64decode(salt.encode("ascii")),
            int(iterations),
        )

        self.assertEqual(algorithm, "pbkdf2_sha256")
        self.assertEqual(int(iterations), PBKDF2_ITERATIONS)
        self.assertEqual(base64.urlsafe_b64decode(expected.encode("ascii")), actual)

    def test_rejects_more_than_200_records(self) -> None:
        with self.assertRaises(ValueError):
            generate_connection_batch(201)


if __name__ == "__main__":
    unittest.main()
