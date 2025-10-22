import base64
import hashlib
import logging
import os
from pathlib import Path
import time
from typing import Optional

import paramiko
import redis

from logger.setup_logging import setup_logging


logger = logging.getLogger("uploader")

class VerifyFingerprintPolicy(paramiko.MissingHostKeyPolicy):
    """
    Accept the server only if its host key SHA256 fingerprint matches expected.
    """
    def __init__(self, expected_fingerprints):
        if isinstance(expected_fingerprints, str):
            expected_fingerprints = [expected_fingerprints]
        self.expected = set(expected_fingerprints)

    def missing_host_key(self, client, hostname, key):
        fp = "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode()
        if fp not in self.expected:
            raise paramiko.SSHException(
                f"Host key mismatch for {hostname}"
            )
        # Accept and remember so future connections don't trigger this again
        client._host_keys.add(hostname, key.get_name(), key)

def newest_file(dirpath: Path) -> Optional[Path]:
    """
    Find the newst file my mtime in a folder.
    """
    files = [p for p in dirpath.iterdir() if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)

def is_file_stable(p: Path, settle_sec: float = 1.0) -> bool:
    """
    Avoid uploading a file that is still being written.
    """
    try:
        s1 = p.stat().st_size
        time.sleep(settle_sec)
        s2 = p.stat().st_size
        return s1 == s2
    except FileNotFoundError:
        return False

def remote_file_size(sftp: paramiko.SFTPClient, remote_path: str) -> Optional[int]:
    try:
        return sftp.stat(remote_path).st_size
    except FileNotFoundError:
        return None

def upload_if_needed(sftp: paramiko.SFTPClient, local_file: Path, remote_dir: str) -> bool:
    """
    Uploads a file onto remote dir. Decision for upload is based on if file
    already exists on remote dir and if it is the same size as local file.

    Args:
        sftp: SFTP Client.
        local_file: Path object of the files to be uploaded.
        remote_dir: String of the upload destination.
    
    Returns:
        bool: True if file uploaded, false if skipped.
    """
    remote_final = f"{remote_dir.rstrip('/')}/{local_file.name}"

    # Skip if remote exists with same size
    remote_size = remote_file_size(sftp, remote_final)
    local_size = local_file.stat().st_size
    if remote_size is not None and remote_size == local_size:
        return False  
    elif remote_size is not None and remote_size != local_size:
        # Add suffix if same file already exists
        ts = int(local_file.stat().st_mtime)
        remote_final = f"{remote_final}.dup_{ts}"

    try:
        sftp.put(str(local_file), remote_final)
    except IOError:
        logger.error(f"File {local_file.name} could note be uploaded to the remote server")
        raise

    return True

def uploader_local_gufeng(
    host: str,
    user: str,
    password: str,
    local_dir: str,
    remote_dir: str,
    interval_sec: int = 30,
) -> None:
    """
    Every 'interval_sec':
      - pick newest file in 'local_dir'
      - if file is stable and not already present remotely (same size), upload it
    """
    local_path = Path(local_dir)

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(VerifyFingerprintPolicy(os.getenv("LPI_HOST_KEY")))
    ssh.connect(host, 22, user, password=password, timeout=10)

    ssh.get_transport().set_keepalive(30)

    sftp = ssh.open_sftp()
    try:
        last_uploaded_name: Optional[str] = None

        while True:
            nf = newest_file(local_path)
            if nf and is_file_stable(nf):
                try:
                    # Local de-dup guard in case remote dir is cleared.
                    if nf.name != last_uploaded_name:
                        uploaded = upload_if_needed(sftp, nf, remote_dir)
                        if uploaded:
                            last_uploaded_name = nf.name
                            logger.debug(f"File {nf.name} has been successfully uploaded to remote server.")
                except Exception:
                    logger.exception(f"File {nf.name} could not be uploaded to remote server, skip.")
                    time.sleep(5)

            time.sleep(interval_sec)

    finally:
        try:
            sftp.close()
        except Exception:
            pass
        ssh.close()

def main():
    setup_logging(process_name="uploader")

    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST","redis"),
        port=int(os.getenv("REDIS_PORT",6379)),
        db=int(os.getenv("REDIS_DB",0)),
        decode_responses=True
    )

    while True:
        uploader_local_gufeng(
            host=os.getenv("LPI_SFTP_HOST"),
            user=os.getenv("LPI_SFTP_USER"),
            password=os.getenv("LPI_SFTP_PASSWORD"),
            local_dir="/app/files/finished_100hz",
            remote_dir="/FTPServer/Messtechnik/M2412511/data/Logger1_100Hz_30sek"
        )

if __name__ == "__main__":
    main()
