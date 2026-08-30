"""Unit tests for Worker SSH/AWS connectivity hardening (no real network)."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import paramiko
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from backend.services.cloud_provider import AWSProvider, build_worker_cloud_init
import backend.services.cloud_provider as cloud_provider_module
from backend.services.worker_provisioner import (
    WORKER_CODEX_CLI_VERSION,
    WorkerProvisioner,
)
from backend.services.ssh_executor import (
    SSHHostKeyMismatchError,
    SSHExecutor,
    SSHKeyPreflightError,
    derive_openssh_public_key,
    openssh_public_key_fingerprint,
    preflight_private_key,
    probe_ssh_host_key,
    validate_openssh_public_key,
    worker_known_hosts_path,
)


def _private_key_file(
    tmp_path: Path,
    *,
    mode: int = 0o600,
    name: str = "worker-key",
) -> Path:
    private_key = ed25519.Ed25519PrivateKey.generate()
    data = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / name
    path.write_bytes(data)
    path.chmod(mode)
    return path


def test_private_key_preflight_derives_comment_free_openssh_key(tmp_path):
    path = _private_key_file(tmp_path)

    material = preflight_private_key(path)

    assert material.private_key_path == str(path)
    assert material.openssh_public_key.startswith("ssh-ed25519 ")
    assert len(material.openssh_public_key.split()) == 2
    assert derive_openssh_public_key(path) == material.openssh_public_key
    assert validate_openssh_public_key(material.openssh_public_key) == material.openssh_public_key


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (lambda path: path.chmod(0o644), "key_permissions"),
        (lambda path: path.unlink(), "key_not_found"),
    ],
)
def test_private_key_preflight_rejects_unsafe_files(tmp_path, mutate, expected_code):
    path = _private_key_file(tmp_path)
    mutate(path)

    with pytest.raises(SSHKeyPreflightError) as exc_info:
        preflight_private_key(path)

    assert exc_info.value.code == expected_code


def test_private_key_preflight_rejects_symlink(tmp_path):
    target = _private_key_file(tmp_path)
    link = tmp_path / "worker-key-link"
    link.symlink_to(target)

    with pytest.raises(SSHKeyPreflightError) as exc_info:
        preflight_private_key(link)

    assert exc_info.value.code == "key_symlink"


def test_private_key_preflight_rejects_encrypted_key(tmp_path):
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = tmp_path / "encrypted-key"
    path.write_bytes(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.BestAvailableEncryption(b"secret"),
    ))
    path.chmod(0o600)

    with pytest.raises(SSHKeyPreflightError) as exc_info:
        preflight_private_key(path)

    assert exc_info.value.code == "key_encrypted"


def test_public_key_validator_rejects_options_and_comments(tmp_path):
    public_key = derive_openssh_public_key(_private_key_file(tmp_path))

    with pytest.raises(ValueError):
        validate_openssh_public_key(f'command="id" {public_key}')
    with pytest.raises(ValueError):
        validate_openssh_public_key(f"{public_key} comment")


async def test_worker_bootstrap_and_setup_pin_same_exact_codex_cli_version():
    setup_script = (
        Path(__file__).resolve().parents[2] / "scripts" / "setup.sh"
    ).read_text(encoding="utf-8")
    provisioner = WorkerProvisioner(db_factory=None, cloud=object())
    provisioner._log = AsyncMock()
    ssh = AsyncMock()
    ssh.run.return_value = (0, "system init complete")

    await provisioner._step_system_init(ssh, worker_id=1)

    worker_script = ssh.run.await_args.args[0]
    version_pattern = re.compile(
        r'^CODEX_CLI_VERSION="([^"]+)"$',
        re.MULTILINE,
    )
    setup_version = version_pattern.search(setup_script)
    worker_version = version_pattern.search(worker_script)
    assert setup_version is not None
    assert worker_version is not None
    assert (
        setup_version.group(1)
        == worker_version.group(1)
        == WORKER_CODEX_CLI_VERSION
        == "0.147.0"
    )

    assert '@openai/codex@${CODEX_CLI_VERSION}' in setup_script
    assert (
        '"$(codex --version 2>/dev/null | head -1)" '
        '!= "codex-cli ${CODEX_CLI_VERSION}"'
    ) in setup_script
    assert '@openai/codex@$CODEX_CLI_VERSION' in worker_script
    assert (
        'test "$(codex --version 2>/dev/null | head -1)" '
        '= "codex-cli $CODEX_CLI_VERSION"'
    ) in worker_script
    assert "@openai/codex@latest" not in setup_script
    assert "@openai/codex@latest" not in worker_script


class _FakeChannel:
    def __init__(self, stdout: bytes, stderr: bytes):
        self.stdout = bytearray(stdout)
        self.stderr = bytearray(stderr)
        self.input = b""
        self.write_shutdown = False

    def sendall(self, data: bytes):
        self.input += data

    def shutdown_write(self):
        self.write_shutdown = True

    def recv_ready(self):
        return bool(self.stdout)

    def recv(self, size: int):
        chunk = bytes(self.stdout[:size])
        del self.stdout[:size]
        return chunk

    def recv_stderr_ready(self):
        return bool(self.stderr)

    def recv_stderr(self, size: int):
        chunk = bytes(self.stderr[:size])
        del self.stderr[:size]
        return chunk

    def exit_status_ready(self):
        return True

    def recv_exit_status(self):
        return 7

    def close(self):
        pass


class _FakeSSHClient:
    def __init__(self, channel: _FakeChannel):
        self.channel = channel
        self.connect_kwargs = None
        self.policy = None
        self.loaded_system_keys = False
        self.loaded_host_keys = None

    def load_system_host_keys(self):
        self.loaded_system_keys = True

    def load_host_keys(self, path):
        self.loaded_host_keys = path

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, host, **kwargs):
        self.connect_kwargs = {"host": host, **kwargs}

    def exec_command(self, command, timeout=None):
        return SimpleNamespace(channel=self.channel), SimpleNamespace(channel=self.channel), object()

    def close(self):
        pass


def test_ssh_uses_only_selected_key_and_drains_both_streams(tmp_path, monkeypatch):
    key_path = _private_key_file(tmp_path)
    known_hosts = tmp_path / "known-hosts" / "i-worker"
    channel = _FakeChannel(b"o" * 200_000, b"e" * 200_000)
    client = _FakeSSHClient(channel)
    monkeypatch.setattr(paramiko, "SSHClient", lambda: client)
    executor = SSHExecutor(
        "10.0.0.9",
        "ubuntu",
        str(key_path),
        known_hosts_path=str(known_hosts),
    )

    code, output = executor._execute_sync("worker-api", 5, b'{"hello":"world"}')

    assert code == 7
    assert output.count("o") == 200_000
    assert output.count("e") == 200_000
    assert channel.input == b'{"hello":"world"}'
    assert channel.write_shutdown is True
    assert client.loaded_system_keys is False
    assert client.loaded_host_keys == str(known_hosts)
    assert client.connect_kwargs["port"] == 22
    assert "key_filename" not in client.connect_kwargs
    assert client.connect_kwargs["pkey"].get_name() == "ssh-ed25519"
    assert client.connect_kwargs["allow_agent"] is False
    assert client.connect_kwargs["look_for_keys"] is False


def test_managed_ssh_uses_non_default_port_and_exact_pinned_host_key(
    tmp_path,
    monkeypatch,
):
    key_path = _private_key_file(tmp_path)
    pinned = derive_openssh_public_key(key_path)
    key_kind, key_body = pinned.split(" ", 1)
    channel = _FakeChannel(b"ok", b"")
    client = _FakeSSHClient(channel)
    monkeypatch.setattr(paramiko, "SSHClient", lambda: client)
    executor = SSHExecutor(
        "ssh.example.internal",
        "deploy",
        str(key_path),
        port=2222,
        host_key_policy="pinned",
        pinned_host_key=pinned,
    )

    result = executor._execute_result_sync("true", 5, None, 1024)

    assert result.exit_code == 7
    assert result.stdout == "ok"
    assert result.stderr == ""
    assert result.truncated is False
    assert client.connect_kwargs["port"] == 2222
    assert client.loaded_system_keys is False
    assert client.loaded_host_keys is None
    presented = SimpleNamespace(
        get_name=lambda: key_kind,
        get_base64=lambda: key_body,
    )
    client.policy.missing_host_key(client, "ssh.example.internal", presented)

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_key = derive_openssh_public_key(_private_key_file(other_dir))
    other_kind, other_body = other_key.split(" ", 1)
    with pytest.raises(SSHHostKeyMismatchError):
        client.policy.missing_host_key(
            client,
            "ssh.example.internal",
            SimpleNamespace(
                get_name=lambda: other_kind,
                get_base64=lambda: other_body,
            ),
        )


def test_managed_ssh_rejects_private_key_replaced_after_authorization(tmp_path):
    key_path = _private_key_file(tmp_path)
    authorized_fingerprint = openssh_public_key_fingerprint(
        derive_openssh_public_key(key_path)
    )
    replacement = _private_key_file(tmp_path, name="replacement-key")
    executor = SSHExecutor(
        "ssh.example.internal",
        "deploy",
        str(key_path),
        expected_public_key_fingerprint=authorized_fingerprint,
    )
    key_path.write_bytes(replacement.read_bytes())
    key_path.chmod(0o600)

    with pytest.raises(SSHKeyPreflightError) as exc_info:
        executor.connect()

    assert exc_info.value.code == "key_changed"


def test_pinned_ssh_refuses_to_downgrade_host_checking_for_rsync(tmp_path):
    key_path = _private_key_file(tmp_path)
    pinned = derive_openssh_public_key(key_path)
    executor = SSHExecutor(
        "ssh.example.internal",
        "deploy",
        str(key_path),
        host_key_policy="pinned",
        pinned_host_key=pinned,
    )

    with pytest.raises(ValueError, match="do not support the rsync transport"):
        executor._rsync_ssh_command()


def test_managed_ssh_result_bounds_combined_output(tmp_path, monkeypatch):
    key_path = _private_key_file(tmp_path)
    channel = _FakeChannel(b"stdout", b"stderr")
    client = _FakeSSHClient(channel)
    monkeypatch.setattr(paramiko, "SSHClient", lambda: client)
    executor = SSHExecutor("worker.internal", "ubuntu", str(key_path))

    result = executor._execute_result_sync("noisy", 5, None, 8)

    assert result.exit_code == 7
    assert result.stdout == "stdout"
    assert result.stderr == "st"
    assert result.truncated is True
    assert result.duration_ms >= 0


def test_openssh_public_key_fingerprint_is_stable(tmp_path):
    public_key = derive_openssh_public_key(_private_key_file(tmp_path))

    first = openssh_public_key_fingerprint(public_key)
    second = openssh_public_key_fingerprint(public_key)

    assert first == second
    assert first.startswith("SHA256:")


def test_probe_ssh_host_key_uses_requested_endpoint(tmp_path, monkeypatch):
    public_key = derive_openssh_public_key(_private_key_file(tmp_path))
    key_kind, key_body = public_key.split(" ", 1)
    key = SimpleNamespace(
        get_name=lambda: key_kind,
        get_base64=lambda: key_body,
    )
    sock = SimpleNamespace(close=lambda: None)
    seen = {}

    def fake_create_connection(endpoint, timeout):
        seen.update(endpoint=endpoint, socket_timeout=timeout)
        return sock

    class FakeTransport:
        def __init__(self, received_sock):
            assert received_sock is sock

        def start_client(self, timeout):
            seen["handshake_timeout"] = timeout

        def get_remote_server_key(self):
            return key

        def close(self):
            seen["closed"] = True

    monkeypatch.setattr(
        "backend.services.ssh_executor.socket.create_connection",
        fake_create_connection,
    )
    monkeypatch.setattr(paramiko, "Transport", FakeTransport)

    result = probe_ssh_host_key(
        "ssh.example.internal", port=2222, timeout=4,
    )

    assert seen == {
        "endpoint": ("ssh.example.internal", 2222),
        "socket_timeout": 4.0,
        "handshake_timeout": 4.0,
        "closed": True,
    }
    assert result.key_type == key_kind
    assert result.openssh_public_key == public_key
    assert result.sha256_fingerprint.startswith("SHA256:")


async def test_run_with_input_keeps_payload_out_of_logs(tmp_path, monkeypatch, caplog):
    executor = SSHExecutor("worker.internal", "ubuntu", str(_private_key_file(tmp_path)))
    seen = {}

    def fake_run(command, payload, timeout):
        seen.update(command=command, payload=payload, timeout=timeout)
        return 0, "ok"

    monkeypatch.setattr(executor, "_run_with_input_sync", fake_run)
    with caplog.at_level(logging.DEBUG, logger="backend.services.ssh_executor"):
        result = await executor.run_with_input(
            "codex-api --stdin", "super-secret-payload", timeout=17,
        )

    assert result == (0, "ok")
    assert seen == {
        "command": "codex-api --stdin",
        "payload": b"super-secret-payload",
        "timeout": 17,
    }
    assert "super-secret-payload" not in caplog.text
    assert "codex-api" not in caplog.text


async def test_probe_returns_structured_authentication_failure(tmp_path, monkeypatch):
    executor = SSHExecutor("10.0.0.9", "ubuntu", str(_private_key_file(tmp_path)))
    monkeypatch.setattr(
        executor,
        "run",
        AsyncMock(side_effect=paramiko.AuthenticationException("sensitive server text")),
    )

    result = await executor.probe()

    assert result.ok is False
    assert result.error_code == "authentication_failed"
    assert "sensitive server text" not in (result.detail or "")
    assert executor.last_probe_result == result
    assert await executor.check_alive() is False


def test_rsync_transport_forces_single_identity(tmp_path):
    known_hosts = tmp_path / "known-hosts" / "i-worker"
    executor = SSHExecutor(
        "10.0.0.9",
        "ubuntu",
        str(_private_key_file(tmp_path)),
        known_hosts_path=str(known_hosts),
    )

    command = executor._rsync_ssh_command()

    assert "IdentitiesOnly=yes" in command
    assert "BatchMode=yes" in command
    assert f"UserKnownHostsFile={known_hosts}" in command
    assert known_hosts.stat().st_mode & 0o777 == 0o600


async def test_copy_file_uses_secluded_remote_arguments(
    tmp_path,
    monkeypatch,
):
    executor = SSHExecutor(
        "10.0.0.9",
        "ubuntu",
        str(_private_key_file(tmp_path)),
    )
    executor.run = AsyncMock(return_value=(0, ""))
    monkeypatch.setattr(
        executor,
        "_rsync_ssh_command",
        lambda: "ssh-safe",
    )
    captured = {}

    def fake_run(command, **_kwargs):
        captured["command"] = command
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "backend.services.ssh_executor.subprocess.run",
        fake_run,
    )
    local = tmp_path / "attachment.txt"
    local.write_text("attachment", encoding="utf-8")

    await executor.copy_file(
        str(local),
        "/srv/ccm/uploads/path with spaces\nand-newline",
    )

    assert "--secluded-args" in captured["command"]
    assert captured["command"][-1].endswith(
        ":/srv/ccm/uploads/path with spaces\nand-newline"
    )


def test_worker_known_hosts_is_scoped_to_cloud_instance(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))

    first = worker_known_hosts_path("i-worker-one")
    replacement = worker_known_hosts_path("i-worker-two")

    assert first != replacement
    assert first.endswith("/ccm-worker-known-hosts/i-worker-one")
    with pytest.raises(ValueError):
        worker_known_hosts_path("../escape")


class _FakeAwsError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _FakeEC2:
    def __init__(self):
        self.groups: dict[str, dict] = {}
        self.run_params = None
        self.authorize_calls: list[dict] = []
        self.create_group_calls: list[dict] = []
        self.terminate_calls: list[list[str]] = []

    def describe_subnets(self, *, SubnetIds):
        assert SubnetIds == ["subnet-manager"]
        return {"Subnets": [{"SubnetId": "subnet-manager", "VpcId": "vpc-manager"}]}

    def describe_security_groups(self, *, GroupIds=None, Filters=None):
        if GroupIds is not None:
            return {"SecurityGroups": [self.groups[group_id] for group_id in GroupIds]}
        names = next(item["Values"] for item in Filters if item["Name"] == "group-name")
        vpcs = next(item["Values"] for item in Filters if item["Name"] == "vpc-id")
        return {"SecurityGroups": [
            group for group in self.groups.values()
            if group.get("GroupName") in names and group.get("VpcId") in vpcs
        ]}

    def describe_instances(self, *, Filters):
        assert Filters[0]["Name"] == "client-token"
        return {"Reservations": []}

    def create_security_group(self, **kwargs):
        self.create_group_calls.append(kwargs)
        if any(group.get("GroupName") == kwargs["GroupName"] for group in self.groups.values()):
            raise _FakeAwsError("InvalidGroup.Duplicate")
        group_id = "sg-worker"
        self.groups[group_id] = {
            "GroupId": group_id,
            "GroupName": kwargs["GroupName"],
            "Description": kwargs["Description"],
            "VpcId": kwargs["VpcId"],
            "IpPermissions": [],
        }
        return {"GroupId": group_id}

    def authorize_security_group_ingress(self, **kwargs):
        self.authorize_calls.append(kwargs)
        group = self.groups[kwargs["GroupId"]]
        permission = kwargs["IpPermissions"][0]
        source = permission["UserIdGroupPairs"][0]["GroupId"]
        duplicate = any(
            existing.get("FromPort") == permission["FromPort"]
            and any(pair.get("GroupId") == source for pair in existing.get("UserIdGroupPairs", []))
            for existing in group["IpPermissions"]
        )
        if duplicate:
            raise _FakeAwsError("InvalidPermission.Duplicate")
        group["IpPermissions"].append(permission)

    def run_instances(self, **params):
        self.run_params = params
        return {"Instances": [{"InstanceId": "i-worker"}]}

    def terminate_instances(self, *, InstanceIds):
        self.terminate_calls.append(InstanceIds)
        return {"TerminatingInstances": [{"InstanceId": InstanceIds[0]}]}


def _provider(fake_ec2: _FakeEC2, *, manager_groups=None) -> AWSProvider:
    provider = AWSProvider(region="test-region")
    provider._client = fake_ec2
    provider._self_info = {
        "instance_id": "i-manager",
        "state": "running",
        "private_ip": "10.0.0.1",
        "public_ip": None,
        "instance_type": "t3.medium",
        "image_id": "ami-manager",
        "vpc_id": "vpc-manager",
        "subnet_id": "subnet-manager",
        "key_name": "manager-key",
        "security_group_ids": manager_groups or ["sg-manager"],
        "name": "manager",
    }
    return provider


async def test_aws_create_injects_public_key_and_exact_manager_sg_rules(tmp_path):
    private_path = _private_key_file(tmp_path)
    public_key = derive_openssh_public_key(private_path)
    ec2 = _FakeEC2()
    provider = _provider(ec2, manager_groups=["sg-manager-a", "sg-manager-b"])

    instance_id = await provider.create_instance("worker-one", {
        "ssh_public_key": public_key,
        "ssh_user": "ubuntu",
        "ccm_port": 8123,
        "client_token": "ccm-stable-create-token",
    })

    assert instance_id == "i-worker"
    assert ec2.run_params["SecurityGroupIds"] == ["sg-worker"]
    assert "KeyName" not in ec2.run_params
    assert ec2.run_params["SubnetId"] == "subnet-manager"
    assert ec2.run_params["ClientToken"] == "ccm-stable-create-token"
    assert ec2.run_params["UserData"].startswith("#!/bin/bash")
    assert "PRIVATE KEY" not in ec2.run_params["UserData"]
    assert build_worker_cloud_init(public_key, "ubuntu") == ec2.run_params["UserData"]
    exact_rules = {
        (
            call["IpPermissions"][0]["FromPort"],
            call["IpPermissions"][0]["UserIdGroupPairs"][0]["GroupId"],
        )
        for call in ec2.authorize_calls
    }
    assert exact_rules == {
        (22, "sg-manager-a"),
        (22, "sg-manager-b"),
        (8123, "sg-manager-a"),
        (8123, "sg-manager-b"),
    }
    assert all(
        not call["IpPermissions"][0].get("IpRanges")
        for call in ec2.authorize_calls
    )


async def test_aws_adopts_instance_by_client_token_before_run_instances(tmp_path):
    class LostResponseEC2(_FakeEC2):
        def describe_instances(self, *, Filters):
            assert Filters == [{
                "Name": "client-token",
                "Values": ["ccm-lost-response"],
            }]
            return {"Reservations": [{"Instances": [{
                "InstanceId": "i-already-created",
                "State": {"Name": "running"},
            }]}]}

        def run_instances(self, **params):
            raise AssertionError("RunInstances must not be repeated after adoption")

    ec2 = LostResponseEC2()
    provider = _provider(ec2)

    instance_id = await provider.create_instance("renamed-worker", {
        "ssh_public_key": derive_openssh_public_key(_private_key_file(tmp_path)),
        "client_token": "ccm-lost-response",
    })

    assert instance_id == "i-already-created"
    assert ec2.create_group_calls == []


async def test_aws_termination_scope_uses_same_session_account_and_region():
    class FakeSTS:
        def get_caller_identity(self):
            return {
                "Account": "123456789012",
                "Arn": "arn:aws-us-gov:iam::123456789012:role/ccm-manager",
            }

    class FakeSession:
        def client(self, service, *, region_name):
            assert service == "sts"
            assert region_name == "us-gov-west-1"
            return FakeSTS()

    provider = AWSProvider(region="us-gov-west-1")
    provider._client = _FakeEC2()
    provider._boto_session = FakeSession()

    assert await provider.termination_scope() == {
        "provider": "aws",
        "partition": "aws-us-gov",
        "account_id": "123456789012",
        "region": "us-gov-west-1",
    }


async def test_aws_client_token_lookup_can_prove_exact_terminated_identity():
    class TerminatedEC2(_FakeEC2):
        def describe_instances(self, *, Filters):
            assert Filters == [{
                "Name": "client-token",
                "Values": ["ccm-terminated-worker"],
            }]
            return {"Reservations": [{"Instances": [{
                "InstanceId": "i-terminated-worker",
                "State": {"Name": "terminated"},
            }]}]}

    provider = _provider(TerminatedEC2())

    with pytest.raises(RuntimeError, match="already terminating/terminated"):
        await provider.find_instance_by_create_token(
            "ccm-terminated-worker"
        )
    assert await provider.find_instance_by_create_token(
        "ccm-terminated-worker",
        include_terminated=True,
    ) == "i-terminated-worker"


async def test_aws_ingress_duplicate_is_individually_idempotent(tmp_path):
    public_key = derive_openssh_public_key(_private_key_file(tmp_path))
    ec2 = _FakeEC2()
    description = "CCM Worker access from manager i-manager"
    ec2.groups["sg-worker"] = {
        "GroupId": "sg-worker",
        "GroupName": "ccm-worker-i-manager",
        "Description": description,
        "VpcId": "vpc-manager",
        "IpPermissions": [{
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "UserIdGroupPairs": [{"GroupId": "sg-manager"}],
        }],
    }
    provider = _provider(ec2)

    await provider.create_instance("worker-two", {"ssh_public_key": public_key})

    assert ec2.run_params is not None
    assert len(ec2.create_group_calls) == 0
    assert {
        call["IpPermissions"][0]["FromPort"] for call in ec2.authorize_calls
    } == {22, 8000}


async def test_aws_uses_key_name_only_when_explicitly_configured(tmp_path):
    ec2 = _FakeEC2()
    provider = _provider(ec2)

    await provider.create_instance("explicit-key-worker", {
        "ssh_public_key": derive_openssh_public_key(_private_key_file(tmp_path)),
        "key_name": "explicit-worker-key",
    })

    assert ec2.run_params["KeyName"] == "explicit-worker-key"


async def test_new_security_group_eventual_consistency_is_retried(
    tmp_path, monkeypatch,
):
    class EventuallyConsistentEC2(_FakeEC2):
        def __init__(self):
            super().__init__()
            self.describe_not_found = 1
            self.authorize_not_found = 1

        def describe_security_groups(self, **kwargs):
            if kwargs.get("GroupIds") and self.describe_not_found:
                self.describe_not_found -= 1
                raise _FakeAwsError("InvalidGroup.NotFound")
            return super().describe_security_groups(**kwargs)

        def authorize_security_group_ingress(self, **kwargs):
            if self.authorize_not_found:
                self.authorize_not_found -= 1
                raise _FakeAwsError("InvalidGroup.NotFound")
            return super().authorize_security_group_ingress(**kwargs)

    monkeypatch.setattr(cloud_provider_module.time, "sleep", lambda _seconds: None)
    ec2 = EventuallyConsistentEC2()
    provider = _provider(ec2)

    await provider.create_instance("eventual-worker", {
        "ssh_public_key": derive_openssh_public_key(_private_key_file(tmp_path)),
    })

    assert ec2.run_params is not None
    assert ec2.describe_not_found == 0
    assert ec2.authorize_not_found == 0


async def test_aws_rejects_public_or_foreign_ingress_before_instance_create(tmp_path):
    ec2 = _FakeEC2()
    ec2.groups["sg-custom"] = {
        "GroupId": "sg-custom",
        "GroupName": "unsafe",
        "Description": "unsafe",
        "VpcId": "vpc-manager",
        "IpPermissions": [{
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            "UserIdGroupPairs": [],
        }],
    }
    provider = _provider(ec2)

    with pytest.raises(RuntimeError, match="non-CCM ingress"):
        await provider.create_instance("unsafe-worker", {
            "ssh_public_key": derive_openssh_public_key(_private_key_file(tmp_path)),
            "security_group_ids": ["sg-custom"],
        })

    assert ec2.run_params is None
    assert ec2.authorize_calls == []


async def test_aws_rejects_invalid_public_key_before_cloud_mutation():
    ec2 = _FakeEC2()
    provider = _provider(ec2)

    with pytest.raises(ValueError, match="public key"):
        await provider.create_instance("bad-key", {
            "ssh_public_key": "ssh-ed25519 not-base64 private-data",
        })

    assert ec2.create_group_calls == []
    assert ec2.run_params is None


async def test_aws_requires_public_key_before_cloud_mutation():
    ec2 = _FakeEC2()
    provider = _provider(ec2)

    with pytest.raises(ValueError, match="ssh_public_key is required"):
        await provider.create_instance("missing-key", {})

    assert ec2.create_group_calls == []
    assert ec2.run_params is None


async def test_aws_rejects_unsafe_client_token_before_instance_create(tmp_path):
    ec2 = _FakeEC2()
    provider = _provider(ec2)

    with pytest.raises(ValueError, match="client_token"):
        await provider.create_instance("bad-token", {
            "ssh_public_key": derive_openssh_public_key(_private_key_file(tmp_path)),
            "client_token": "contains whitespace",
        })

    assert ec2.run_params is None


async def test_aws_terminate_treats_already_absent_instance_as_success_only_with_scope_proof():
    class MissingInstanceEC2(_FakeEC2):
        def terminate_instances(self, *, InstanceIds):
            raise _FakeAwsError("InvalidInstanceID.NotFound")

    provider = _provider(MissingInstanceEC2())

    with pytest.raises(_FakeAwsError, match="InvalidInstanceID.NotFound"):
        await provider.terminate_instance("i-already-gone")

    await provider.terminate_instance(
        "i-already-gone",
        allow_not_found=True,
    )

    with pytest.raises(ValueError, match="exact boolean"):
        await provider.terminate_instance(
            "i-already-gone",
            allow_not_found="yes",
        )


async def test_aws_terminate_submits_exact_instance_id():
    ec2 = _FakeEC2()
    provider = _provider(ec2)

    await provider.terminate_instance("i-worker")

    assert ec2.terminate_calls == [["i-worker"]]


async def test_aws_terminate_propagates_non_absence_errors():
    class DeniedEC2(_FakeEC2):
        def terminate_instances(self, *, InstanceIds):
            raise _FakeAwsError("UnauthorizedOperation")

    provider = _provider(DeniedEC2())

    with pytest.raises(_FakeAwsError, match="UnauthorizedOperation"):
        await provider.terminate_instance("i-still-running")
