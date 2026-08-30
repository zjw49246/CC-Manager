"""云实例管理抽象层（elastic-worker 设计 §16.2）。

AWSProvider 通过 IMDSv2 + boto3 从 Manager 自身实例元数据自举配置
（机型/AMI/子网/密钥对继承本机，"配置和本机一样"），凭证优先走 IAM
instance profile。所有 boto3 调用是同步的，统一用 asyncio.to_thread 包装。
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import shlex
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx

from backend.services.ssh_executor import validate_openssh_public_key

logger = logging.getLogger(__name__)

IMDS_BASE = "http://169.254.169.254/latest"
DEFAULT_WORKER_CCM_PORT = 8000
_SSH_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_MANAGED_SG_NAME_PREFIX = "ccm-worker-"
_CLOUD_SCOPE_KEYS = frozenset({
    "provider",
    "partition",
    "account_id",
    "region",
})


def canonical_cloud_termination_scope(value: object) -> dict[str, str]:
    """Validate the provider scope frozen beside an irreversible effect."""

    if not isinstance(value, dict) or set(value) != _CLOUD_SCOPE_KEYS:
        raise ValueError("cloud termination scope has an invalid shape")
    provider = value.get("provider")
    partition = value.get("partition")
    account_id = value.get("account_id")
    region = value.get("region")
    if (
        provider != "aws"
        or not isinstance(partition, str)
        or re.fullmatch(r"[a-z0-9-]{3,32}", partition) is None
        or not isinstance(account_id, str)
        or re.fullmatch(r"[0-9]{12}", account_id) is None
        or not isinstance(region, str)
        or re.fullmatch(r"[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+", region) is None
    ):
        raise ValueError("cloud termination scope identity is invalid")
    return {
        "provider": provider,
        "partition": partition,
        "account_id": account_id,
        "region": region,
    }


def _validate_ssh_user(ssh_user: str) -> str:
    if not isinstance(ssh_user, str) or _SSH_USER_RE.fullmatch(ssh_user) is None:
        raise ValueError("ssh_user must be a safe Linux account name")
    return ssh_user


def _validate_port(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be an integer TCP port")
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer TCP port") from exc
    if port < 1 or port > 65535:
        raise ValueError(f"{label} must be between 1 and 65535")
    return port


def build_worker_cloud_init(ssh_public_key: str, ssh_user: str) -> str:
    """Build idempotent cloud-init user data containing only a public key.

    A shell payload is used instead of YAML's ``users`` merge semantics so an
    existing AMI account is updated without changing its sudo/groups setup.
    Both dynamic values are strictly validated before interpolation.
    """

    public_key = validate_openssh_public_key(ssh_public_key)
    user = _validate_ssh_user(ssh_user)
    encoded_key = base64.b64encode(public_key.encode("ascii")).decode("ascii")
    quoted_user = shlex.quote(user)
    quoted_key = shlex.quote(encoded_key)
    return f"""#!/bin/bash
set -eu
umask 077
ccm_ssh_user={quoted_user}
ccm_key_b64={quoted_key}
if ! id "$ccm_ssh_user" >/dev/null 2>&1; then
  echo "CCM worker SSH user does not exist" >&2
  exit 1
fi
ccm_home="$(getent passwd "$ccm_ssh_user" | cut -d: -f6)"
ccm_group="$(id -gn "$ccm_ssh_user")"
test -n "$ccm_home"
install -d -m 0700 -o "$ccm_ssh_user" -g "$ccm_group" "$ccm_home/.ssh"
ccm_auth="$ccm_home/.ssh/authorized_keys"
touch "$ccm_auth"
chown "$ccm_ssh_user:$ccm_group" "$ccm_auth"
chmod 0600 "$ccm_auth"
ccm_public_key="$(printf %s "$ccm_key_b64" | base64 -d)"
if ! grep -qxF -- "$ccm_public_key" "$ccm_auth"; then
  printf '%s\n' "$ccm_public_key" >> "$ccm_auth"
fi
"""


def _aws_error_code(exc: BaseException) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    return error.get("Code") if isinstance(error, dict) else None


class CloudProvider(ABC):
    @abstractmethod
    async def self_describe(self) -> dict:
        """返回 Manager 自身实例信息（用于配置自举）。"""

    @abstractmethod
    async def create_instance(self, name: str, overrides: dict | None = None) -> str:
        """创建实例，返回 instance_id。"""

    @abstractmethod
    async def describe_instance(self, instance_id: str) -> dict:
        """返回 {state, private_ip, public_ip, instance_type, name}。"""

    @abstractmethod
    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> str:
        """等实例 Running，返回 private_ip。"""

    @abstractmethod
    async def stop_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    async def start_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    async def terminate_instance(
        self,
        instance_id: str,
        *,
        allow_not_found: bool = False,
    ) -> None:
        """Terminate one exact instance.

        ``allow_not_found`` is intentionally opt-in.  A provider-side absence
        is safe to treat as success only after the caller has matched a
        durable account/partition/region receipt for the irreversible effect.
        """

    @abstractmethod
    async def termination_scope(self) -> dict[str, str]:
        """Return the immutable provider/account/region effect boundary."""

    @abstractmethod
    async def find_instance_by_create_token(
        self,
        client_token: str,
        *,
        include_terminated: bool = False,
    ) -> str | None:
        """Resolve the unique live instance created by an idempotency token."""


class AWSProvider(CloudProvider):
    def __init__(self, region: str | None = None):
        self._region = region
        self._client: Any = None
        self._boto_session: Any = None
        self._self_info: dict | None = None

    # -- 内部工具 ---------------------------------------------------------

    async def _imds(self, path: str) -> str:
        async with httpx.AsyncClient(timeout=5) as c:
            token = (await c.put(
                f"{IMDS_BASE}/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            )).text
            r = await c.get(
                f"{IMDS_BASE}/meta-data/{path}",
                headers={"X-aws-ec2-metadata-token": token},
            )
            r.raise_for_status()
            return r.text

    async def _ec2(self) -> Any:
        if self._client is None:
            if self._region is None:
                self._region = await self._imds("placement/region")
            import boto3  # 延迟导入：未装 boto3 时其余功能不受影响

            self._boto_session = await asyncio.to_thread(
                boto3.Session,
                region_name=self._region,
            )
            self._client = await asyncio.to_thread(
                self._boto_session.client,
                "ec2",
                region_name=self._region,
            )
        return self._client

    async def termination_scope(self) -> dict[str, str]:
        """Bind EC2 effects to the same boto session, account, and region."""

        await self._ec2()
        if self._boto_session is None:
            # Tests may inject an EC2 client directly. Production always
            # reaches the branch in ``_ec2`` above.
            import boto3

            self._boto_session = await asyncio.to_thread(
                boto3.Session,
                region_name=self._region,
            )
        sts = await asyncio.to_thread(
            self._boto_session.client,
            "sts",
            region_name=self._region,
        )
        identity = await asyncio.to_thread(sts.get_caller_identity)
        account_id = identity.get("Account") if isinstance(identity, dict) else None
        arn = identity.get("Arn") if isinstance(identity, dict) else None
        partition = (
            arn.split(":", 2)[1]
            if isinstance(arn, str) and arn.startswith("arn:") and ":" in arn[4:]
            else None
        )
        if (
            not isinstance(account_id, str)
            or len(account_id) != 12
            or not account_id.isdigit()
            or not isinstance(partition, str)
            or not partition
            or not isinstance(self._region, str)
            or not self._region
        ):
            raise RuntimeError("AWS termination scope identity is malformed")
        return canonical_cloud_termination_scope({
            "provider": "aws",
            "partition": partition,
            "account_id": account_id,
            "region": self._region,
        })

    async def find_instance_by_create_token(
        self,
        client_token: str,
        *,
        include_terminated: bool = False,
    ) -> str | None:
        ec2 = await self._ec2()
        if (
            not isinstance(client_token, str)
            or re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", client_token) is None
        ):
            raise ValueError("client_token must be 1-64 safe ASCII characters")
        return await asyncio.to_thread(
            self._find_instance_by_client_token_sync,
            ec2,
            client_token,
            include_terminated,
        )

    @staticmethod
    def _parse(inst: dict) -> dict:
        name = next(
            (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), None
        )
        return {
            "instance_id": inst["InstanceId"],
            "state": inst["State"]["Name"],
            "private_ip": inst.get("PrivateIpAddress"),
            "public_ip": inst.get("PublicIpAddress"),
            "instance_type": inst.get("InstanceType"),
            "image_id": inst.get("ImageId"),
            "vpc_id": inst.get("VpcId"),
            "subnet_id": inst.get("SubnetId"),
            "key_name": inst.get("KeyName"),
            "security_group_ids": [g["GroupId"] for g in inst.get("SecurityGroups", [])],
            "name": name,
        }

    @staticmethod
    def _validate_worker_group_ingress(
        group: dict,
        *,
        manager_group_ids: set[str],
        allowed_ports: set[int],
    ) -> None:
        """Fail closed if a purported Worker SG exposes any other ingress."""

        for permission in group.get("IpPermissions", []):
            if (
                permission.get("IpProtocol") != "tcp"
                or permission.get("FromPort") != permission.get("ToPort")
                or permission.get("FromPort") not in allowed_ports
                or permission.get("IpRanges")
                or permission.get("Ipv6Ranges")
                or permission.get("PrefixListIds")
            ):
                raise RuntimeError(
                    f"Worker security group {group.get('GroupId')} has non-CCM ingress"
                )
            sources = permission.get("UserIdGroupPairs") or []
            if not sources or any(
                pair.get("GroupId") not in manager_group_ids for pair in sources
            ):
                raise RuntimeError(
                    f"Worker security group {group.get('GroupId')} has an untrusted ingress source"
                )

    @staticmethod
    def _assert_required_worker_group_ingress(
        group: dict,
        *,
        manager_group_ids: set[str],
        allowed_ports: set[int],
    ) -> None:
        actual = {
            (permission.get("FromPort"), pair.get("GroupId"))
            for permission in group.get("IpPermissions", [])
            for pair in permission.get("UserIdGroupPairs", [])
        }
        required = {
            (port, manager_group_id)
            for port in allowed_ports
            for manager_group_id in manager_group_ids
        }
        if not required.issubset(actual):
            raise RuntimeError(
                f"Worker security group {group.get('GroupId')} is missing required CCM ingress"
            )

    @staticmethod
    def _describe_groups_by_id(ec2: Any, group_ids: list[str]) -> list[dict]:
        last_error: BaseException | None = None
        for attempt in range(6):
            try:
                response = ec2.describe_security_groups(GroupIds=group_ids)
                groups = response.get("SecurityGroups") or []
                if {group.get("GroupId") for group in groups} == set(group_ids):
                    return groups
                last_error = RuntimeError(
                    "AWS did not return every configured Worker security group"
                )
            except Exception as exc:
                if _aws_error_code(exc) != "InvalidGroup.NotFound":
                    raise
                last_error = exc
            if attempt < 5:
                time.sleep(min(0.2 * (2 ** attempt), 1.0))
        raise RuntimeError(
            "Worker security group did not become visible after creation"
        ) from last_error

    @staticmethod
    def _find_managed_worker_group(
        ec2: Any,
        *,
        group_name: str,
        vpc_id: str,
    ) -> dict | None:
        response = ec2.describe_security_groups(Filters=[
            {"Name": "group-name", "Values": [group_name]},
            {"Name": "vpc-id", "Values": [vpc_id]},
        ])
        groups = response.get("SecurityGroups") or []
        if len(groups) > 1:
            raise RuntimeError(f"Multiple Worker security groups named {group_name}")
        return groups[0] if groups else None

    @classmethod
    def _create_or_find_managed_worker_group(
        cls,
        ec2: Any,
        *,
        manager_instance_id: str,
        vpc_id: str,
    ) -> dict:
        group_name = f"{_MANAGED_SG_NAME_PREFIX}{manager_instance_id}"
        description = f"CCM Worker access from manager {manager_instance_id}"
        group = cls._find_managed_worker_group(
            ec2, group_name=group_name, vpc_id=vpc_id,
        )
        if group is None:
            try:
                response = ec2.create_security_group(
                    GroupName=group_name,
                    Description=description,
                    VpcId=vpc_id,
                    TagSpecifications=[{
                        "ResourceType": "security-group",
                        "Tags": [
                            {"Key": "Name", "Value": group_name},
                            {"Key": "ManagedBy", "Value": "CCM"},
                            {"Key": "CCMManagerInstance", "Value": manager_instance_id},
                        ],
                    }],
                )
                group_id = response.get("GroupId")
                if not group_id:
                    raise RuntimeError("AWS did not return the created Worker security group id")
                groups = cls._describe_groups_by_id(ec2, [group_id])
                group = groups[0]
            except Exception as exc:
                if _aws_error_code(exc) != "InvalidGroup.Duplicate":
                    raise
                # Concurrent Worker creates may race on the stable group name.
                for _ in range(5):
                    group = cls._find_managed_worker_group(
                        ec2, group_name=group_name, vpc_id=vpc_id,
                    )
                    if group is not None:
                        break
                    time.sleep(0.2)
                if group is None:
                    raise RuntimeError(
                        "Worker security group was created concurrently but is not visible"
                    ) from exc

        if group.get("Description") != description:
            raise RuntimeError(
                f"Refusing to reuse unmanaged security group {group.get('GroupId')}"
            )
        return group

    @staticmethod
    def _authorize_manager_ingress(
        ec2: Any,
        *,
        worker_group_id: str,
        manager_group_ids: set[str],
        ports: set[int],
    ) -> None:
        """Authorize exact source-SG/port pairs; each call is independently idempotent."""

        for port in sorted(ports):
            for manager_group_id in sorted(manager_group_ids):
                for attempt in range(6):
                    try:
                        ec2.authorize_security_group_ingress(
                            GroupId=worker_group_id,
                            IpPermissions=[{
                                "IpProtocol": "tcp",
                                "FromPort": port,
                                "ToPort": port,
                                "UserIdGroupPairs": [{"GroupId": manager_group_id}],
                            }],
                        )
                        break
                    except Exception as exc:
                        error_code = _aws_error_code(exc)
                        if error_code == "InvalidPermission.Duplicate":
                            break
                        if error_code != "InvalidGroup.NotFound" or attempt >= 5:
                            raise
                        time.sleep(min(0.2 * (2 ** attempt), 1.0))

    @classmethod
    def _ensure_worker_security_groups_sync(
        cls,
        ec2: Any,
        *,
        manager: dict,
        subnet_id: str,
        requested_group_ids: list[str] | None,
        ccm_port: int,
    ) -> list[str]:
        manager_group_ids = {
            group_id for group_id in manager.get("security_group_ids", [])
            if isinstance(group_id, str) and group_id
        }
        if not manager_group_ids:
            raise RuntimeError("Manager instance has no security group to use as Worker ingress source")

        subnet_response = ec2.describe_subnets(SubnetIds=[subnet_id])
        subnets = subnet_response.get("Subnets") or []
        if len(subnets) != 1 or not subnets[0].get("VpcId"):
            raise RuntimeError(f"Could not resolve Worker subnet {subnet_id}")
        target_vpc_id = subnets[0]["VpcId"]
        manager_vpc_id = manager.get("vpc_id")
        if not manager_vpc_id or target_vpc_id != manager_vpc_id:
            raise RuntimeError("Worker subnet must be in the Manager instance VPC")

        allowed_ports = {22, ccm_port}
        if requested_group_ids:
            groups = cls._describe_groups_by_id(ec2, requested_group_ids)
            if any(group.get("VpcId") != target_vpc_id for group in groups):
                raise RuntimeError("Configured Worker security group is in another VPC")
        else:
            group = cls._create_or_find_managed_worker_group(
                ec2,
                manager_instance_id=manager["instance_id"],
                vpc_id=target_vpc_id,
            )
            groups = [group]

        # Validate before mutation so a pre-existing public/foreign rule makes
        # creation fail closed instead of being hidden by the required rules.
        for group in groups:
            cls._validate_worker_group_ingress(
                group,
                manager_group_ids=manager_group_ids,
                allowed_ports=allowed_ports,
            )
            cls._authorize_manager_ingress(
                ec2,
                worker_group_id=group["GroupId"],
                manager_group_ids=manager_group_ids,
                ports=allowed_ports,
            )

        group_ids = [group["GroupId"] for group in groups]
        refreshed = cls._describe_groups_by_id(ec2, group_ids)
        for group in refreshed:
            cls._validate_worker_group_ingress(
                group,
                manager_group_ids=manager_group_ids,
                allowed_ports=allowed_ports,
            )
            cls._assert_required_worker_group_ingress(
                group,
                manager_group_ids=manager_group_ids,
                allowed_ports=allowed_ports,
            )
        return group_ids

    @staticmethod
    def _find_instance_by_client_token_sync(
        ec2: Any,
        client_token: str,
        include_terminated: bool = False,
    ) -> str | None:
        """Adopt a RunInstances result whose response was lost.

        ClientToken makes another RunInstances call safe only when every
        parameter is identical.  Discovery first also recovers the instance
        if an operator changed a mutable tag/config value before retry.
        """
        response = ec2.describe_instances(Filters=[{
            "Name": "client-token",
            "Values": [client_token],
        }])
        instances = [
            instance
            for reservation in response.get("Reservations", [])
            for instance in reservation.get("Instances", [])
            if isinstance(instance, dict)
        ]
        live = [
            instance for instance in instances
            if (instance.get("State") or {}).get("Name")
            not in {"terminated", "shutting-down"}
        ]
        candidates = instances if include_terminated else live
        if len(candidates) > 1:
            raise RuntimeError(
                "Multiple EC2 instances share the Worker ClientToken; refusing an ambiguous adoption"
            )
        if len(candidates) == 1:
            instance_id = candidates[0].get("InstanceId")
            if not isinstance(instance_id, str) or not instance_id:
                raise RuntimeError("EC2 ClientToken lookup returned an instance without id")
            return instance_id
        if instances:
            raise RuntimeError(
                "The Worker ClientToken belongs to an instance that is already terminating/terminated"
            )
        return None

    # -- 接口实现 ---------------------------------------------------------

    async def self_describe(self) -> dict:
        if self._self_info is None:
            my_id = await self._imds("instance-id")
            self._self_info = await self.describe_instance(my_id)
        return self._self_info

    async def describe_instance(self, instance_id: str) -> dict:
        ec2 = await self._ec2()
        # AWS 最终一致性：run_instances 返回 ID 后 describe 可能短暂 NotFound
        for attempt in range(8):
            try:
                resp = await asyncio.to_thread(
                    ec2.describe_instances, InstanceIds=[instance_id]
                )
                if resp["Reservations"]:
                    return self._parse(resp["Reservations"][0]["Instances"][0])
            except Exception as e:
                if "InvalidInstanceID" not in str(e) or attempt >= 7:
                    raise
            await asyncio.sleep(3)
        raise RuntimeError(f"instance {instance_id} not found after 8 attempts")

    async def create_instance(self, name: str, overrides: dict | None = None) -> str:
        me = await self.self_describe()
        overrides = overrides or {}
        subnet_id = overrides.get("subnet_id") or me["subnet_id"]
        ccm_port = _validate_port(
            overrides.get("ccm_port", DEFAULT_WORKER_CCM_PORT), label="ccm_port",
        )
        raw_public_key = overrides.get("ssh_public_key")
        if raw_public_key is None:
            raise ValueError(
                "ssh_public_key is required so Worker SSH does not depend on an unrelated EC2 KeyName"
            )
        ssh_public_key = validate_openssh_public_key(raw_public_key)
        ssh_user = _validate_ssh_user(overrides.get("ssh_user") or "ubuntu")
        requested_group_ids = overrides.get("security_group_ids")
        if requested_group_ids is not None and (
            not isinstance(requested_group_ids, list)
            or not requested_group_ids
            or any(not isinstance(value, str) or not value for value in requested_group_ids)
            or len(set(requested_group_ids)) != len(requested_group_ids)
        ):
            raise ValueError("security_group_ids must be a non-empty list of unique ids")
        client_token = overrides.get("client_token")
        if client_token is not None and (
            not isinstance(client_token, str)
            or not re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", client_token)
        ):
            raise ValueError("client_token must be 1-64 safe ASCII characters")

        ec2 = await self._ec2()
        if client_token is not None:
            adopted_instance_id = await asyncio.to_thread(
                self._find_instance_by_client_token_sync,
                ec2,
                client_token,
                False,
            )
            if adopted_instance_id is not None:
                logger.warning(
                    "adopted Worker instance %s from EC2 ClientToken after a lost create response",
                    adopted_instance_id,
                )
                return adopted_instance_id
        security_group_ids = await asyncio.to_thread(
            self._ensure_worker_security_groups_sync,
            ec2,
            manager=me,
            subnet_id=subnet_id,
            requested_group_ids=requested_group_ids,
            ccm_port=ccm_port,
        )
        params = {
            "ImageId": overrides.get("image_id") or me["image_id"],
            "InstanceType": overrides.get("instance_type") or me["instance_type"],
            "SubnetId": subnet_id,
            "SecurityGroupIds": security_group_ids,
            "MinCount": 1,
            "MaxCount": 1,
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": name}],
                }
            ],
        }
        if client_token is not None:
            params["ClientToken"] = client_token
        # The injected CCM public key is the sole default SSH identity.  An
        # inherited Manager KeyName may refer to an unknown/deleted key pair
        # and silently grants a second credential.  Keep KeyName only when an
        # administrator explicitly configured WORKER_KEY_NAME.
        key_name = overrides.get("key_name")
        if key_name:
            params["KeyName"] = key_name
        params["UserData"] = build_worker_cloud_init(ssh_public_key, ssh_user)
        resp = await asyncio.to_thread(ec2.run_instances, **params)
        iid = resp["Instances"][0]["InstanceId"]
        logger.info("created worker instance %s (%s)", iid, name)
        return iid

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> str:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            info = await self.describe_instance(instance_id)
            if info["state"] == "running" and info["private_ip"]:
                return info["private_ip"]
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(
                    f"instance {instance_id} not running after {timeout}s "
                    f"(state={info['state']})"
                )
            await asyncio.sleep(5)

    async def stop_instance(self, instance_id: str) -> None:
        ec2 = await self._ec2()
        await asyncio.to_thread(ec2.stop_instances, InstanceIds=[instance_id])

    async def start_instance(self, instance_id: str) -> None:
        ec2 = await self._ec2()
        await asyncio.to_thread(ec2.start_instances, InstanceIds=[instance_id])

    async def terminate_instance(
        self,
        instance_id: str,
        *,
        allow_not_found: bool = False,
    ) -> None:
        if type(allow_not_found) is not bool:
            raise ValueError("allow_not_found must be an exact boolean")
        ec2 = await self._ec2()
        try:
            await asyncio.to_thread(
                ec2.terminate_instances, InstanceIds=[instance_id]
            )
        except Exception as exc:
            # EC2's terminate operation is idempotent from CCM's perspective:
            # a record whose instance no longer exists is already destroyed.
            # Every other AWS/IAM/network failure must propagate so the Worker
            # stays visible and the administrator can retry destruction.
            if (
                allow_not_found
                and _aws_error_code(exc) == "InvalidInstanceID.NotFound"
            ):
                logger.info("worker instance %s is already absent", instance_id)
                return
            raise

    async def update_instance_tags(self, instance_id: str, tags: dict) -> None:
        """Update tags on an EC2 instance via ec2.create_tags()."""
        ec2 = await self._ec2()
        tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
        await asyncio.to_thread(
            ec2.create_tags, Resources=[instance_id], Tags=tag_list
        )


def get_cloud_provider(provider_name: str) -> CloudProvider:
    providers: dict[str, type[CloudProvider]] = {"aws": AWSProvider}
    if provider_name not in providers:
        raise ValueError(f"unsupported cloud provider: {provider_name}")
    return providers[provider_name]()
