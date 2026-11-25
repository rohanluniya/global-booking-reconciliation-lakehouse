"""Durable local replayable topic broker (Kafka-compatible local_native log + DLQ + checkpoints)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class BrokerUnavailableError(RuntimeError):
    """Raised during chaos drills when the event broker is intentionally taken offline."""


@dataclass(frozen=True)
class TopicMessage:
    topic: str
    partition: int
    offset: int
    key: str
    payload: dict[str, Any]
    headers: dict[str, str]
    ingested_at: str


class LocalEventBroker:
    """Append-only partitioned JSONL event log with consumer checkpoints and replay support."""

    def __init__(self, broker_dir: Path, num_partitions: int = 4) -> None:
        self.broker_dir = broker_dir
        self.num_partitions = num_partitions
        self.online = True
        self.broker_dir.mkdir(parents=True, exist_ok=True)
        (self.broker_dir / "_checkpoints").mkdir(parents=True, exist_ok=True)

    def _partition_for_key(self, key: str) -> int:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big") % self.num_partitions

    def _partition_file(self, topic: str, partition: int) -> Path:
        tdir = self.broker_dir / topic
        tdir.mkdir(parents=True, exist_ok=True)
        return tdir / f"partition_{partition}.jsonl"

    def clear_topic(self, topic: str) -> None:
        tdir = self.broker_dir / topic
        if tdir.exists():
            for child in tdir.glob("*.jsonl"):
                child.unlink()

    def publish(
        self,
        topic: str,
        key: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> TopicMessage:
        if not self.online:
            raise BrokerUnavailableError(f"Broker is offline; cannot publish to topic '{topic}'")
        partition = self._partition_for_key(key)
        pfile = self._partition_file(topic, partition)
        offset = 0
        if pfile.exists():
            with pfile.open("r", encoding="utf-8") as fh:
                offset = sum(1 for _ in fh)
        msg = TopicMessage(
            topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            payload=payload,
            headers=headers or {},
            ingested_at=datetime.now(UTC).isoformat(),
        )
        with pfile.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "key": msg.key,
                        "payload": msg.payload,
                        "headers": msg.headers,
                        "ingested_at": msg.ingested_at,
                    }
                )
                + "\n"
            )
        return msg

    def publish_batch(
        self,
        topic: str,
        items: list[tuple[str, dict[str, Any], dict[str, str] | None]],
    ) -> int:
        if not self.online:
            raise BrokerUnavailableError(f"Broker is offline; cannot publish batch to '{topic}'")
        grouped: dict[int, list[tuple[str, dict[str, Any], dict[str, str]]]] = {}
        for key, payload, headers in items:
            part = self._partition_for_key(key)
            grouped.setdefault(part, []).append((key, payload, headers or {}))
        now = datetime.now(UTC).isoformat()
        published = 0
        for part, records in grouped.items():
            pfile = self._partition_file(topic, part)
            offset = 0
            if pfile.exists():
                with pfile.open("r", encoding="utf-8") as fh:
                    offset = sum(1 for _ in fh)
            with pfile.open("a", encoding="utf-8") as fh:
                for key, payload, headers in records:
                    fh.write(
                        json.dumps(
                            {
                                "topic": topic,
                                "partition": part,
                                "offset": offset,
                                "key": key,
                                "payload": payload,
                                "headers": headers,
                                "ingested_at": now,
                            }
                        )
                        + "\n"
                    )
                    offset += 1
                    published += 1
        return published

    def read_topic(
        self,
        topic: str,
        consumer_group: str | None = None,
        from_beginning: bool = False,
    ) -> list[TopicMessage]:
        if not self.online:
            raise BrokerUnavailableError(f"Broker is offline; cannot read topic '{topic}'")
        checkpoints = {} if from_beginning or not consumer_group else self.get_checkpoint(consumer_group, topic)
        messages: list[TopicMessage] = []
        for part in range(self.num_partitions):
            pfile = self._partition_file(topic, part)
            if not pfile.exists():
                continue
            min_offset = checkpoints.get(str(part), -1) + 1
            with pfile.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if row["offset"] >= min_offset:
                        messages.append(
                            TopicMessage(
                                topic=row["topic"],
                                partition=row["partition"],
                                offset=row["offset"],
                                key=row["key"],
                                payload=row["payload"],
                                headers=row.get("headers", {}),
                                ingested_at=row["ingested_at"],
                            )
                        )
        return messages

    def _checkpoint_file(self, consumer_group: str) -> Path:
        return self.broker_dir / "_checkpoints" / f"{consumer_group}.json"

    def get_checkpoint(self, consumer_group: str, topic: str) -> dict[str, int]:
        cfile = self._checkpoint_file(consumer_group)
        if not cfile.exists():
            return {}
        data = json.loads(cfile.read_text(encoding="utf-8"))
        return {str(k): int(v) for k, v in data.get(topic, {}).items()}

    def commit_checkpoint(self, consumer_group: str, topic: str, messages: list[TopicMessage]) -> None:
        cfile = self._checkpoint_file(consumer_group)
        data: dict[str, Any] = {}
        if cfile.exists():
            data = json.loads(cfile.read_text(encoding="utf-8"))
        topic_offsets = {str(k): int(v) for k, v in data.get(topic, {}).items()}
        for msg in messages:
            key = str(msg.partition)
            topic_offsets[key] = max(topic_offsets.get(key, -1), msg.offset)
        data[topic] = topic_offsets
        cfile.write_text(json.dumps(data, indent=2), encoding="utf-8")
