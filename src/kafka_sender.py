"""Kafka producer with Confluent Schema Registry (JSON Schema from file).

Requires: pip install confluent-kafka authlib cachetools orjson
  (or: poetry install --with kafka)

Environment (optional):
  KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
  SCHEMA_REGISTRY_URL       default http://localhost:8081
  KAFKA_TOPIC               default bakehouse_recommendation
  KAFKA_VALUE_SCHEMA_PATH   override path to value JSON Schema file

CLI:
  python src/kafka_sender.py
  python src/kafka_sender.py --mode register
  python src/kafka_sender.py --mode test-compat

Evolution: edit schemas/<topic>-value.json, run test-compat, then register.
  Subject name follows TopicNameStrategy: "<topic>-value".
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def value_subject(topic: str) -> str:
    return f"{topic}-value"


def default_schema_path(topic: str) -> Path:
    return repo_root() / "schemas" / f"{topic}-value.json"


def resolve_schema_path(topic: str, override: Path | None) -> Path:
    if override is not None:
        return override.expanduser().resolve()
    env = os.environ.get("KAFKA_VALUE_SCHEMA_PATH")
    if env:
        return Path(env).expanduser().resolve()
    return default_schema_path(topic)


def load_json_schema_string(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    json.loads(text)
    return text


def schema_from_path(path: Path) -> Schema:
    return Schema(load_json_schema_string(path), schema_type="JSON")


def make_schema_registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


def register_value_schema(
    *,
    topic: str,
    schema_registry_url: str,
    schema_path: Path,
    normalize: bool = False,
) -> int:
    """Register the file as the next version for subject ``<topic>-value``. Returns schema id."""
    client = make_schema_registry_client(schema_registry_url)
    subject = value_subject(topic)
    registered = client.register_schema_full_response(
        subject,
        schema_from_path(schema_path),
        normalize_schemas=normalize,
    )
    return registered.schema_id


def test_value_schema_compatibility(
    *,
    topic: str,
    schema_registry_url: str,
    schema_path: Path,
) -> bool:
    """Return True if schema in file is compatible with subject config (vs latest)."""
    client = make_schema_registry_client(schema_registry_url)
    subject = value_subject(topic)
    return client.test_compatibility(subject, schema_from_path(schema_path))


def _value_to_dict(obj: object, _ctx: object) -> dict:
    if isinstance(obj, dict):
        return obj
    raise TypeError("produce() value must be a dict for this serializer")


def make_producer(
    *,
    bootstrap_servers: str,
    schema_registry_url: str,
    schema_path: Path,
) -> SerializingProducer:
    schema_str = load_json_schema_string(schema_path)
    sr = make_schema_registry_client(schema_registry_url)
    value_serializer = JSONSerializer(schema_str, sr, _value_to_dict)
    return SerializingProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": value_serializer,
        }
    )


def acked(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def cmd_send(args: argparse.Namespace) -> None:
    path = resolve_schema_path(args.topic, args.schema_path)
    if not path.is_file():
        print(f"Schema file not found: {path}", file=sys.stderr)
        sys.exit(1)
    p = make_producer(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
        schema_path=path,
    )
    for i in range(10):
        p.produce(args.topic, key=str(i), value={"id": i, "msg": "hello"}, on_delivery=acked)
    p.flush(10)


def cmd_register(args: argparse.Namespace) -> None:
    path = resolve_schema_path(args.topic, args.schema_path)
    if not path.is_file():
        print(f"Schema file not found: {path}", file=sys.stderr)
        sys.exit(1)
    schema_id = register_value_schema(
        topic=args.topic,
        schema_registry_url=args.schema_registry,
        schema_path=path,
        normalize=args.normalize,
    )
    print(f"Registered {value_subject(args.topic)} schema_id={schema_id} from {path}")


def cmd_test_compat(args: argparse.Namespace) -> None:
    path = resolve_schema_path(args.topic, args.schema_path)
    if not path.is_file():
        print(f"Schema file not found: {path}", file=sys.stderr)
        sys.exit(1)
    ok = test_value_schema_compatibility(
        topic=args.topic,
        schema_registry_url=args.schema_registry,
        schema_path=path,
    )
    if ok:
        print(f"Compatible: {path} -> {value_subject(args.topic)}")
    else:
        print(f"NOT compatible: {path} -> {value_subject(args.topic)}", file=sys.stderr)
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--mode",
        choices=("send", "register", "test-compat"),
        default="send",
        help="send: produce samples; register: SR register only; test-compat: validate vs SR",
    )
    p.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "bakehouse_recommendation"),
        help="Kafka topic (subject will be <topic>-value)",
    )
    p.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        metavar="HOST:PORT",
    )
    p.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        metavar="URL",
    )
    p.add_argument(
        "--schema-path",
        type=Path,
        default=None,
        help="JSON Schema file (default: repo schemas/<topic>-value.json)",
    )
    p.add_argument(
        "--normalize",
        action="store_true",
        help="(register only) let SR normalize schema before storing",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dispatch = {
        "send": cmd_send,
        "register": cmd_register,
        "test-compat": cmd_test_compat,
    }
    dispatch[args.mode](args)


if __name__ == "__main__":
    main()
