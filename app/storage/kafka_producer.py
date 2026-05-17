import json
import logging
from typing import Any, Dict
from confluent_kafka import Producer
from app.core.config import settings

logger = logging.getLogger(__name__)


class SalesforceKafkaProducer:
    """
    Thread-safe event streaming hub for broadcasting normalized
    Salesforce record payloads downstream to the Glynac pipeline.
    """

    def __init__(self):
        self.enabled = True  # Bus actively driven by configuration settings
        self.producer = None

        # Map incoming Salesforce object strings to their dedicated schema topics
        self._topic_mapping = {
            "Contact": settings.KAFKA_SF_CONTACTS_TOPIC,
            "Account": settings.KAFKA_SF_ACCOUNTS_TOPIC,
            "Opportunity": settings.KAFKA_SF_OPPORTUNITIES_TOPIC,
            "Activity": settings.KAFKA_SF_ACTIVITIES_TOPIC,
            "Lead": settings.KAFKA_SF_LEADS_TOPIC,
            "User": settings.KAFKA_SF_USERS_TOPIC,
            "CampaignMember": settings.KAFKA_SF_CAMPAIGNS_TOPIC,
        }

        conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "black-diamond-salesforce-service",
            "acks": "all",  # Guarantee full cluster acknowledgement for financial/CRM data safety
            "compression.type": "snappy",
            "linger.ms": 20,  # Micro-batching window to boost network throughput
        }

        try:
            logger.info(
                f"Initializing Kafka cluster producer connection to: {settings.KAFKA_BOOTSTRAP_SERVERS}"
            )
            self.producer = Producer(conf)
        except Exception as e:
            logger.critical(
                f"Fatal error initializing Kafka cluster connection footprint: {e}"
            )
            raise e

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback executed by the worker pool once broker cluster confirms receipt."""
        if err is not None:
            logger.error(f"Event delivery failed on downstream broker bus: {err}")
        else:
            logger.debug(
                f"Event acknowledged successfully: Topic {msg.topic()} | Partition [{msg.partition()}]"
            )

    def publish_record(
        self,
        record: Dict[str, Any],
        object_type: str,
        org_id: str,
        scan_id: str,
        sf_job_id: str,
        page_num: int,
        extracted_at: str,
    ) -> None:
        """
        Wraps a sanitized CRM record into the official Glynac corporate JSON
        envelope schema and streams it asynchronously to the target topic.
        """
        if not self.producer:
            return

        target_topic = self._topic_mapping.get(object_type)
        if not target_topic:
            logger.error(
                f"Streaming dropped: Unknown topic mapping target for object type '{object_type}'."
            )
            return

        # Enforce the strict corporate JSON tracking structure outlined in Section 11.2
        envelope = {
            "meta": {
                "source": "salesforce",
                "object": object_type,
                "org_id": org_id,
                "scan_id": scan_id,
                "sf_job_id": sf_job_id,
                "page": page_num,
                "extracted_at": extracted_at,
            },
            "record": record,
        }

        try:
            # Route using record 'Id' as the Kafka partitioning key to maintain record sequencing on the same node
            record_id = str(record.get("Id", ""))
            payload_bytes = json.dumps(envelope, default=str).encode("utf-8")

            self.producer.produce(
                topic=target_topic,
                key=record_id.encode("utf-8") if record_id else None,
                value=payload_bytes,
                callback=self._delivery_report,
            )
        except BufferError:
            logger.warning(
                "Kafka local queue buffer full. Executing blocking flush refresh..."
            )
            self.producer.poll(0.5)
            # Re-attempt submission post queue relief
            self.producer.produce(
                target_topic, value=payload_bytes, callback=self._delivery_report
            )
        except Exception as e:
            logger.error(
                f"Failed to submit message sequence to Kafka topic cluster: {e}"
            )

    def flush_bus(self, timeout: float = 10.0) -> int:
        """Blocks execution until all flying in-flight events are fully delivered."""
        if self.producer:
            logger.info("Flushing event stream buffer to brokers...")
            return self.producer.flush(timeout)
        return 0
