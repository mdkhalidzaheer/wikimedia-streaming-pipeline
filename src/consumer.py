import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WikimediaConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='wikimedia.recentchanges'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='wikimedia-fresh-2',       # ← bumped to fresh group
            consumer_timeout_ms=15000
        )
        self.db_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="wikievents",
            user="dataeng",
            password="dataeng123"
        )
        self.db_conn.autocommit = False

    def enrich_event(self, event):
        return {
            'event_id':        event.get('id'),
            'event_timestamp': datetime.fromtimestamp(event.get('timestamp', 0)),
            'user_name':       event.get('user'),
            'page_title':      event.get('title'),           # ← THIS WAS MISSING
            'wiki':            event.get('wiki'),
            'change_type':     event.get('type'),
            'is_bot':          event.get('bot', False),
            'is_minor':        event.get('minor', False),
            'comment':         event.get('comment', '')[:200],
            'url':             event.get('meta', {}).get('uri', ''),
            'bytes_changed':   (
                event.get('length', {}).get('new', 0) -
                event.get('length', {}).get('old', 0)
            ),
            'raw_event':       Json(event)
        }

    def _flush_batch(self, cursor, batch):
        insert_query = """
            INSERT INTO wiki_events (
                event_id, event_timestamp, user_name, page_title,
                wiki, change_type, is_bot, is_minor,
                comment, url, bytes_changed, raw_event
            )
            VALUES (
                %(event_id)s, %(event_timestamp)s, %(user_name)s, %(page_title)s,
                %(wiki)s, %(change_type)s, %(is_bot)s, %(is_minor)s,
                %(comment)s, %(url)s, %(bytes_changed)s, %(raw_event)s
            )
            ON CONFLICT (event_id) DO NOTHING
        """
        cursor.executemany(insert_query, batch)
        self.db_conn.commit()
        logger.info(f"💾 Flushed {len(batch)} events to PostgreSQL")

    def process_messages(self):
        logger.info("🗄️  Consumer started. Writing to PostgreSQL...")
        cursor = self.db_conn.cursor()
        batch_size = 3
        batch = []
        count = 0

        try:
            for message in self.consumer:
                try:
                    enriched = self.enrich_event(message.value)
                    batch.append(enriched)
                    count += 1
                    logger.info(f"✅ Queued #{count} | {enriched['change_type']} | {enriched['page_title']}")

                    if len(batch) >= batch_size:
                        self._flush_batch(cursor, batch)
                        self.consumer.commit()
                        batch = []

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.db_conn.rollback()

        except Exception as e:
            logger.info(f"Consumer stopped: {e}")

        finally:
            if batch:
                logger.info(f"Flushing remaining {len(batch)} events...")
                self._flush_batch(cursor, batch)
                self.consumer.commit()

            logger.info(f"✅ Total events written to DB: {count}")
            cursor.close()
            self.db_conn.close()
            self.consumer.close()


if __name__ == "__main__":
    consumer = WikimediaConsumer()
    consumer.process_messages()
