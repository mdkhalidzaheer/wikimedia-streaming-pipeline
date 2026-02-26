import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WikimediaProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='wikimedia.recentchanges'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5
        )
        self.url = "https://stream.wikimedia.org/v2/stream/recentchange"
        self.event_count = 0
        self.start_time = time.time()
    
    def produce(self):
        # ✅ FIX: Add User-Agent header (Wikimedia policy)
        headers = {
            'User-Agent': 'DataEngPortfolio/1.0 (contact: your-email@example.com; learning real-time pipelines)'
        }
        
        logger.info(f"🚀 Starting producer for topic: {self.topic}")
        logger.info("⏳ Waiting for Wikipedia edits (1-5/min)...")
        
        try:
            response = requests.get(self.url, stream=True, timeout=300, headers=headers)
            response.raise_for_status()
            logger.info(f"✅ Stream connected! Status: {response.status_code}")
            
            for line_num, line in enumerate(response.iter_lines()):
                if line and line.startswith(b'data: '):
                    try:
                        event_data = json.loads(line[6:].decode('utf-8'))
                        self.producer.send(self.topic, value=event_data)
                        self.event_count += 1
                        
                        elapsed = time.time() - self.start_time
                        event_type = event_data.get('type', 'unknown').upper()
                        title = event_data.get('title', 'N/A')[:40]
                        logger.info(f"📤 #{self.event_count} | {event_type} | {title}... | {elapsed:.0f}s")
                        
                        if self.event_count >= 3:  # Stop after 3 for testing
                            logger.info("🎉 3 events sent! Ready for consumer.")
                            time.sleep(3)
                            break
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON error line {line_num}: {e}")
                elif line_num % 50 == 0 and line_num > 0:
                    logger.info(f"Heartbeat: Processed {line_num} lines...")
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Stream failed: {e}")
            logger.error(f"Response: {e.response.text if hasattr(e, 'response') else 'No response'}") # type: ignore
    
    def close(self):
        logger.info(f"Flushing final {self.event_count} events...")
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    producer = WikimediaProducer()
    try:
        producer.produce()
    except KeyboardInterrupt:
        logger.info("Shutdown...")
    finally:
        producer.close()
