import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
import threading
import time

# ================== Logging ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ================== Kafka Config ==================
KAFKA_CONFIG = {
    "bootstrap.servers": "192.168.235.136:9092",
    "topic": "transaction_data"
}

# ================== Consumer Config ==================
CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_CONFIG["bootstrap.servers"],
    'group.id': 'realtime_transaction_processor',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 10000,
    'max.poll.interval.ms': 300000,
    'fetch.min.bytes': 1,
}

class RealTimeKafkaProcessor:
    def __init__(self):
        self.consumer = None
        self.running = True
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()
        
    def connect_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = Consumer(CONSUMER_CONFIG)
            self.consumer.subscribe([KAFKA_CONFIG["topic"]])
            logger.info(f"✅ Connected to Kafka topic: {KAFKA_CONFIG['topic']}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def validate_transaction_data(self, data: Dict[str, Any]) -> bool:
        """Validate transaction data structure"""
        required_fields = ['transaction_id', 'transaction_amount', 'transaction_type']
        
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"⚠️ Missing required field: {field}")
                return False
        return True
    
    def process_transaction(self, transaction_data: Dict[str, Any]):
        """Process individual transaction - customize this method"""
        try:
            # Basic validation
            if not self.validate_transaction_data(transaction_data):
                self.error_count += 1
                return
            
            # Extract key fields
            tx_id = transaction_data.get('transaction_id')
            amount = transaction_data.get('transaction_amount', 0)
            tx_type = transaction_data.get('transaction_type')
            status = transaction_data.get('transaction_status')
            channel = transaction_data.get('channel')
            
            # Custom processing logic here
            if amount > 10000:  # High value transaction
                logger.info(f"🚨 HIGH VALUE: ID={tx_id}, Amount={amount}, Type={tx_type}")
            
            if status == 'FAILED':
                logger.warning(f"⚠️ FAILED TRANSACTION: ID={tx_id}, Amount={amount}")
            
            # Print summary for demo
            logger.info(f"💳 Transaction: ID={tx_id[:8]}..., Amount=${amount}, Type={tx_type}, Channel={channel}")
            
            self.processed_count += 1
            
        except Exception as e:
            logger.error(f"❌ Error processing transaction: {e}")
            self.error_count += 1
    
    def parse_message(self, message_value: str) -> Dict[str, Any]:
        """Parse Kafka message value"""
        try:
            # Try JSON first
            return json.loads(message_value)
        except json.JSONDecodeError:
            try:
                # Handle Python dict string format (fallback)
                import ast
                return ast.literal_eval(message_value)
            except (ValueError, SyntaxError):
                logger.error(f"❌ Cannot parse message: {message_value[:100]}...")
                return {}
    
    def print_stats(self):
        """Print processing statistics"""
        elapsed = time.time() - self.start_time
        rate = self.processed_count / elapsed if elapsed > 0 else 0
        
        logger.info(f"📊 Stats: Processed={self.processed_count}, Errors={self.error_count}, "
                   f"Rate={rate:.2f} msg/sec, Runtime={elapsed:.1f}s")
    
    def start_processing(self):
        """Start the main processing loop"""
        if not self.connect_consumer():
            return
        
        logger.info("🚀 Starting real-time transaction processing...")
        
        # Stats reporting thread
        def stats_reporter():
            while self.running:
                time.sleep(30)  # Report every 30 seconds
                if self.running:
                    self.print_stats()
        
        stats_thread = threading.Thread(target=stats_reporter, daemon=True)
        stats_thread.start()
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("End of partition reached")
                    else:
                        logger.error(f"❌ Consumer error: {msg.error()}")
                    continue
                
                # Process message
                try:
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = msg.value().decode('utf-8')
                    
                    # Parse and process
                    transaction_data = self.parse_message(value)
                    if transaction_data:
                        self.process_transaction(transaction_data)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    self.error_count += 1
                
        except KeyboardInterrupt:
            logger.info("🛑 Received stop signal...")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        
        self.print_stats()
        logger.info("✅ Kafka consumer stopped cleanly")

# ================== Advanced Processing Examples ==================
class AdvancedTransactionProcessor(RealTimeKafkaProcessor):
    """Extended processor with advanced features"""
    
    def __init__(self):
        super().__init__()
        self.high_value_threshold = 50000
        self.fraud_patterns = ['TEST', 'DUMMY', 'FAKE']
        self.transaction_buffer = []
        self.buffer_size = 100
    
    def detect_fraud(self, transaction_data: Dict[str, Any]) -> bool:
        """Simple fraud detection logic"""
        description = transaction_data.get('description', '').upper()
        amount = transaction_data.get('transaction_amount', 0)
        
        # Check for suspicious patterns
        if any(pattern in description for pattern in self.fraud_patterns):
            return True
        
        # Check for unusual amounts
        if amount > self.high_value_threshold:
            return True
            
        return False
    
    def process_transaction(self, transaction_data: Dict[str, Any]):
        """Enhanced transaction processing"""
        try:
            if not self.validate_transaction_data(transaction_data):
                self.error_count += 1
                return
            
            tx_id = transaction_data.get('transaction_id')
            amount = transaction_data.get('transaction_amount', 0)
            tx_type = transaction_data.get('transaction_type')
            
            # Fraud detection
            if self.detect_fraud(transaction_data):
                logger.warning(f"🚨 POTENTIAL FRAUD: ID={tx_id}, Amount={amount}")
            
            # Buffer transactions for batch processing
            self.transaction_buffer.append(transaction_data)
            
            if len(self.transaction_buffer) >= self.buffer_size:
                self.process_batch(self.transaction_buffer.copy())
                self.transaction_buffer.clear()
            
            # Real-time logging
            logger.info(f"💳 TX: {tx_id[:8]}... | ${amount:,.2f} | {tx_type}")
            self.processed_count += 1
            
        except Exception as e:
            logger.error(f"❌ Error in advanced processing: {e}")
            self.error_count += 1
    
    def process_batch(self, transactions):
        """Process batch of transactions"""
        total_amount = sum(tx.get('transaction_amount', 0) for tx in transactions)
        logger.info(f"📦 Batch processed: {len(transactions)} transactions, Total: ${total_amount:,.2f}")

# ================== Main ==================
def main():
    logger.info("🚀 Starting Real-Time Kafka Transaction Processor...")
    
    # Choose processor type
    use_advanced = True  # Set to False for basic processing
    
    if use_advanced:
        processor = AdvancedTransactionProcessor()
        logger.info("Using Advanced Transaction Processor")
    else:
        processor = RealTimeKafkaProcessor()
        logger.info("Using Basic Transaction Processor")
    
    try:
        processor.start_processing()
    except Exception as e:
        logger.error(f"❌ Failed to start processor: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()