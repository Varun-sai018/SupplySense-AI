import unittest
import os
import sys

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

class TestConfig(unittest.TestCase):
    def test_configuration_loads(self):
        """Test that the configuration can be imported and values are accessible."""
        # Ensure we have mock environment variables before importing settings
        # to avoid missing env var exceptions if .env is missing.
        os.environ['DB_HOST'] = 'test_host'
        os.environ['DB_PORT'] = '3306'
        os.environ['DB_USER'] = 'test_user'
        os.environ['DB_PASSWORD'] = 'test_pass'
        os.environ['DB_NAME'] = 'test_db'
        os.environ['KAFKA_BROKER'] = 'test_broker'
        os.environ['KAFKA_TOPIC_EVENTS'] = 'test_topic'
        os.environ['KAFKA_CONSUMER_GROUP'] = 'test_group'
        os.environ['OLIST_DATA_DIR'] = 'test_dir'

        import importlib
        import config.settings
        importlib.reload(config.settings)
        from config import settings
        
        self.assertEqual(settings.DB_HOST, 'test_host')
        self.assertEqual(settings.DB_PORT, 3306)
        self.assertEqual(settings.DB_USER, 'test_user')
        self.assertEqual(settings.KAFKA_BROKER, 'test_broker')
        self.assertEqual(settings.KAFKA_TOPIC_EVENTS, 'test_topic')
        self.assertEqual(settings.KAFKA_CONSUMER_GROUP, 'test_group')
        self.assertEqual(settings.OLIST_DATA_DIR, 'test_dir')

    def tearDown(self):
        from dotenv import load_dotenv
        import importlib
        import config.settings
        load_dotenv(override=True)
        importlib.reload(config.settings)

if __name__ == '__main__':
    unittest.main()
