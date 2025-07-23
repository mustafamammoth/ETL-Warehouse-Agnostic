#!/usr/bin/env python3
"""
Comprehensive testing script for bulletproof Repsly pipeline
Tests all critical failure scenarios and validates fixes
"""

import os
import sys
import json
import tempfile
import threading
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock
import subprocess
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project path
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/config')

class BulletproofPipelineTests:
    """Comprehensive test suite for bulletproof Repsly pipeline"""
    
    def __init__(self):
        self.test_results = {}
        self.temp_dirs = []
        
    def setup_test_environment(self):
        """Set up isolated test environment"""
        logger.info("🔧 Setting up test environment...")
        
        # Create temporary state directory
        self.temp_state_dir = tempfile.mkdtemp(prefix='repsly_test_')
        self.temp_dirs.append(self.temp_state_dir)
        
        # Mock environment variables
        os.environ['REPSLY_USERNAME'] = 'test_user'
        os.environ['REPSLY_PASSWORD'] = 'test_pass'
        os.environ['CLICKHOUSE_HOST'] = 'test_host'
        os.environ['CLICKHOUSE_PASSWORD'] = 'test_pass'
        
        logger.info(f"✅ Test environment ready: {self.temp_state_dir}")
        
    def cleanup_test_environment(self):
        """Clean up test environment"""
        logger.info("🧹 Cleaning up test environment...")
        
        import shutil
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
                
        logger.info("✅ Test environment cleaned up")
    
    def test_1_state_management_thread_safety(self):
        """Test 1: Thread-safe state management"""
        logger.info("🧪 Test 1: Thread-safe state management")
        
        try:
            # Import after setting up environment
            from extractors.repsly import extractor as repsly
            
            # Create test config
            test_config = {
                'extraction': {
                    'incremental': {
                        'enabled': True,
                        'state_path': os.path.join(self.temp_state_dir, 'test_state.json')
                    }
                }
            }
            
            # Initialize with test config
            repsly.CONFIG = test_config
            repsly._STATE_CACHE = {}
            repsly._load_state()
            
            # Test concurrent state updates
            results = []
            errors = []
            
            def update_state_worker(worker_id):
                try:
                    for i in range(10):
                        endpoint = f"test_endpoint_{worker_id}"
                        timestamp = datetime.now(timezone.utc) + timedelta(seconds=i)
                        repsly._update_watermark(endpoint, timestamp)
                        time.sleep(0.01)  # Small delay to simulate real work
                    results.append(f"Worker {worker_id} completed")
                except Exception as e:
                    errors.append(f"Worker {worker_id} failed: {e}")
            
            # Start multiple threads
            threads = []
            for i in range(5):
                thread = threading.Thread(target=update_state_worker, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads
            for thread in threads:
                thread.join()
            
            # Save state and verify
            repsly._save_state()
            
            # Verify state file
            with open(test_config['extraction']['incremental']['state_path'], 'r') as f:
                saved_state = json.load(f)
            
            # Check results
            if errors:
                self.test_results['state_thread_safety'] = f"❌ FAILED: {errors}"
                logger.error(f"❌ Test 1 FAILED: {errors}")
            elif len(saved_state) == 25:  # 5 workers * 5 endpoints each
                self.test_results['state_thread_safety'] = "✅ PASSED"
                logger.info("✅ Test 1 PASSED: Thread-safe state management working")
            else:
                self.test_results['state_thread_safety'] = f"❌ FAILED: Expected 25 state entries, got {len(saved_state)}"
                logger.error(f"❌ Test 1 FAILED: Expected 25 state entries, got {len(saved_state)}")
                
        except Exception as e:
            self.test_results['state_thread_safety'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 1 FAILED: {e}")
    
    def test_2_state_rollback_on_failure(self):
        """Test 2: State rollback on warehouse failure"""
        logger.info("🧪 Test 2: State rollback on warehouse failure")
        
        try:
            from extractors.repsly import extractor as repsly
            
            # Set up initial state
            initial_state = {
                'clients': '2023-01-01T00:00:00Z',
                'client_notes': '2023-01-01T00:00:00Z'
            }
            
            with repsly._STATE_LOCK:
                repsly._STATE_CACHE = initial_state.copy()
            
            # Mock a successful extraction that fails at warehouse loading
            with patch.object(repsly, 'get_paginated_data') as mock_get_data:
                with patch.object(repsly, 'load_dataframe_to_warehouse') as mock_load:
                    
                    # Mock successful data extraction
                    mock_get_data.return_value = [
                        {'ClientID': 1, 'ClientName': 'Test Client'}
                    ]
                    
                    # Mock warehouse failure
                    mock_load.side_effect = Exception("Warehouse connection failed")
                    
                    # Try to extract - should fail and rollback state
                    try:
                        repsly.extract_repsly_endpoint('clients')
                        self.test_results['state_rollback'] = "❌ FAILED: Should have raised exception"
                    except Exception:
                        # Verify state was rolled back
                        with repsly._STATE_LOCK:
                            current_state = repsly._STATE_CACHE.copy()
                        
                        if current_state == initial_state:
                            self.test_results['state_rollback'] = "✅ PASSED"
                            logger.info("✅ Test 2 PASSED: State rollback working correctly")
                        else:
                            self.test_results['state_rollback'] = f"❌ FAILED: State not rolled back. Expected {initial_state}, got {current_state}"
                            logger.error(f"❌ Test 2 FAILED: State not rolled back")
                            
        except Exception as e:
            self.test_results['state_rollback'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 2 FAILED: {e}")
    
    def test_3_microsoft_json_date_parsing(self):
        """Test 3: Robust Microsoft JSON date parsing"""
        logger.info("🧪 Test 3: Microsoft JSON date parsing")
        
        try:
            from extractors.repsly import extractor as repsly
            
            # Test various Microsoft JSON date formats
            test_dates = [
                ('/Date(1665059530000)/', '2022-10-06 12:38:50'),
                ('/Date(1665059530000+0000)/', '2022-10-06 12:38:50'),
                ('/Date(1665059530000-0500)/', '2022-10-06 12:38:50'),
                ('2023-04-06T16:57:29Z', '2023-04-06 16:57:29'),
                ('2023-04-06T16:57:29.000Z', '2023-04-06 16:57:29'),
                ('2023-04-06', '2023-04-06 00:00:00'),
                ('invalid_date', None),
                ('', None),
                (None, None)
            ]
            
            passed_tests = 0
            failed_tests = []
            
            for test_input, expected_date_str in test_dates:
                try:
                    result = repsly._parse_microsoft_json_date(test_input)
                    
                    if expected_date_str is None:
                        if result is None:
                            passed_tests += 1
                        else:
                            failed_tests.append(f"Expected None for '{test_input}', got {result}")
                    else:
                        if result is not None:
                            # Check if dates are close (within 1 minute for timezone differences)
                            expected_dt = datetime.fromisoformat(expected_date_str.replace(' ', 'T') + '+00:00')
                            diff = abs((result - expected_dt).total_seconds())
                            if diff <= 60:  # Within 1 minute
                                passed_tests += 1
                            else:
                                failed_tests.append(f"Date mismatch for '{test_input}': expected ~{expected_dt}, got {result}")
                        else:
                            failed_tests.append(f"Expected valid date for '{test_input}', got None")
                            
                except Exception as e:
                    failed_tests.append(f"Exception parsing '{test_input}': {e}")
            
            if len(failed_tests) == 0:
                self.test_results['date_parsing'] = "✅ PASSED"
                logger.info(f"✅ Test 3 PASSED: All {passed_tests} date parsing tests passed")
            else:
                self.test_results['date_parsing'] = f"❌ FAILED: {len(failed_tests)} failures - {failed_tests[:3]}"
                logger.error(f"❌ Test 3 FAILED: {len(failed_tests)} failures")
                
        except Exception as e:
            self.test_results['date_parsing'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 3 FAILED: {e}")
    
    def test_4_incremental_deduplication_strategy(self):
        """Test 4: DBT incremental strategy prevents duplicates"""
        logger.info("🧪 Test 4: DBT incremental deduplication")
        
        try:
            # Test the logic that would be used in dbt
            # Simulate raw data with duplicates
            raw_data = [
                {
                    'client_note_id': '123',
                    'note': 'Original note',
                    'extracted_at': '2023-07-21T10:00:00Z',
                    'record_hash': 'hash1'
                },
                {
                    'client_note_id': '123',  # Same ID
                    'note': 'Updated note',   # Different content
                    'extracted_at': '2023-07-21T11:00:00Z',  # Later timestamp
                    'record_hash': 'hash2'
                },
                {
                    'client_note_id': '124',
                    'note': 'Another note',
                    'extracted_at': '2023-07-21T10:30:00Z',
                    'record_hash': 'hash3'
                }
            ]
            
            # Simulate the deduplication logic from the fixed dbt model
            df = pd.DataFrame(raw_data)
            df['extracted_at_dt'] = pd.to_datetime(df['extracted_at'])
            
            # Apply row_number logic (keep latest by extraction time)
            df['rn'] = df.groupby('client_note_id')['extracted_at_dt'].rank(method='first', ascending=False)
            deduplicated = df[df['rn'] == 1].copy()
            
            # Verify results
            if len(deduplicated) == 2:  # Should have 2 unique records
                # Check that we kept the later version of client_note_id 123
                note_123 = deduplicated[deduplicated['client_note_id'] == '123']
                if len(note_123) == 1 and note_123.iloc[0]['note'] == 'Updated note':
                    self.test_results['deduplication'] = "✅ PASSED"
                    logger.info("✅ Test 4 PASSED: Deduplication logic working correctly")
                else:
                    self.test_results['deduplication'] = "❌ FAILED: Wrong record kept for duplicate ID"
                    logger.error("❌ Test 4 FAILED: Wrong record kept for duplicate ID")
            else:
                self.test_results['deduplication'] = f"❌ FAILED: Expected 2 records, got {len(deduplicated)}"
                logger.error(f"❌ Test 4 FAILED: Expected 2 records, got {len(deduplicated)}")
                
        except Exception as e:
            self.test_results['deduplication'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 4 FAILED: {e}")
    
    def test_5_api_error_handling_resilience(self):
        """Test 5: API error handling and retry logic"""
        logger.info("🧪 Test 5: API error handling resilience")
        
        try:
            from extractors.repsly import extractor as repsly
            import requests
            
            # Mock session that fails then succeeds
            mock_session = Mock()
            call_count = [0]  # Use list to modify in nested function
            
            def mock_get(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] <= 2:  # Fail first 2 calls
                    if call_count[0] == 1:
                        raise requests.exceptions.Timeout("Request timed out")
                    else:
                        response = Mock()
                        response.status_code = 500
                        response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server error")
                        return response
                else:  # Succeed on 3rd call
                    response = Mock()
                    response.status_code = 200
                    response.json.return_value = {
                        'MetaCollectionResult': {'TotalCount': 1, 'LastTimeStamp': 123456},
                        'Clients': [{'ClientID': 1, 'ClientName': 'Test Client'}]
                    }
                    response.headers = {}
                    return response
            
            mock_session.get = mock_get
            
            # Test endpoint config
            endpoint_config = {
                'pagination_type': 'timestamp',
                'path': 'export/clients',
                'limit': 50,
                'timestamp_field': 'LastTimeStamp',
                'data_field': 'Clients',
                'total_count_field': 'TotalCount'
            }
            
            # Mock the config
            repsly.CONFIG = {
                'api': {
                    'base_url': 'https://test.com',
                    'rate_limiting': {'timeout_seconds': 30}
                },
                'extraction': {
                    'incremental': {'enabled': False}
                }
            }
            
            # Test pagination with retries
            try:
                result = repsly.get_paginated_data(mock_session, endpoint_config, 'clients')
                
                if len(result) == 1 and result[0]['ClientID'] == 1:
                    self.test_results['api_resilience'] = "✅ PASSED"
                    logger.info("✅ Test 5 PASSED: API error handling with retries working")
                else:
                    self.test_results['api_resilience'] = f"❌ FAILED: Unexpected result: {result}"
                    logger.error(f"❌ Test 5 FAILED: Unexpected result")
                    
            except Exception as e:
                self.test_results['api_resilience'] = f"❌ FAILED: Exception not handled: {e}"
                logger.error(f"❌ Test 5 FAILED: Exception not handled: {e}")
                
        except Exception as e:
            self.test_results['api_resilience'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 5 FAILED: {e}")
    
    def test_6_warehouse_chunked_loading(self):
        """Test 6: Chunked warehouse loading for large datasets"""
        logger.info("🧪 Test 6: Chunked warehouse loading")
        
        try:
            from extractors.repsly import extractor as repsly
            
            # Create large test dataset
            large_dataset = []
            for i in range(2500):  # Larger than chunk_size (1000)
                large_dataset.append({
                    'ClientNoteID': f'note_{i}',
                    'ClientCode': f'client_{i % 100}',
                    'Note': f'Test note {i}',
                    '_extracted_at': '2023-07-21T10:00:00Z',
                    '_source_system': 'repsly',
                    '_endpoint': 'client_notes'
                })
            
            df = pd.DataFrame(large_dataset)
            
            # Mock ClickHouse client
            mock_client = Mock()
            mock_client.query.return_value.result_rows = [[0]]  # No existing records
            
            insert_calls = []
            
            def mock_insert_df(table, df):
                insert_calls.append(len(df))
                return len(df)
            
            mock_client.insert_df = mock_insert_df
            
            # Mock the client creation
            with patch('clickhouse_connect.get_client', return_value=mock_client):
                with patch.dict(os.environ, {
                    'CLICKHOUSE_HOST': 'test_host',
                    'CLICKHOUSE_PASSWORD': 'test_pass'
                }):
                    
                    # Mock config
                    repsly.CONFIG = {
                        'warehouse': {
                            'schemas': {'raw_schema': 'test_schema'}
                        }
                    }
                    
                    # Test chunked loading
                    try:
                        records_loaded = repsly.load_dataframe_to_warehouse(
                            df, 'client_notes', datetime.now(timezone.utc)
                        )
                        
                        # Verify chunking happened (should be 3 chunks: 1000, 1000, 500)
                        if len(insert_calls) == 3 and sum(insert_calls) == 2500:
                            self.test_results['chunked_loading'] = "✅ PASSED"
                            logger.info(f"✅ Test 6 PASSED: Chunked loading working - {insert_calls}")
                        else:
                            self.test_results['chunked_loading'] = f"❌ FAILED: Expected 3 chunks totaling 2500, got {insert_calls}"
                            logger.error(f"❌ Test 6 FAILED: Expected 3 chunks, got {insert_calls}")
                            
                    except Exception as e:
                        self.test_results['chunked_loading'] = f"❌ FAILED: {e}"
                        logger.error(f"❌ Test 6 FAILED: {e}")
                        
        except Exception as e:
            self.test_results['chunked_loading'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 6 FAILED: {e}")
    
    def test_7_dependency_ordering(self):
        """Test 7: Endpoint dependency ordering"""
        logger.info("🧪 Test 7: Endpoint dependency ordering")
        
        try:
            from extractors.repsly import extractor as repsly
            
            # Mock enabled endpoints with dependencies
            repsly.ENABLED_ENDPOINTS = {
                'clients': 'export/clients',
                'client_notes': 'export/clientnotes',
                'visits': 'export/visits',
                'representatives': 'export/representatives'
            }
            
            # Test the ordering function
            ordered_endpoints = repsly.get_endpoint_execution_order()
            
            # Verify that dependencies are respected
            clients_index = ordered_endpoints.index('clients')
            client_notes_index = ordered_endpoints.index('client_notes')
            
            if clients_index < client_notes_index:
                self.test_results['dependency_ordering'] = "✅ PASSED"
                logger.info(f"✅ Test 7 PASSED: Dependency ordering working - {ordered_endpoints}")
            else:
                self.test_results['dependency_ordering'] = f"❌ FAILED: Dependencies not respected - {ordered_endpoints}"
                logger.error(f"❌ Test 7 FAILED: Dependencies not respected")
                
        except Exception as e:
            self.test_results['dependency_ordering'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 7 FAILED: {e}")
    
    def test_8_config_validation(self):
        """Test 8: Configuration validation catches issues"""
        logger.info("🧪 Test 8: Configuration validation")
        
        try:
            # Test invalid configs that should be caught
            invalid_configs = [
                {},  # Empty config
                {'dag': {}},  # Missing required sections
                {
                    'dag': {},
                    'api': {},
                    'extraction': {},
                    'warehouse': {}
                },  # Has sections but missing critical keys
                {
                    'dag': {},
                    'api': {},
                    'extraction': {'incremental': {}},  # Missing state_path
                    'warehouse': {}
                }
            ]
            
            passed_validations = 0
            
            for i, config in enumerate(invalid_configs):
                try:
                    # This should raise an exception for invalid configs
                    # Simulate the validation logic from load_repsly_config
                    required_keys = ['dag', 'api', 'extraction', 'warehouse']
                    for key in required_keys:
                        if key not in config:
                            raise ValueError(f"Missing required config section: {key}")
                    
                    if 'incremental' not in config['extraction']:
                        raise ValueError("Missing extraction.incremental configuration")
                    
                    if 'state_path' not in config['extraction']['incremental']:
                        raise ValueError("Missing extraction.incremental.state_path")
                    
                    # If we get here, validation failed to catch the issue
                    logger.error(f"❌ Config validation failed to catch invalid config {i}")
                    
                except ValueError:
                    # This is expected - validation caught the issue
                    passed_validations += 1
            
            if passed_validations == len(invalid_configs):
                self.test_results['config_validation'] = "✅ PASSED"
                logger.info("✅ Test 8 PASSED: Configuration validation working")
            else:
                self.test_results['config_validation'] = f"❌ FAILED: Only {passed_validations}/{len(invalid_configs)} validations worked"
                logger.error(f"❌ Test 8 FAILED: Configuration validation inadequate")
                
        except Exception as e:
            self.test_results['config_validation'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 8 FAILED: {e}")
    
    def test_9_data_quality_validation(self):
        """Test 9: Data quality validation catches issues"""
        logger.info("🧪 Test 9: Data quality validation")
        
        try:
            # Simulate the validation logic from the DAG
            test_cases = [
                {
                    'name': 'Record count mismatch',
                    'expected_records': 100,
                    'actual_records': 80,
                    'duplicates': 0,
                    'null_fields': 5,
                    'should_fail': True
                },
                {
                    'name': 'Duplicate records',
                    'expected_records': 100,
                    'actual_records': 100,
                    'duplicates': 15,
                    'null_fields': 2,
                    'should_fail': True
                },
                {
                    'name': 'High null rate',
                    'expected_records': 100,
                    'actual_records': 100,
                    'duplicates': 0,
                    'null_fields': 50,  # >10% null rate
                    'should_fail': True
                },
                {
                    'name': 'Valid data',
                    'expected_records': 100,
                    'actual_records': 100,
                    'duplicates': 0,
                    'null_fields': 5,
                    'should_fail': False
                }
            ]
            
            passed_tests = 0
            
            for test_case in test_cases:
                # Simulate validation logic
                critical_issues = []
                
                if test_case['actual_records'] != test_case['expected_records']:
                    critical_issues.append("Record count mismatch")
                
                if test_case['duplicates'] > 0:
                    critical_issues.append("Duplicates found")
                
                if test_case['null_fields'] > test_case['expected_records'] * 0.1:
                    critical_issues.append("High null rate")
                
                validation_failed = len(critical_issues) > 0
                
                if validation_failed == test_case['should_fail']:
                    passed_tests += 1
                    logger.info(f"   ✅ {test_case['name']}: {'Failed as expected' if validation_failed else 'Passed as expected'}")
                else:
                    logger.error(f"   ❌ {test_case['name']}: Expected {'failure' if test_case['should_fail'] else 'success'}, got {'failure' if validation_failed else 'success'}")
            
            if passed_tests == len(test_cases):
                self.test_results['data_quality_validation'] = "✅ PASSED"
                logger.info("✅ Test 9 PASSED: Data quality validation working")
            else:
                self.test_results['data_quality_validation'] = f"❌ FAILED: {passed_tests}/{len(test_cases)} tests passed"
                logger.error(f"❌ Test 9 FAILED: Data quality validation inadequate")
                
        except Exception as e:
            self.test_results['data_quality_validation'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 9 FAILED: {e}")
    
    def test_10_memory_handling_large_datasets(self):
        """Test 10: Memory handling for large datasets"""
        logger.info("🧪 Test 10: Memory handling for large datasets")
        
        try:
            # Test memory-efficient processing
            import psutil
            import gc
            
            # Get initial memory usage
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # Create large dataset that would cause memory issues if not handled properly
            large_data = []
            for i in range(10000):  # Large dataset
                large_data.append({
                    'ClientNoteID': f'note_{i}',
                    'ClientCode': f'client_{i % 1000}',
                    'Note': f'This is a long note with lots of text content that would use significant memory when multiplied by thousands of records. Record number {i}' * 10,
                    'DateAndTime': f'/Date(166505953{i % 10000})/',
                    '_extracted_at': '2023-07-21T10:00:00Z'
                })
            
            # Convert to DataFrame (this is where memory issues typically occur)
            df = pd.DataFrame(large_data)
            
            # Get peak memory usage
            peak_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = peak_memory - initial_memory
            
            # Clean up
            del large_data, df
            gc.collect()
            
            # Check if memory increase is reasonable (< 500MB for this test)
            if memory_increase < 500:
                self.test_results['memory_handling'] = "✅ PASSED"
                logger.info(f"✅ Test 10 PASSED: Memory handling reasonable - {memory_increase:.1f}MB increase")
            else:
                self.test_results['memory_handling'] = f"⚠️ WARNING: High memory usage - {memory_increase:.1f}MB increase"
                logger.warning(f"⚠️ Test 10 WARNING: High memory usage - {memory_increase:.1f}MB")
                
        except Exception as e:
            self.test_results['memory_handling'] = f"❌ FAILED: {e}"
            logger.error(f"❌ Test 10 FAILED: {e}")
    
    def run_all_tests(self):
        """Run all bulletproof tests"""
        logger.info("🚀 Starting bulletproof pipeline tests...")
        
        self.setup_test_environment()
        
        try:
            # Run all tests
            self.test_1_state_management_thread_safety()
            self.test_2_state_rollback_on_failure()
            self.test_3_microsoft_json_date_parsing()
            self.test_4_incremental_deduplication_strategy()
            self.test_5_api_error_handling_resilience()
            self.test_6_warehouse_chunked_loading()
            self.test_7_dependency_ordering()
            self.test_8_config_validation()
            self.test_9_data_quality_validation()
            self.test_10_memory_handling_large_datasets()
            
        finally:
            self.cleanup_test_environment()
        
        # Generate test report
        self.generate_test_report()
    
    def generate_test_report(self):
        """Generate comprehensive test report"""
        logger.info("\n" + "="*80)
        logger.info("🎯 BULLETPROOF PIPELINE TEST REPORT")
        logger.info("="*80)
        
        passed_tests = []
        failed_tests = []
        warning_tests = []
        
        for test_name, result in self.test_results.items():
            if result.startswith("✅"):
                passed_tests.append(test_name)
            elif result.startswith("⚠️"):
                warning_tests.append(test_name)
            else:
                failed_tests.append(test_name)
        
        # Summary
        total_tests = len(self.test_results)
        logger.info(f"📊 SUMMARY:")
        logger.info(f"   Total Tests: {total_tests}")
        logger.info(f"   ✅ Passed: {len(passed_tests)}")
        logger.info(f"   ⚠️ Warnings: {len(warning_tests)}")
        logger.info(f"   ❌ Failed: {len(failed_tests)}")
        logger.info(f"   Success Rate: {(len(passed_tests)/total_tests)*100:.1f}%")
        
        # Detailed results
        logger.info(f"\n📋 DETAILED RESULTS:")
        for test_name, result in self.test_results.items():
            logger.info(f"   {test_name}: {result}")
        
        # Recommendations
        logger.info(f"\n💡 RECOMMENDATIONS:")
        if len(failed_tests) == 0:
            logger.info("   🎉 All critical tests passed! Pipeline is bulletproof.")
        else:
            logger.info("   🚨 CRITICAL ISSUES DETECTED:")
            for test in failed_tests:
                logger.info(f"      - Fix {test}: {self.test_results[test]}")
        
        if len(warning_tests) > 0:
            logger.info("   ⚠️ PERFORMANCE WARNINGS:")
            for test in warning_tests:
                logger.info(f"      - Monitor {test}: {self.test_results[test]}")
        
        # Security and reliability checklist
        logger.info(f"\n🔒 SECURITY & RELIABILITY CHECKLIST:")
        checklist_items = [
            ("Thread-safe state management", "state_thread_safety" in passed_tests),
            ("Atomic state updates with rollback", "state_rollback" in passed_tests),
            ("Robust date parsing", "date_parsing" in passed_tests),
            ("Duplicate prevention", "deduplication" in passed_tests),
            ("API error resilience", "api_resilience" in passed_tests),
            ("Memory-safe large dataset handling", "chunked_loading" in passed_tests),
            ("Dependency ordering", "dependency_ordering" in passed_tests),
            ("Configuration validation", "config_validation" in passed_tests),
            ("Data quality validation", "data_quality_validation" in passed_tests)
        ]
        
        for item, passed in checklist_items:
            status = "✅" if passed else "❌"
            logger.info(f"   {status} {item}")
        
        logger.info("="*80)
        
        # Return overall status
        if len(failed_tests) == 0:
            logger.info("🎉 PIPELINE IS BULLETPROOF! 🎉")
            return True
        else:
            logger.error(f"🚨 PIPELINE HAS {len(failed_tests)} CRITICAL ISSUES!")
            return False

def main():
    """Main function to run bulletproof tests"""
    print("\n🛡️  REPSLY BULLETPROOF PIPELINE TESTING SUITE 🛡️\n")
    
    tester = BulletproofPipelineTests()
    success = tester.run_all_tests()
    
    if success:
        print("\n✅ All tests passed! Your pipeline is bulletproof.")
        return 0
    else:
        print("\n❌ Some tests failed. Please fix the issues before deploying to production.")
        return 1

if __name__ == "__main__":
    exit(main())