#!/usr/bin/env python3
"""Test script to identify the exact error in daily_data_simulator.py"""

import traceback
import sys
sys.path.append('/opt/spark/scripts')

try:
    from daily_data_simulator import DailyDataSimulator
    
    print("Creating simulator...")
    simulator = DailyDataSimulator('config/dq-config.json')
    
    print("Testing application_train simulation...")
    result = simulator.simulate_application_train_daily('2025-06-15')
    print('Success:', result)
    
except Exception as e:
    print('Error:', str(e))
    traceback.print_exc()
