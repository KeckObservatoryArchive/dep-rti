import os
import sys
import pytest
#import from parent dir
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parentdir)
import instrument

"""
run instrument tests with
pytest -m instrument 
"""

@pytest.mark.instrument
def test_get_oa():
	instrObj = instrument.Instrument('HIRES', None, None, None, None)
	instrObj.init()

	oa = instrObj.get_oa('2021-01-16', 1)
	assert oa == 'tridenour'

	oa = instrObj.get_oa('2021-01-17', 1)
	assert oa == 'None'
