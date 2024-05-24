import pytest
from lib.Utils import get_pyspark_session

@pytest.fixture
def spark():
    spark_session =  get_pyspark_session("LOCAL")
    yield spark_session
    spark_session.stop()

    
