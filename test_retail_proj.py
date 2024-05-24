import pytest
from lib.Utils import get_pyspark_session
from lib.DataReader import read_customers
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders, filter_order_genric
from lib.ConfigReader import get_app_config



def test_read_customers_df(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

def test_read_orders_df(spark):   
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_counts = filter_closed_orders(orders_df).count()
    assert filtered_counts == 7556

def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == 'data/orders.csv'

@pytest.mark.latest
def test_check_closed_count(spark):
      orders_df = read_orders(spark,"LOCAL")
      filtered_counts = filter_order_genric(orders_df,'CLOSED').count()
      assert filtered_counts == 7556
