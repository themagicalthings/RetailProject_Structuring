import pytest
from lib.DataReader import read_customer, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.configReader import get_app_config, get_pyspark_config



def test_read_customer_df(spark) :
    customers_count = read_customer(spark,"LOCAL").count()
    assert customers_count == 12435


def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count ==68884

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.transformation
def test_count_orders_state(spark, expected_results):
    customers_df = read_customer(spark,"LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()

@pytest.mark.latest()
def test_check_closed_count(spark):
    orders_df = read_orders(spark,'LOCAL')
    filtered_count = filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556

# parameterized generic test cases
@pytest.mark.parametrize(
        "status, count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT", 15030),
         ("COMPLETE",22900)]
)
def test_check_count(spark,status,count):
    orders_df = read_orders(spark,'LOCAL')
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count