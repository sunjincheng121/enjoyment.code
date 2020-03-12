import pandas as pd
from scipy import stats
from pyflink.table import DataTypes
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE(), udf_type="pandas")
def cdf(v):
    return pd.Series(stats.norm.cdf(v))

