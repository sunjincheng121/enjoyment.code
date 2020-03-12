# -*- coding: utf-8 -*-

from pyflink.table import ScalarFunction, DataTypes
from pyflink.table.udf import udf


# Extend ScalarFunction
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add1 = udf(Add(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())


# Named Function
@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add2(i, j):
  return i + j

# Lambda Function
add3 = udf(lambda i, j: i + j, [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# Callable Function
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add4 = udf(CallableAdd(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())


