import sys
import os
sys.path.insert(0,
                os.path.abspath(
                    os.path.join(
                        os.path.abspath(
                            os.path.dirname(__file__)),
                        '.')))
import unittest
from moira.checker import state, expression


class Expression(unittest.TestCase):

    def testDefault(self):
        self.assertEqual(expression.getExpression(t1=10, warn_value=60, error_value=90), state.OK)
        self.assertEqual(expression.getExpression(t1=60, warn_value=60, error_value=90), state.WARN)
        self.assertEqual(expression.getExpression(t1=90, warn_value=60, error_value=90), state.ERROR)
        self.assertEqual(expression.getExpression(t1=40, warn_value=30, error_value=10), state.OK)
        self.assertEqual(expression.getExpression(t1=20, warn_value=30, error_value=10), state.WARN)
        self.assertEqual(expression.getExpression(t1=10, warn_value=30, error_value=10), state.ERROR)
        self.assertEqual(expression.getExpression(**{'t1': 10, 'warn_value': 30, 'error_value': 10}), state.ERROR)

    def testCustom(self):
        self.assertEqual(expression.getExpression("ERROR if t1 > 10 and t2 > 3 else OK", t1=11, t2=4), state.ERROR)
        with self.assertRaises(expression.ExpressionError):
            expression.getExpression("ERROR if f.min(t1,t2) else OK", t1=11, t2=4)
        with self.assertRaises(expression.ExpressionError):
            expression.getExpression("(lambda f: ())", t1=11, t2=4)
