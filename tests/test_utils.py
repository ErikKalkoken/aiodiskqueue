import unittest

from aiodiskqueue.utils import NoDirectInstantiation


class TestNoDirectInstantiation(unittest.TestCase):
    def test_should_raise_error_when_instantiated_directly(self):
        # given
        class MyClass(metaclass=NoDirectInstantiation):
            pass

        # when/then
        with self.assertRaises(TypeError):
            MyClass()

    def test_should_allow_instantiation_via_create_method(self):
        # given
        class MyClass(metaclass=NoDirectInstantiation):
            pass

        # when
        obj = MyClass._create()
        # then
        self.assertIsInstance(obj, MyClass)
