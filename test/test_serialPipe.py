import unittest
from unittest import TestCase

from src.SerialPipe import SerialPipe
import operator


class TestSerialPipe(TestCase):

    def test_execute_should_run_single_step_with_function_and_args(self):
        simple_pipeline = {
            'single_step': {
                'function': operator.add,
                'args': [
                    'Hello ',
                    'World!'
                ]
            }
        }
        serial_pipe = SerialPipe(simple_pipeline)
        actual = serial_pipe.execute()
        self.assertEqual('Hello World!', actual)

    def test_execute_should_run_single_step_with_function_and_args_dict(self):
        simple_pipeline = {
            'single_step': {
                'function': lambda first, second: first + second,
                'args': {
                    'first': 'Hello ',
                    'second': 'World!'
                }
            }
        }
        serial_pipe = SerialPipe(simple_pipeline)
        actual = serial_pipe.execute()
        self.assertEqual('Hello World!', actual)

    def test_execute_should_run_each_step_in_specified_order_and_pass_output_to_next_step(self):
        multi_step_pipeline = {
            'step1': {
                'function': operator.add,
                'args': [
                    'Hello ',
                    'World!'
                ],
                'order': 0
            },
            'step3': {
                'function': lambda x: x ** 2,
                'order': 2
            },
            'step2': {
                'function': len,
                'order': 1
            }
        }
        serial_pipe = SerialPipe(multi_step_pipeline)
        actual = serial_pipe.execute()
        self.assertEqual(144, actual)

    def test_execute_each_step_should_accept_previous_step_output_as_input_data_along_with_additional_arguments(self):
        multi_step_pipeline = {
            'step1': {
                'function': lambda first, second: first + second,
                'args': {
                    'first': 'Hello ',
                    'second': 'World!'
                },
                'order': 0
            },
            'step2': {
                'function': operator.add,
                'args': [
                    'Hello again!'
                ],
                'order': 1
            },
            'step3': {
                'function': lambda x: x*2,
                'order': 2
            },
            'step4': {
                'function': lambda input_data, first, second: "{}x{}x{}".format(str(len(input_data)), first, second),
                'args': {
                    'first': '1',
                    'second': '2'
                },
                'order': 3
            },
        }
        serial_pipe = SerialPipe(multi_step_pipeline)
        actual = serial_pipe.execute()
        self.assertEqual('48x1x2', actual)


if __name__ == '__main__':
    unittest.main()
