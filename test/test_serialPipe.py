import unittest
from unittest import TestCase

from src.SerialPipe import SerialPipe
import operator


class TestSerialPipe(TestCase):

    def test_execute_should_run_single_step_with_function_and_args(self):
        simple_pipeline_spec = {
            'single_step': {
                'function': operator.add,
                'args': [
                    'Hello ',
                    'World!'
                ]
            }
        }
        serial_pipe = SerialPipe(simple_pipeline_spec)
        actual = serial_pipe.execute()
        self.assertEqual('Hello World!', actual)

    def test_execute_should_run_single_step_with_function_and_args_dict(self):
        simple_pipeline_spec = {
            'single_step': {
                'function': lambda first, second: first + second,
                'args': {
                    'first': 'Hello ',
                    'second': 'World!'
                }
            }
        }
        serial_pipe = SerialPipe(simple_pipeline_spec)
        actual = serial_pipe.execute()
        self.assertEqual('Hello World!', actual)

    def test_execute_should_run_each_step_in_specified_order_and_pass_output_to_next_step(self):
        multi_step_pipeline_spec = {
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
        serial_pipe = SerialPipe(multi_step_pipeline_spec)
        actual = serial_pipe.execute()
        self.assertEqual(144, actual)

    def test_execute_each_step_should_accept_previous_step_output_as_input_data_along_with_additional_arguments(self):
        multi_step_pipeline_spec = {
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
                'function': lambda x: x * 2,
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
        serial_pipe = SerialPipe(multi_step_pipeline_spec)
        actual = serial_pipe.execute()
        self.assertEqual('48x1x2', actual)

    def test_get_id_should_return_unique_non_random_id(self):
        pipeline_spec1 = {
            'single_step': {
                'function': operator.add,
                'args': [
                    'Hello ',
                    'World!'
                ]
            }
        }
        pipeline_spec2 = {
            'single_step': {
                'function': lambda first, second: first + second,
                'args': {
                    'first': 'Hello ',
                    'second': 'World!'
                }
            }
        }
        pipeline1 = SerialPipe(pipeline_spec1)
        pipeline11 = SerialPipe(pipeline_spec1)
        pipeline2 = SerialPipe(pipeline_spec2)
        self.assertEqual(pipeline1.get_id(), pipeline11.get_id())
        self.assertNotEqual(pipeline1.get_id(), pipeline2.get_id())

    def test_get_outputs_should_get_saved_step_outputs(self):
        multi_step_pipeline_spec = {
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
        serial_pipe = SerialPipe(multi_step_pipeline_spec)
        serial_pipe.execute()
        step_outputs = serial_pipe.get_outputs()
        self.assertEqual('Hello World!', step_outputs['step1'])
        self.assertEqual(12, step_outputs['step2'])
        self.assertEqual(144, step_outputs['step3'])

    def test_get_outputs_should_get_saved_outputs_from_persistent_storage_based_on_id(self):
        pipeline_spec = {
            'single_step': {
                'function': operator.add,
                'args': [
                    'Hello ',
                    'World!'
                ]
            }
        }
        pipeline1 = SerialPipe(pipeline_spec)
        pipeline2 = SerialPipe(pipeline_spec)
        pipeline1.execute()
        self.assertEqual('Hello World!', pipeline2.get_outputs()['single_step'])


if __name__ == '__main__':
    unittest.main()
