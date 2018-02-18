import pickle
import os
import hashlib


class SerialPipe:

    def __init__(self, pipeline_spec):
        keys = sorted(pipeline_spec.keys(),
                      key=lambda step_name: pipeline_spec[step_name]['order'] if 'order' in pipeline_spec[step_name]
                      else -1)
        values = [pipeline_spec[step_name] for step_name in keys]
        self.__pipeline_entries = zip(keys, values)
        self.__step_outputs = {}
        self.__id = hashlib.md5(str(pipeline_spec).encode('UTF-8')).hexdigest()

    def execute(self):
        prev_output = None
        for entry in self.__pipeline_entries:
            prev_output = self.__execute_pipeline_step(entry[1], prev_output)
            self.__step_outputs[entry[0]] = prev_output
            with open(self.__id, 'wb') as f:
                pickle.dump(self.__step_outputs, f)
        return prev_output

    def get_outputs(self):
        if self.__step_outputs == {}:
            if os.path.exists(self.__id):
                with open(self.__id, 'rb') as f:
                    self.__step_outputs = pickle.load(f)
        return self.__step_outputs

    def get_id(self):
        return self.__id

    @staticmethod
    def __execute_pipeline_step(pipeline_step, input_data):
        func = pipeline_step['function']
        if 'args' not in pipeline_step:
            return func(input_data)
        args = pipeline_step['args']
        if isinstance(args, dict):
            if input_data is None:
                return func(**args)
            else:
                return func(input_data=input_data, **args)
        elif isinstance(args, list):
            if input_data is None:
                return func(*args)
            else:
                return func(input_data, *args)
