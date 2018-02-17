class SerialPipe:

    def __init__(self, pipeline):
        keys = sorted(pipeline.keys(),
                      key=lambda step_name: pipeline[step_name]['order'] if 'order' in pipeline[step_name]
                      else -1)
        values = [pipeline[step_name] for step_name in keys]
        self.__pipeline_entries = zip(keys, values)

    def execute(self):
        prev_output = None
        for entry in self.__pipeline_entries:
            prev_output = self.__execute_pipeline_step(entry[1], prev_output)
        return prev_output

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
