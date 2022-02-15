# Copyright 2021 Agnostiq Inc.
#
# This file is a part of Covalent Cloud.

# All executor plugins inherit from the BaseExecutor base class.
from covalent.executor import BaseExecutor

# DispatchInfo objects are used to share info of a dispatched computation between different
# tasks (electrons) of the workflow (lattice).
from covalent._shared_files.util_classes import DispatchInfo

# The function to be computed is in the form of a Covalent TransportableObject.
# This import is not strictly necessary, as it is only used for type-hints.
from covalent._workflow.transport import TransportableObject

# The covalent logger module is a simple wrapper around the
# standard Python logging module.
from covalent._shared_files import logger
app_log = logger.app_log
log_stack_info = logger.log_stack_info

# These modules are used to capture print/logging statements
# inside electron definitions.
import io
from contextlib import redirect_stderr, redirect_stdout

# For type-hints
from typing import Any

# The plugin class name must be given by the executor_plugin_name attribute. In case this
# module has more than one class defined, this lets Covalent know which is the executor class.
executor_plugin_name = "CustomExecutor"


class CustomExecutor(BaseExecutor):
    def __init__(
        # Inputs to an executor can be positional or keyword arguments.
        self,
        executor_input1: str,
        executor_input2: int = 0,
        *args,
        **kwargs,

    ) -> None:
        self.executor_input1 = executor_input1
        self.executor_input2 = executor_input2
        self.args = args
        self.kwargs = kwargs

        # Call the BaseExecutor initialization:
        # There are a number of optional arguments to BaseExecutor
        # that could be specfied as keyword inputs to CustomExecutor.
        base_kwargs = {}
        for key in self.kwargs:
            if key in [
                "log_stdout",
                "log_stderr",
                "conda_env",
                "cache_dir",
                "current_env_on_conda_fail",
            ]:
                base_kwargs[key] = self.kwargs[key]

        super().__init__(**base_kwargs)
    

    def execute(
        self,
        function: TransportableObject,
        kwargs: Any,
        execution_args: dict,
        dispatch_id: str,
        node_id: int = -1,
    ) -> Any:

        """
        Executes the input function and returns the result.

        Args:
            function: The input (serialized) python function which will be executed and
                whose result is ultimately returned by this function.
            kwargs: Keyword arguments to be used by function.
            execution_args: Executor-specific arguments.
            dispatch_id: The unique identifier of the external lattice process which is
                calling this function.
            node_id: The node ID of this task in the workflow graph.

        Returns:
            output: The result of the executed function.
        """

        # Convert the function to be executed, which is in the form of
        # a Covalent TransportableObject, to a 'standard' Python function:
        fn = function.get_deserialized()
        app_log.debug(type(fn))

        # In this block is where operations specific to your custom executor
        # can be defined. These operations could manipulate the function, the
        # inputs/outputs, or anything that Python allows.
        external_object = ExternalClass(3)
        app_log.debug(external_object.multiplier)

        logs, errors = "", ""
        with self.get_dispatch_context(DispatchInfo(dispatch_id)), redirect_stdout(
            io.StringIO()
        ) as stdout, redirect_stderr(io.StringIO()) as stderr:
            # Here we simply execute the function on the local machine.
            # But this could be sent to a more capable machine for the operation,
            # or a different Python virtual environment, or more.
            result = fn(**kwargs)

            logs, errors = stdout.getvalue(), stderr.getvalue()

        # Other custom operations can be applied here.
        result = self.helper_function(result)

        debug_message = f"Function '{fn.__name__}' was executed on node {node_id} with execution arguments {execution_args}"
        app_log.debug(debug_message)

        return (result, logs, errors)

    def helper_function(self, result):
        return 2 * result


# This class can be used in the custom executor, but will be ignored by the
# plugin-loader, since it is not designated as the plugin class.
class ExternalClass:
    def __init__(self, multiplier: int):
        self.multiplier = multiplier
