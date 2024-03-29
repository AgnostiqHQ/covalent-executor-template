# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the Apache License 2.0 (the "License"). A copy of the
# License may be obtained with this software package or at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Use of this file is prohibited except in compliance with the License.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This is an example of a custom Covalent executor plugin."""

# For type-hints
from typing import Any, Callable, Dict, List

# The current status of the execution can be kept up-to-date with Covalent Result objects.
from covalent._results_manager.result import Result

# Importing TransportableObject to re-serialize the result for post processing
from covalent._workflow.transport import TransportableObject

# All executor plugins inherit either from the `BaseExecutor` class, or the `BaseAsyncExecutor` class.
from covalent.executor.base import BaseExecutor

# The plugin class name must be given by the EXECUTOR_PLUGIN_NAME attribute. In case this
# module has more than one class defined, this lets Covalent know which is the executor class.
executor_plugin_name = "CustomExecutor"

_EXECUTOR_PLUGIN_DEFAULTS = {
    "executor_input1": "",
    "executor_input2": "",
}


class CustomExecutor(BaseExecutor):
    def __init__(
        self,
        executor_input1: str,
        executor_input2: int = 0,
        **kwargs,
    ) -> None:
        self.executor_input1 = executor_input1
        self.executor_input2 = executor_input2
        self.kwargs = kwargs

        # Some optional arguments that will be passed to the BaseExecutor if specified
        base_kwargs = {
            key: self.kwargs[key]
            for key in self.kwargs
            if key
            in [
                "conda_env",
                "cache_dir",
                "current_env_on_conda_fail",
            ]
        }

        # Call the BaseExecutor initialization
        super().__init__(**base_kwargs)

    def run(self, function: Callable, args: List, kwargs: Dict, task_metadata: Dict) -> Any:
        """
        Run the function howsoever desired. It might be on a remote machine,
        might be by making changes to the args or kwargs, etc.

        This is the main function that should be overridden to determine how
        exactly the input function should execute.

        Args:
            function: Input function to be run
            args: Postional arguments to the input function
            kwargs: Keyword arguments to the input function
            task_metadata: Dictionary of metadata for the task. Current keys are
                          `dispatch_id` and `node_id`
        """

        # This function is where operations specific to your custom executor
        # can be defined. These operations could manipulate the function, the
        # inputs/outputs, result, etc.

        external_object = ExternalClass(task_metadata["node_id"])

        # Any print statements here will be shown in the UI
        print(external_object.multiplier)

        # Here we simply execute the function on the local machine.
        # But this could be sent to a more capable machine for the operation.
        result = function(*args, **kwargs)

        # Other custom operations can be applied here.
        if result is not None:
            result = self.helper_function(result)

        return result

    def helper_function(self, result):
        """An example helper function."""
        # Deserializing the result here - which will require its dependencies
        # installed on this environment and then re-serializing for further processing
        return TransportableObject(2 * result.get_deserialized())

    def get_status(self, info_dict: dict) -> Result:
        """
        Get the current status of the task.

        Args:
            info_dict: a dictionary containing any neccessary parameters needed to query the
                status. For this class (LocalExecutor), the only info is given by the
                "STATUS" key in info_dict.

        Returns:
            A Result status object (or None, if "STATUS" is not in info_dict).
        """

        return info_dict.get("STATUS", Result.NEW_OBJ)


# This class can be used in the custom executor, but will be ignored by the
# plugin-loader, since it is not designated as the plugin class.
class ExternalClass:
    """An example external class."""

    def __init__(self, multiplier: int):
        self.multiplier = multiplier
