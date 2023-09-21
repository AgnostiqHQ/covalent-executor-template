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

"""Tests for Covalent custom async executor."""

from functools import partial

import covalent as ct
import covalent._results_manager.results_manager as rm
import pytest
from covalent._workflow.transport import TransportableObject
from covalent.executor.base import wrapper_fn

from covalent_executor_template.custom_async import CustomAsyncExecutor


def test_init():
    """Test that initialization properly sets member variables."""

    arguments = {
        "executor_input1": "input1",
        "executor_input2": 5,
        "kwarg": "custom param",
        "current_env_on_conda_fail": True,
    }

    executor = CustomAsyncExecutor(**arguments)

    assert executor.executor_input1 == "input1"
    assert executor.executor_input2 == 5
    assert executor.kwargs["kwarg"] == "custom param"
    assert executor.current_env_on_conda_fail is True


@pytest.mark.asyncio
async def test_deserialization(mocker):
    """Test that the input function and its arguments are deserialized."""

    executor = CustomAsyncExecutor(executor_input1="input1")

    def simple_task(x, kw):
        return x

    transportable_func = TransportableObject(simple_task)
    input_function = partial(wrapper_fn, transportable_func, [], [])
    arg = TransportableObject(5)
    args = [arg]
    kwarg = TransportableObject(None)
    kwargs = {"kw": kwarg}

    deserialized_func_mock = mocker.patch.object(
        transportable_func,
        "get_deserialized",
        return_value=simple_task,
    )

    deserialized_args_mock = mocker.patch.object(
        arg,
        "get_deserialized",
        return_value=5,
    )

    deserialized_kwargs_mock = mocker.patch.object(
        kwarg,
        "get_deserialized",
        return_value=None,
    )

    await executor.run(
        function=input_function,
        args=args,
        kwargs=kwargs,
        task_metadata={"dispatch_id": 0, "node_id": 0},
    )

    deserialized_func_mock.assert_called_once()
    deserialized_args_mock.assert_called_once()
    deserialized_kwargs_mock.assert_called_once()


@pytest.mark.asyncio
async def test_function_call(mocker):
    """Test that the deserialized function is called with correct arguments."""

    executor = CustomAsyncExecutor(executor_input1="input1")

    simple_task = mocker.MagicMock(return_value=0)
    simple_task.__name__ = "simple_task"

    transport_function = TransportableObject(simple_task)

    deserialized_func_mock = mocker.patch.object(
        transport_function,
        "get_deserialized",
        return_value=simple_task,
    )

    args = [TransportableObject(5)]
    kwargs = {"kw_arg": TransportableObject(10)}

    await executor.run(
        function=partial(wrapper_fn, transport_function, [], []),
        args=args,
        kwargs=kwargs,
        task_metadata={"dispatch_id": 0, "node_id": 0},
    )

    deserialized_func_mock.assert_called_once()
    simple_task.assert_called_once_with(5, kw_arg=10)


def test_final_result():
    """Functional test to check that the result of the function execution is as expected."""

    executor = CustomAsyncExecutor(executor_input1="input1")

    @ct.electron(executor=executor)
    def simple_task(x):
        return x

    @ct.lattice
    def sample_workflow(a):
        c = simple_task(a)
        return c

    dispatch_id = ct.dispatch(sample_workflow)(5)
    print(dispatch_id)
    result = ct.get_result(dispatch_id=dispatch_id, wait=True)
    print(result)

    rm._delete_result(dispatch_id)

    # The custom executor has a doubling of each electron's result, for illustrative purporses.
    assert result.result == 10
