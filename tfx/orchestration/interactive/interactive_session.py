# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""TFX interactive session for iterative development.

See `examples/chicago_taxi_pipeline/taxi_pipeline_interactive.ipynb` for an
example of how to run TFX in a Jupyter notebook for iterative development.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import logging
import os
import tempfile
from typing import Text


from ml_metadata.proto import metadata_store_pb2
from tfx.components.base import base_component
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration.component_launcher import ComponentLauncher


class InteractiveSession(object):
  """TFX interactive session for interactive TFX notebook development."""

  def __init__(
      self,
      base_dir: Text = None,
      metadata_connection_config: metadata_store_pb2.ConnectionConfig = None):
    if not base_dir:
      base_dir = tempfile.mkdtemp(prefix='tfx-')
      logging.warning(
          'InteractiveSession base_dir argument not provided: using temporary '
          'directory %s as base_dir.',
          base_dir)
    if not metadata_connection_config:
      metadata_sqlite_path = os.path.join(base_dir, 'metadata.sqlite')
      metadata_connection_config = metadata.sqlite_metadata_connection_config(
          metadata_sqlite_path)
      logging.warning(
          'InteractiveSession metadata_connection_config not provided: using '
          'sqlite metadata database at %s.',
          metadata_sqlite_path)
    self.base_dir = base_dir
    self.metadata_connection_config = metadata_connection_config
    self.pipeline_name = 'interactive-%s' % datetime.datetime.now().isoformat()

  def run(self,
          component: base_component.BaseComponent):
    """Run a given TFX component in the interactive session."""
    run_id = datetime.datetime.now().isoformat()
    pipeline_info = data_types.PipelineInfo(
        self.pipeline_name, self.base_dir, run_id=run_id)
    driver_args = data_types.DriverArgs('worker-1', self.base_dir, True)
    additional_pipeline_args = {}
    for name, output in component.outputs.items():
      for artifact in output.get():
        artifact.pipeline_name = self.pipeline_name
        artifact.producer_component = component.component_name
        artifact.run_id = run_id
        artifact.name = name
    launcher = ComponentLauncher(component, pipeline_info, driver_args,
                                 self.metadata_connection_config,
                                 additional_pipeline_args)
    execution_id = launcher.run()
    return ExecutionResult(component, execution_id)


class ExecutionResult(object):
  """Execution result from a component run in an InteractiveSession."""

  def __init__(self,
               component: base_component.BaseComponent,
               execution_id: int):
    self.component = component
    self.execution_id = execution_id

  def __repr__(self):
    outputs_parts = []
    for name, channel in self.component.outputs.items():
      repr_string = '%s: %s' % (name, repr(channel))
      for line in repr_string.split('\n'):
        outputs_parts.append(line)
    outputs_str = '\n'.join('        %s' % line for line in outputs_parts)
    return ('ExecutionResult(\n    component: %s'
            '\n    execution_id: %s'
            '\n    outputs:\n%s'
            ')') % (self.component.component_name,
                    self.execution_id,
                    outputs_str)
