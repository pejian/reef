/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver.client;

import com.google.protobuf.ByteString;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.client.JobMessageObserver;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobControlProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.JobStatusProto;
import com.microsoft.reef.runtime.common.driver.DriverRuntimeConfigurationOptions;
import com.microsoft.reef.runtime.common.driver.DriverShutdownManager;
import com.microsoft.reef.runtime.common.driver.api.AbstractDriverRuntimeConfiguration;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.runtime.RuntimeClock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends JobStatus reports to the client side.
 */
@DriverSide
@Unit
public final class ClientJobStatusHandler implements JobMessageObserver {

  private final static Logger LOG = Logger.getLogger(ClientJobStatusHandler.class.getName());

  private final static ObjectSerializableCodec<Throwable> CODEC = new ObjectSerializableCodec<>();

  private final RuntimeClock clock;

  private final String jobID;

  private final ClientConnection clientConnection;

  private final AutoCloseable jobControlChannel;

  private final DriverShutdownManager driverShutdownManager;

  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;
  private boolean closed = false;

  @Inject
  public ClientJobStatusHandler(
      final RemoteManager remoteManager,
      final RuntimeClock clock,
      final @Parameter(DriverRuntimeConfigurationOptions.JobControlHandler.class) EventHandler<JobControlProto> jobControlHandler,
      final @Parameter(AbstractDriverRuntimeConfiguration.JobIdentifier.class) String jobID,
      final @Parameter(AbstractDriverRuntimeConfiguration.ClientRemoteIdentifier.class) String clientRID,
      final ClientConnection clientConnection,
      final DriverShutdownManager driverShutdownManager) {

    this.clock = clock;
    this.jobID = jobID;
    this.clientConnection = clientConnection;
    this.driverShutdownManager = driverShutdownManager;

    // Get a handler for sending job status messages to the client
    this.jobControlChannel = remoteManager.registerHandler(clientRID, JobControlProto.class, jobControlHandler);
  }


  @Override
  public synchronized void onNext(final byte[] message) {
    LOG.log(Level.FINEST, "Job message from {0}", this.jobID);
    this.sendInit();
    this.clientConnection.send(JobStatusProto.newBuilder()
        .setIdentifier(this.jobID.toString())
        .setState(ReefServiceProtos.State.RUNNING)
        .setMessage(ByteString.copyFrom(message))
        .build());
  }

  @Override
  public synchronized void onError(final Throwable exception) {
    this.driverShutdownManager.onError(exception);
  }


  private synchronized void sendInit() {
    if (state == ReefServiceProtos.State.INIT) {
      this.clientConnection.send(JobStatusProto.newBuilder()
          .setIdentifier(this.jobID.toString())
          .setState(ReefServiceProtos.State.INIT)
          .build());
      this.state = ReefServiceProtos.State.RUNNING;
    }
  }

  /**
   * Wake based event handler for sending job messages to the client
   */
  public final class JobMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      ClientJobStatusHandler.this.onNext(message);
    }
  }

  /**
   * Wake based event handler for sending job exceptions to the client
   */
  public final class JobExceptionHandler implements EventHandler<Exception> {
    @Override
    public void onNext(final Exception exception) {
      ClientJobStatusHandler.this.onError(exception);
    }
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime time) {
      LOG.log(Level.FINEST, "StartTime: {0}", time);
      ClientJobStatusHandler.this.sendInit();
    }
  }
}
