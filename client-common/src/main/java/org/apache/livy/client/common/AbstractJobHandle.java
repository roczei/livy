/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.client.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.livy.JobHandle;
import org.apache.livy.annotations.Private;

@Private
public abstract class AbstractJobHandle<T> implements JobHandle<T> {

  protected final List<Listener<T>> listeners;
  protected volatile State state;
  // Flips false -> true exactly once, only when the job actually reaches
  // State.STARTED (enforced by `changeState`: the guard
  // `newState.ordinal() > state.ordinal()` disallows re-entering STARTED,
  // so the `startedFired = true` assignment executes at most once).
  // `addListener` uses this flag to decide whether a late listener needs a
  // replayed `onJobStarted` before the current state. A terminal state
  // (FAILED / CANCELLED / SUCCEEDED) can also be reached without STARTED
  // (e.g. RPC error on submit, or the RSC client being stopped), and in
  // that case we must NOT fire an onJobStarted that never actually happened.
  //
  // Surfaced by `TestSparkClient.testJobFailure` on the -Pspark4 test
  // matrix: with the faster Netty 4.2 dispatch on loopback, the driver
  // can complete SENT -> STARTED -> FAILED before the client thread
  // finishes `handle.addListener(...)`, so without this replay Mockito
  // `verify(listener).onJobStarted(handle)` would fail with
  // `WantedButNotInvoked`.
  private volatile boolean startedFired;

  protected AbstractJobHandle() {
    this.listeners = new LinkedList<>();
    this.state = State.SENT;
    this.startedFired = false;
  }

  @Override
  public JobHandle.State getState() {
    return state;
  }

  @Override
  public void addListener(JobHandle.Listener<T> l) {
    synchronized (listeners) {
      listeners.add(l);
      // A late listener would otherwise miss `onJobStarted` if the job had
      // already progressed past STARTED before this call, so replay it once
      // here. Two guards:
      //   1. `startedFired`      -- STARTED really happened (not a job that
      //                             went SENT -> FAILED directly, e.g. RPC
      //                             error on submit or RSCClient.stop()).
      //   2. `state != STARTED`  -- the fireStateChange(state, l) call on
      //                             the next line already delivers STARTED
      //                             when we caught the lock at that state;
      //                             skip here to avoid a duplicate callback.
      //
      // Surfaced by `TestSparkClient.testJobFailure` on the -Pspark4 test
      // matrix: with the faster Netty 4.2 dispatch on loopback, the driver
      // can complete SENT -> STARTED -> FAILED before the client thread
      // finishes `handle.addListener(...)`, so without this replay Mockito
      // `verify(listener).onJobStarted(handle)` would fail with
      // `WantedButNotInvoked`.
      if (startedFired && state != State.STARTED) {
        fireStateChange(State.STARTED, l);
      }
      fireStateChange(state, l);
    }
  }

  /**
   * Changes the state of this job handle, making sure that illegal state transitions are ignored.
   * Fires events appropriately.
   *
   * As a rule, state transitions can only occur if the new state is "higher" than the current
   * state (i.e., has a higher ordinal number) and is not a "final" state. "Final" states are
   * CANCELLED, FAILED and SUCCEEDED, defined here in the code as having an ordinal number higher
   * than the CANCELLED enum constant.
   *
   * @param newState The new state to change to.
   * @return Whether the state changed.
   */
  public boolean changeState(JobHandle.State newState) {
    synchronized (listeners) {
      if (newState.ordinal() > state.ordinal() && state.ordinal() < State.CANCELLED.ordinal()) {
        // STARTED must be entered at most once per handle. The
        // `newState.ordinal() > state.ordinal()` check above should already
        // prevent this, but a future refactor of the ordering could silently
        // reintroduce a double-STARTED and break `addListener`'s replay
        // logic, so fail loudly here.
        if (newState == State.STARTED && startedFired) {
          throw new IllegalStateException(
            "STARTED entered twice for JobHandle (state=" + state + ")");
        }
        state = newState;
        if (newState == State.STARTED) {
          startedFired = true;
        }
        for (Listener<T> l : listeners) {
          fireStateChange(newState, l);
        }
        return true;
      }
      return false;
    }
  }

  protected abstract T result();
  protected abstract Throwable error();

  private void fireStateChange(State s, Listener<T> l) {
    switch (s) {
    case SENT:
      break;
    case QUEUED:
      l.onJobQueued(this);
      break;
    case STARTED:
      l.onJobStarted(this);
      break;
    case CANCELLED:
      l.onJobCancelled(this);
      break;
    case FAILED:
      l.onJobFailed(this, error());
      break;
    case SUCCEEDED:
      try {
        l.onJobSucceeded(this, result());
      } catch (Exception e) {
        // Shouldn't really happen.
        throw new IllegalStateException(e);
      }
      break;
    default:
      throw new IllegalStateException();
    }
  }

}
