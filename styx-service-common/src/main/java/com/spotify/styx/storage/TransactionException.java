/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.storage;

import com.google.cloud.datastore.DatastoreException;
import java.util.Objects;

public class TransactionException extends StorageException {

  public TransactionException(DatastoreException cause) {
    super(cause.getMessage() +
          ", code=" + cause.getCode() +
          ", reason=" + cause.getReason() +
          ", isRetryable=" + cause.isRetryable()
        , Objects.requireNonNull(cause));
  }

  @Override
  public synchronized DatastoreException getCause() {
    return (DatastoreException) super.getCause();
  }

  public boolean isConflict() {
    return isConflict(getCause());
  }

  public boolean isAlreadyExists() {
    return isAlreadyExists(getCause());
  }

  public boolean isNotFound() {
    return isNotFound(getCause());
  }

  private static boolean isConflict(DatastoreException cause) {
    return cause.getCode() == 10 || "ABORTED".equals(cause.getReason());
  }

  private static boolean isAlreadyExists(DatastoreException cause) {
    return "ALREADY_EXISTS".equals(cause.getReason())
           || messageStartsWith(cause, "entity already exists");
  }

  private static boolean isNotFound(DatastoreException cause) {
    return "NOT_FOUND".equals(cause.getReason())
           || messageStartsWith(cause, "no entity to update");
  }

  private static boolean messageStartsWith(Throwable cause, String prefix) {
    var message = cause.getMessage();
    return message != null && message.startsWith(prefix);
  }
}
