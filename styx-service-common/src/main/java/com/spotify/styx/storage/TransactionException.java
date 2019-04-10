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

public class TransactionException extends StorageException {

  public TransactionException(DatastoreException cause) {
    super(cause.getMessage() +
          ", code=" + cause.getCode() +
          ", reason=" + cause.getReason() +
          ", isRetryable=" + cause.isRetryable()
        , cause);
  }

  // TODO: represent the failure cause using an enum instead

  public boolean isConflict() {
    if (getCause() != null && getCause() instanceof DatastoreException) {
      var dex = (DatastoreException) getCause();
      // TODO: transaction closed does not technically mean that we got a transaction conflict,
      //  but the datastore client has a bug where upon encountering a 10 ABORTED error for lookup
      //  operations performed in a transaction it incorrectly immediately retries the lookup operation
      //  without restarting the transaction. The datastore service then replies with 3 INVALID_ARGUMENT
      //  "transaction closed" and the datastore client then propagates this error message.
      // NOTE: This could mis-classify other errors/bugs as conflicts as well.
      return dex.getCode() == 10 || messageStartsWith("transaction closed");
    } else {
      return false;
    }
  }

  public boolean isAlreadyExists() {
    if (getCause() != null && getCause() instanceof DatastoreException) {
      DatastoreException datastoreException = (DatastoreException) getCause();
      // TODO remove check on message when Google fixes the Datastore emulator
      return "ALREADY_EXISTS".equals(datastoreException.getReason())
             || messageStartsWith("entity already exists");
    } else {
      return false;
    }
  }

  public boolean isNotFound() {
    if (getCause() != null && getCause() instanceof DatastoreException) {
      DatastoreException datastoreException = (DatastoreException) getCause();
      // TODO remove check on message when Google fixes the Datastore emulator
      return "NOT_FOUND".equals(datastoreException.getReason())
             || messageStartsWith("no entity to update");
    } else {
      return false;
    }
  }

  private boolean messageStartsWith(String prefix) {
    final String message = getMessage();
    return message != null && message.startsWith(prefix);
  }
}
