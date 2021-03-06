/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.styx.serialization;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.PropertyNamingStrategy.SNAKE_CASE;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spotify.styx.model.Event;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.TypeWrapperModule;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import okio.ByteString;

public final class Json {

  private static final TypeWrapperModule ADT_MODULE = new TypeWrapperModule()
      .setupWrapping(
          Event.class,
          PersistentEvent.class,
          PersistentEvent::wrap,
          PersistentEvent::toEvent)
      .setupWrapping(
          Trigger.class,
          PersistentTrigger.class,
          PersistentTrigger::wrap,
          PersistentTrigger::toTrigger);

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(SNAKE_CASE)
      .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY)
      .disable(WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(ADT_MODULE)
      .disable(FAIL_ON_UNKNOWN_PROPERTIES)
      .registerModule(new JavaTimeModule())
      .registerModule(new Jdk8Module())
      .registerModule(new AutoMatterModule());

  public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
      .setPropertyNamingStrategy(SNAKE_CASE)
      .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY)
      .disable(WRITE_DATES_AS_TIMESTAMPS)
      .disable(FAIL_ON_UNKNOWN_PROPERTIES)
      .registerModule(ADT_MODULE)
      .registerModule(new Jdk8Module())
      .registerModule(new JavaTimeModule())
      .registerModule(new AutoMatterModule());

  private Json() {
    throw new UnsupportedOperationException();
  }

  public static ByteString serialize(Object value) throws JsonProcessingException {
    return ByteString.of(OBJECT_MAPPER.writeValueAsBytes(value));
  }

  public static <T> T deserialize(ByteString json, Class<T> clazz) throws IOException {
    return OBJECT_MAPPER.readValue(json.toByteArray(), clazz);
  }

  public static Event deserializeEvent(ByteString json) throws IOException {
    return OBJECT_MAPPER.readValue(json.toByteArray(), Event.class);
  }

  public static Trigger deserializeTrigger(ByteString json) throws IOException {
    return OBJECT_MAPPER.readValue(json.toByteArray(), Trigger.class);
  }

  public static String deterministicStringUnchecked(Object value) {
    final ObjectWriter writer = Json.OBJECT_MAPPER.writer().with(ORDER_MAP_ENTRIES_BY_KEYS);
    try {
      return writer.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
