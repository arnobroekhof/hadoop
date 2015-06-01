/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Erasure coding schema to housekeeper relevant information.
 */
public final class ECSchema {
  public static final String NUM_DATA_UNITS_KEY = "k";
  public static final String NUM_PARITY_UNITS_KEY = "m";
  public static final String CODEC_NAME_KEY = "codec";

  /**
   * A friendly and understandable name that can mean what's it, also serves as
   * the identifier that distinguish it from other schemas.
   */
  private final String schemaName;

  /**
   * The erasure codec name associated.
   */
  private final String codecName;

  /**
   * Number of source data units coded
   */
  private final int numDataUnits;

  /**
   * Number of parity units generated in a coding
   */
  private final int numParityUnits;

  /*
   * An erasure code can have its own specific advanced parameters, subject to
   * itself to interpret these key-value settings.
   */
  private final Map<String, String> extraOptions;

  /**
   * Constructor with schema name and provided all options. Note the options may
   * contain additional information for the erasure codec to interpret further.
   * @param schemaName schema name
   * @param allOptions all schema options
   */
  public ECSchema(String schemaName, Map<String, String> allOptions) {
    assert (schemaName != null && ! schemaName.isEmpty());

    this.schemaName = schemaName;

    if (allOptions == null || allOptions.isEmpty()) {
      throw new IllegalArgumentException("No schema options are provided");
    }

    this.codecName = allOptions.get(CODEC_NAME_KEY);
    if (codecName == null || codecName.isEmpty()) {
      throw new IllegalArgumentException("No codec option is provided");
    }

    int tmpNumDataUnits = extractIntOption(NUM_DATA_UNITS_KEY, allOptions);
    int tmpNumParityUnits = extractIntOption(NUM_PARITY_UNITS_KEY, allOptions);
    if (tmpNumDataUnits < 0 || tmpNumParityUnits < 0) {
      throw new IllegalArgumentException(
          "No good option for numDataUnits or numParityUnits found ");
    }
    this.numDataUnits = tmpNumDataUnits;
    this.numParityUnits = tmpNumParityUnits;

    allOptions.remove(CODEC_NAME_KEY);
    allOptions.remove(NUM_DATA_UNITS_KEY);
    allOptions.remove(NUM_PARITY_UNITS_KEY);
    // After some cleanup
    this.extraOptions = Collections.unmodifiableMap(allOptions);
  }

  /**
   * Constructor with key parameters provided.
   * @param schemaName
   * @param codecName
   * @param numDataUnits
   * @param numParityUnits
   */
  public ECSchema(String schemaName, String codecName,
                  int numDataUnits, int numParityUnits) {
    this(schemaName, codecName, numDataUnits, numParityUnits, null);
  }

  /**
   * Constructor with key parameters provided. Note the extraOptions may contain
   * additional information for the erasure codec to interpret further.
   * @param schemaName
   * @param codecName
   * @param numDataUnits
   * @param numParityUnits
   * @param extraOptions
   */
  public ECSchema(String schemaName, String codecName, int numDataUnits,
                  int numParityUnits, Map<String, String> extraOptions) {

    assert (schemaName != null && ! schemaName.isEmpty());
    assert (codecName != null && ! codecName.isEmpty());
    assert (numDataUnits > 0 && numParityUnits > 0);

    this.schemaName = schemaName;
    this.codecName = codecName;
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;

    if (extraOptions == null) {
      extraOptions = new HashMap<>();
    }

    // After some cleanup
    this.extraOptions = Collections.unmodifiableMap(extraOptions);
  }

  private int extractIntOption(String optionKey, Map<String, String> options) {
    int result = -1;

    try {
      if (options.containsKey(optionKey)) {
        result = Integer.parseInt(options.get(optionKey));
        if (result <= 0) {
          throw new IllegalArgumentException("Bad option value " + result +
              " found for " + optionKey);
        }
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Option value " +
          options.get(optionKey) + " for " + optionKey +
          " is found. It should be an integer");
    }

    return result;
  }

  /**
   * Get the schema name
   * @return schema name
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Get the codec name
   * @return codec name
   */
  public String getCodecName() {
    return codecName;
  }

  /**
   * Get extra options specific to a erasure code.
   * @return extra options
   */
  public Map<String, String> getExtraOptions() {
    return extraOptions;
  }

  /**
   * Get required data units count in a coding group
   * @return count of data units
   */
  public int getNumDataUnits() {
    return numDataUnits;
  }

  /**
   * Get required parity units count in a coding group
   * @return count of parity units
   */
  public int getNumParityUnits() {
    return numParityUnits;
  }

  /**
   * Make a meaningful string representation for log output.
   * @return string representation
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ECSchema=[");

    sb.append("Name=" + schemaName + ", ");
    sb.append("Codec=" + codecName + ", ");
    sb.append(NUM_DATA_UNITS_KEY + "=" + numDataUnits + ", ");
    sb.append(NUM_PARITY_UNITS_KEY + "=" + numParityUnits);
    sb.append((extraOptions.isEmpty() ? "" : ", "));

    int i = 0;
    for (String opt : extraOptions.keySet()) {
      sb.append(opt + "=" + extraOptions.get(opt) +
          (++i < extraOptions.size() ? ", " : ""));
    }

    sb.append("]");

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ECSchema ecSchema = (ECSchema) o;

    if (numDataUnits != ecSchema.numDataUnits) {
      return false;
    }
    if (numParityUnits != ecSchema.numParityUnits) {
      return false;
    }
    if (!schemaName.equals(ecSchema.schemaName)) {
      return false;
    }
    if (!codecName.equals(ecSchema.codecName)) {
      return false;
    }
    return extraOptions.equals(ecSchema.extraOptions);
  }

  @Override
  public int hashCode() {
    int result = schemaName.hashCode();
    result = 31 * result + codecName.hashCode();
    result = 31 * result + extraOptions.hashCode();
    result = 31 * result + numDataUnits;
    result = 31 * result + numParityUnits;

    return result;
  }
}