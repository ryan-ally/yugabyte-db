// Copyright (c) YugaByte, Inc.
package com.yugabyte.yw.common;

import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ShellProcessContext {
  public static final ShellProcessContext DEFAULT = ShellProcessContext.builder().build();

  // Env vars for this command
  Map<String, String> extraEnvVars;
  // Whether to log stdout&stderr to application.log or not
  boolean logCmdOutput;
  // Human-readable description for logging
  String description;
  // Used to track this execution, can be null
  UUID uuid;
  // Args that will be added to the cmd but will be redacted in logs
  Map<String, String> sensitiveData;
  // Abort the command forcibly if it takes longer than this
  long timeoutSecs;
}
