export class StreamdError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "StreamdError";
  }
}

export class StreamdNetworkError extends StreamdError {
  constructor(
    message: string,
    public readonly statusCode?: number,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = "StreamdNetworkError";
  }
}

export class StreamdPollingError extends StreamdError {
  constructor(
    message: string,
    public readonly consecutiveFailures: number,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = "StreamdPollingError";
  }
}
