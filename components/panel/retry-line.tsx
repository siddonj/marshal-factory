"use client";

import { retryLine, type AutoRetry } from "@/lib/retry";
import { cn } from "@/lib/utils";
import { useNow } from "./time";

/** A watchdog failure's automatic retry, counting down to it. */
export function RetryLine({ retry, className }: { retry: AutoRetry; className?: string }) {
  const now = useNow(1000);
  return <p className={cn("text-xs text-warning", className)}>{retryLine(retry, now ?? undefined)}</p>;
}
