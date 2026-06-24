import { useQuery } from '@tanstack/react-query';
import type { ExchangeHealth, ActuatorHealth, ActuatorMetricResponse, JvmMetrics, KafkaLagMetrics } from '@/types';

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Health fetch failed: ${response.status} ${url}`);
  }
  return response.json() as Promise<T>;
}

async function fetchActuatorMetric(metric: string, tag?: string): Promise<ActuatorMetricResponse> {
  const url = tag
    ? `/actuator/metrics/${metric}?tag=${tag}`
    : `/actuator/metrics/${metric}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Actuator metric unavailable: ${metric}`);
  return response.json() as Promise<ActuatorMetricResponse>;
}

/**
 * Polls the exchange feed health endpoint every 5 seconds.
 *
 * Returns per-exchange CONNECTED/STALE/DISCONNECTED/RECONNECTING status and
 * last-tick age from FeedHealthMonitor. 5s interval gives rapid feedback
 * when a feed goes stale (stale threshold is 5s on the backend).
 *
 * staleTime < refetchInterval: data goes stale at 4s, triggering a background
 * refetch so data is fresh before the next 5s interval fires.
 */
export function useExchangeHealth() {
  return useQuery<ExchangeHealth[]>({
    queryKey: ['health', 'exchanges'],
    queryFn: () => fetchJson<ExchangeHealth[]>('/api/health/exchanges'),
    refetchInterval: 5_000,
    staleTime: 4_000,
  });
}

/**
 * Polls Spring Actuator /actuator/health every 10 seconds for infrastructure status.
 *
 * Reads components.infrastructure.details.kafka/redis/timescaledb to determine
 * per-service UP/DOWN status. Infrastructure health rarely changes; 10s is sufficient.
 *
 * The /actuator endpoint is proxied by Vite in development. In production it would
 * either be proxied by nginx or restricted to internal networks.
 */
export function useSystemHealth() {
  return useQuery<ActuatorHealth>({
    queryKey: ['health', 'system'],
    queryFn: () => fetchJson<ActuatorHealth>('/actuator/health'),
    refetchInterval: 10_000,
    staleTime: 8_000,
  });
}

/**
 * Polls Spring Actuator for JVM heap and thread metrics every 10 seconds.
 *
 * Fetches three metrics in parallel — heap used, heap max (both filtered to
 * area:heap), and live threads. heap max changes rarely at runtime (only if
 * the JVM dynamically resizes) so it uses a 60s interval to reduce noise.
 *
 * Returns a single processed object so the HealthPage doesn't need to know
 * about the raw Actuator measurements[] array structure.
 */
export function useJvmMetrics(): { isLoading: boolean; data: JvmMetrics } {
  const heapUsed = useQuery<ActuatorMetricResponse>({
    queryKey: ['health', 'jvm', 'heap-used'],
    queryFn: () => fetchActuatorMetric('jvm.memory.used', 'area:heap'),
    refetchInterval: 10_000,
    staleTime: 8_000,
    retry: false,
  });
  const heapMax = useQuery<ActuatorMetricResponse>({
    queryKey: ['health', 'jvm', 'heap-max'],
    queryFn: () => fetchActuatorMetric('jvm.memory.max', 'area:heap'),
    refetchInterval: 60_000,
    staleTime: 55_000,
    retry: false,
  });
  const threads = useQuery<ActuatorMetricResponse>({
    queryKey: ['health', 'jvm', 'threads'],
    queryFn: () => fetchActuatorMetric('jvm.threads.live'),
    refetchInterval: 10_000,
    staleTime: 8_000,
    retry: false,
  });

  return {
    isLoading: heapUsed.isLoading || heapMax.isLoading || threads.isLoading,
    data: {
      heapUsedBytes: heapUsed.data?.measurements[0]?.value ?? 0,
      heapMaxBytes:  heapMax.data?.measurements[0]?.value ?? 0,
      liveThreads:   Math.round(threads.data?.measurements[0]?.value ?? 0),
      isAvailable:   !heapUsed.isError,
    },
  };
}

/**
 * Polls Kafka consumer lag via Spring Actuator Micrometer every 10 seconds.
 *
 * Spring Kafka automatically registers kafka.consumer.records-lag-max with
 * Micrometer for each consumer group. The metric may not exist until at least
 * one consumer has fetched records, so errors are handled gracefully (isAvailable=false).
 *
 * Colour coding: green < 10, yellow 10-100, red > 100.
 * A lag of 0 means the consumer is keeping up with the producer in real time.
 */
export function useKafkaLag(): { isLoading: boolean; data: KafkaLagMetrics } {
  const query = useQuery<ActuatorMetricResponse>({
    queryKey: ['health', 'kafka', 'consumer-lag'],
    queryFn: () => fetchActuatorMetric('kafka.consumer.records-lag-max'),
    refetchInterval: 10_000,
    staleTime: 8_000,
    retry: false,
  });

  return {
    isLoading: query.isLoading,
    data: {
      maxLag:      Math.round(query.data?.measurements[0]?.value ?? 0),
      isAvailable: !query.isError && query.data !== undefined,
    },
  };
}
