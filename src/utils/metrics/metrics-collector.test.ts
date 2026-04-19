import { Registry, Histogram, Counter, Gauge } from 'prom-client';
import { MetricsCollector } from './metrics-collector';

describe('MetricsCollector', () => {
  let registry: Registry;
  let metrics: MetricsCollector;
  
  beforeEach(() => {
    registry = new Registry();
    (MetricsCollector as any).instance = undefined;
    metrics = MetricsCollector.initialize(registry);
  });

  it('should initialize and return singleton instance correctly', () => {
    expect(metrics).toBeDefined();
    expect(MetricsCollector.getInstance()).toBe(metrics);
  });

  it('should throw if getInstance called before initialize', () => {
    (MetricsCollector as any).instance = undefined;
    expect(() => MetricsCollector.getInstance()).toThrow();
  });

  it('should record latency without throwing errors', () => {
    const labels = { environment: 'test', component: 'test-comp', stream_name: 'test-stream' } as const;
    expect(() => {
      metrics.observeLatency(labels, 'test-op', process.hrtime.bigint());
    }).not.toThrow();
  });

  it('should increment throughput counter safely', () => {
    const labels = { environment: 'test', component: 'test-comp', stream_name: 'test-stream' } as const;
    expect(() => {
      metrics.incrementThroughput(labels, 'success');
    }).not.toThrow();
  });

  it('should set gauge safely', () => {
    const labels = { environment: 'test', component: 'test-comp', stream_name: 'test-stream' } as const;
    expect(() => {
      metrics.setGauge(labels, 'test-metric', 100);
    }).not.toThrow();
  });

  it('should instrument successful asynchronous operations', async () => {
    const labels = { environment: 'test', component: 'test-comp', stream_name: 'test-stream' } as const;
    const result = await metrics.instrument(labels, 'test-async-op', async () => {
      return 'success-value';
    });
    
    expect(result).toBe('success-value');
  });

  it('should instrument and rethrow on asynchronous errors', async () => {
    const labels = { environment: 'test', component: 'test-comp', stream_name: 'test-stream' } as const;
    
    await expect(metrics.instrument(labels, 'test-error-op', async () => {
      throw new Error('test-error');
    })).rejects.toThrow('test-error');
  });
});
