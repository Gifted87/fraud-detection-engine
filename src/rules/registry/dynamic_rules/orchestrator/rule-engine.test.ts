import { Registry } from 'prom-client';
import { RuleEngine } from './rule-engine';
import { RuleRegistry } from '../registry/rule-registry';
import { KafkaMessagingClient } from '../../../../events/client/kafka_client';
import { TransactionFactory } from '../../../../core/domain_models';

describe('RuleEngine', () => {
  let mockRegistry: Registry;
  let mockRuleRegistry: any;
  let mockKafkaClient: any;
  let engine: RuleEngine;

  beforeEach(() => {
    mockRegistry = new Registry();
    mockRuleRegistry = {
      evaluateAll: jest.fn()
    };
    mockKafkaClient = {
      publish: jest.fn()
    };
    engine = new RuleEngine(mockRegistry, mockRuleRegistry, mockKafkaClient);
    
    TransactionFactory.buildTransactionFlagged = jest.fn().mockReturnValue({ type: 'TxFlagged' });
  });

  it('should evaluate transaction fully and flag if suspicious', async () => {
    mockRuleRegistry.evaluateAll.mockResolvedValueOnce({
      isSuspicious: true,
      aggregateScore: 0.9,
      results: [{ isSuspicious: true, ruleId: 'geo', reason: 'far' }]
    });

    const event = { transactionId: 'tx1', type: 'TransactionValidated' } as any;

    await engine.orchestrate(event);

    expect(mockRuleRegistry.evaluateAll).toHaveBeenCalledWith(event);
    expect(mockKafkaClient.publish).toHaveBeenCalledWith('transactions-flagged', { type: 'TxFlagged' });
  });

  it('should not publish flagging event if transaction is not suspicious', async () => {
    mockRuleRegistry.evaluateAll.mockResolvedValueOnce({
      isSuspicious: false,
      aggregateScore: 0.1,
      results: []
    });

    const event = { transactionId: 'tx2', type: 'TransactionValidated' } as any;

    await engine.orchestrate(event);

    expect(mockKafkaClient.publish).not.toHaveBeenCalled();
  });

  it('should handle and catch orchestration failure gracefully', async () => {
    mockRuleRegistry.evaluateAll.mockRejectedValueOnce(new Error('failure'));

    const event = { transactionId: 'tx3', type: 'TransactionValidated' } as any;

    await expect(engine.orchestrate(event)).resolves.toBeUndefined(); // Caught gracefully
  });
});
