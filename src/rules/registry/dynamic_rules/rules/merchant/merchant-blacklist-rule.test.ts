import { Registry } from 'prom-client';
import { MerchantBlacklistRule } from './merchant-blacklist-rule';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';

describe('MerchantBlacklistRule', () => {
  let rule: MerchantBlacklistRule;
  const BLACKLIST_ID = 'prohibited-merchant-1';

  beforeEach(() => {
    const registry = new Registry();
    process.env.MERCHANT_BLACKLIST = BLACKLIST_ID;
    rule = new MerchantBlacklistRule(registry);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should flag blacklisted merchant', async () => {
    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-12',
      userId: 'user-1',
      merchantId: BLACKLIST_ID,
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const result = await rule.evaluate(transaction);

    expect(result.isSuspicious).toBe(true);
    expect(result.riskScore).toBe(1.0);
    expect(result.reason).toContain(`Merchant ${BLACKLIST_ID} is on the prohibited blacklist.`);
  });

  it('should not flag a regular merchant', async () => {
    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-6',
      userId: 'user-1',
      merchantId: 'safe-merchant-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const result = await rule.evaluate(transaction);

    expect(result.isSuspicious).toBe(false);
    expect(result.riskScore).toBe(0.0);
    expect(result.reason).toContain('Merchant is not blacklisted');
  });

  it('should handle multiple blacklisted merchants correctly', async () => {
    const registry = new Registry();
    process.env.MERCHANT_BLACKLIST = 'm1, m2, m3';
    const multiRule = new MerchantBlacklistRule(registry);

    const tx1: Transaction = { merchantId: 'm1' } as any;
    const tx2: Transaction = { merchantId: 'm2' } as any;
    const txOther: Transaction = { merchantId: 'mOther' } as any;

    expect((await multiRule.evaluate(tx1)).isSuspicious).toBe(true);
    expect((await multiRule.evaluate(tx2)).isSuspicious).toBe(true);
    expect((await multiRule.evaluate(txOther)).isSuspicious).toBe(false);
  });
});
