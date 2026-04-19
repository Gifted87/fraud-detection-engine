import { Registry } from 'prom-client';
import { EventRepository } from './repository';
import { PostgresPoolManager } from './client';
import { CryptoValidator } from '../../../core/domain_models/security/crypto-validator.service';
import { OptimisticConcurrencyError, IntegrityViolationError } from './errors';

describe('EventRepository', () => {
  let mockClient: any;
  let mockPool: any;
  let registry: Registry;
  let repo: EventRepository<any>;

  beforeAll(() => {
    (CryptoValidator as any).instance = null;
    CryptoValidator.initialize('test-key');
  });

  beforeEach(() => {
    mockClient = {
      query: jest.fn(),
      release: jest.fn(),
    };
    mockPool = {
      connect: jest.fn().mockResolvedValue(mockClient)
    };
    (PostgresPoolManager as any).instance = {
      getPool: () => mockPool,
      getStats: () => ({})
    };

    registry = new Registry();
    repo = new EventRepository(registry);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should append event successfully', async () => {
    // Current version query returns 0
    mockClient.query.mockResolvedValueOnce({ rows: [] });
    // Insert query
    mockClient.query.mockResolvedValueOnce({});

    const envelope = { payload: { type: 'Tx' }, signature: 'sig' };
    await repo.append('agg-1', envelope as any, 1n);

    expect(mockClient.query).toHaveBeenCalledWith('BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE');
    expect(mockClient.query).toHaveBeenCalledWith('COMMIT');
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should rollback and throw OptimisticConcurrencyError on version mismatch', async () => {
    // Current version query returns 1
    mockClient.query.mockResolvedValueOnce({ rows: [{ version: '1' }] });
    
    // We try to append version 3 (expecting 2 + 1)
    const envelope = { payload: { type: 'Tx' }, signature: 'sig' };
    
    await expect(repo.append('agg-1', envelope as any, 3n)).rejects.toThrow(OptimisticConcurrencyError);
    expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should load events and throw IntegrityViolationError if signature is invalid', async () => {
    const mockRow = {
      version: '1',
      payload: { type: 'Tx' },
      signature: 'invalid_sig',
      created_at: new Date().toISOString()
    };
    
    mockClient.query.mockResolvedValueOnce({ rows: [mockRow] });
    
    const validator = CryptoValidator.getInstance();
    jest.spyOn(validator, 'verify').mockResolvedValue(false);

    await expect(repo.load('agg-1')).rejects.toThrow(IntegrityViolationError);
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should load events successfully if signature is valid', async () => {
    const mockRow = {
      version: '1',
      payload: { type: 'Tx' },
      signature: 'valid_sig',
      created_at: new Date().toISOString()
    };
    
    mockClient.query.mockResolvedValueOnce({ rows: [mockRow] });
    
    const validator = CryptoValidator.getInstance();
    jest.spyOn(validator, 'verify').mockResolvedValue(true);

    const events = await repo.load('agg-1');
    expect(events.length).toBe(1);
    expect(events[0].signature).toBe('valid_sig');
  });
});
