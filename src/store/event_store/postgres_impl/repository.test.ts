import { Registry } from 'prom-client';
import { EventRepository } from './repository';
import { PostgresPoolManager } from './client';
import { CryptoManager } from '../../../utils/security/crypto';
import { OptimisticConcurrencyError, IntegrityViolationError } from './errors';

describe('EventRepository', () => {
  let mockClient: any;
  let mockPool: any;
  let registry: Registry;
  let repo: EventRepository<any>;
  let mockLogger: any;
  let mockConfig: any;

  beforeAll(() => {
    (CryptoManager as any).instance = null;
    CryptoManager.initialize(
      'test-enc-key',
      'test-salt-long-enough',
      '-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA1PSK//wZg2vQhgUBDn2HvG0Y8IVau0iGjFBw4TBvGSc=\n-----END PUBLIC KEY-----',
      '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIC8Wb4tn/NTE4alDqkPL/Hgd7V9fE4rUCN3JqC4wHMn9\n-----END PRIVATE KEY-----'
    );
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
    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
      fatal: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn()
    };
    mockConfig = {
      DB_URL: 'postgres://localhost:5432/db',
      DB_POOL_MIN: 1,
      DB_POOL_MAX: 1,
      NODE_ENV: 'test'
    };
    repo = new EventRepository({ registry, logger: mockLogger, config: mockConfig as any });
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should append event successfully', async () => {
    // 1. BEGIN
    mockClient.query.mockResolvedValueOnce({});
    // 2. INSERT (returning rowCount: 1)
    mockClient.query.mockResolvedValueOnce({ rowCount: 1 });
    // 3. COMMIT
    mockClient.query.mockResolvedValueOnce({});

    const envelope = { payload: { type: 'Tx' }, signature: 'sig' };
    await repo.append('agg-1', envelope as any);

    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining('BEGIN'));
    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining('INSERT INTO events'), expect.any(Array));
    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining('COMMIT'));
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should rollback and throw OptimisticConcurrencyError on conflict', async () => {
    // 1. BEGIN
    mockClient.query.mockResolvedValueOnce({});
    // 2. INSERT (fails with 23505 Unique Violation)
    const error: any = new Error('duplicate key value violates unique constraint');
    error.code = '23505';
    mockClient.query.mockRejectedValueOnce(error);
    // 3. ROLLBACK
    mockClient.query.mockResolvedValueOnce({});
    
    const envelope = { payload: { type: 'Tx' }, signature: 'sig' };
    
    await expect(repo.append('agg-1', envelope as any)).rejects.toThrow(OptimisticConcurrencyError);
    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining('ROLLBACK'));
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
    
    const manager = CryptoManager.getInstance();
    jest.spyOn(manager, 'verifyEvent').mockResolvedValue(false);

    await expect(repo.load('agg-1')).rejects.toThrow(IntegrityViolationError);
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should load events successfully if signature is valid', async () => {
    const mockRow = {
      version: '1',
      payload: { type: 'Tx' },
      metadata: { createdAtNs: '1000000000', schemaVersion: 'v1.0' },
      signature: 'valid_sig',
      created_at: new Date().toISOString()
    };
    
    mockClient.query.mockResolvedValueOnce({ rows: [mockRow] });
    
    const manager = CryptoManager.getInstance();
    jest.spyOn(manager, 'verifyEvent').mockResolvedValue(true);

    const events = await repo.load('agg-1');
    expect(events.length).toBe(1);
    expect(events[0].signature).toBe('valid_sig');
  });
});
