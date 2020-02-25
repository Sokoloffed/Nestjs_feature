import { Logger } from '@nestjs/common/services/logger.service';
import { loadPackage } from '@nestjs/common/utils/load-package.util';
import {
  fromEvent,
  merge,
  Subject,
  zip,
  ConnectableObservable,
  defer,
  Observable,
  Observer,
  throwError as _throw,
} from 'rxjs';
import { share, take, tap, map, mergeMap, publish } from 'rxjs/operators';
import { isNil } from '@nestjs/common/utils/shared.utils';
import {
  CONNECT_EVENT,
  ERROR_EVENT,
  MESSAGE_EVENT,
  REDIS_DEFAULT_URL,

  // IMPORTANT! ADDED CONSTANT TO PROVIDE A HANDLE MESSAGE LOGIC TO SERVER
  REDIS_LPOP_PING_VALUE,
} from '../constants';
import { InvalidMessageException } from '../errors/invalid-message.exception';
import {
  ClientOpts,
  RedisClient,
  RetryStrategyOptions,
} from '../external/redis.interface';
import { ReadPacket, RedisOptions, WritePacket } from '../interfaces';
import { ClientProxy } from './client-proxy';
import { ECONNREFUSED } from './constants';

let redisPackage: any = {};

export class ClientRedis extends ClientProxy {
  protected readonly logger = new Logger(ClientProxy.name);
  protected readonly subscriptionsCount = new Map<string, number>();
  protected readonly url: string;
  protected pubClient: RedisClient;
  protected subClient: RedisClient;

  // Custom Redis client for lpush operation
  protected listClient: RedisClient;

  protected connection: Promise<any>;
  protected isExplicitlyTerminated = false;

  constructor(protected readonly options: RedisOptions['options']) {
    super();
    this.url = this.getOptionsProp(options, 'url') || REDIS_DEFAULT_URL;

    redisPackage = loadPackage('redis', ClientRedis.name, () =>
      require('redis'),
    );

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public getAckPatternName(pattern: string): string {
    return `${pattern}_ack`;
  }

  public getResPatternName(pattern: string): string {
    return `${pattern}_res`;
  }

  public close() {
    // added closing of listClient
    this.listClient && this.listClient.quit();

    this.pubClient && this.pubClient.quit();
    this.subClient && this.subClient.quit();

    // added setting null reference to listClient
    this.pubClient = this.subClient = this.listClient = null;
    this.isExplicitlyTerminated = true;
  }

  public connect(): Promise<any> {
    // adding listClient to if clause
    if (this.pubClient && this.subClient && this.listClient) {
      return this.connection;
    }
    const error$ = new Subject<Error>();

    // creation of list client for lpush operation
    this.listClient = this.createClient(error$);

    this.pubClient = this.createClient(error$);
    this.subClient = this.createClient(error$);

    this.handleError(this.pubClient);
    this.handleError(this.subClient);

    // adding custom list client to handleError() method
    this.handleError(this.listClient);

    const pubConnect$ = fromEvent(this.pubClient, CONNECT_EVENT);
    const subClient$ = fromEvent(this.subClient, CONNECT_EVENT);

    // is it required?
    const listClient$ = fromEvent(this.listClient, CONNECT_EVENT);

    // is it required?
    this.connection = merge(error$, zip(pubConnect$, subClient$, listClient$))
      .pipe(
        take(1),
        tap(() =>
          this.subClient.on(MESSAGE_EVENT, this.createResponseCallback()),
        ),
        share(),
      )
      .toPromise();
    return this.connection;
  }

  public createClient(error$: Subject<Error>): RedisClient {
    return redisPackage.createClient({
      ...this.getClientOptions(error$),
      url: this.url,
    });
  }

  public handleError(client: RedisClient) {
    client.addListener(ERROR_EVENT, (err: any) => this.logger.error(err));
  }

  public getClientOptions(error$: Subject<Error>): Partial<ClientOpts> {
    // eslint-disable-next-line @typescript-eslint/camelcase
    const retry_strategy = (options: RetryStrategyOptions) =>
      this.createRetryStrategy(options, error$);
    return {
      // eslint-disable-next-line @typescript-eslint/camelcase
      retry_strategy,
    };
  }

  public createRetryStrategy(
    options: RetryStrategyOptions,
    error$: Subject<Error>,
  ): undefined | number | Error {
    if (options.error && (options.error as any).code === ECONNREFUSED) {
      error$.error(options.error);
    }
    if (this.isExplicitlyTerminated) {
      return undefined;
    }
    if (
      !this.getOptionsProp(this.options, 'retryAttempts') ||
      options.attempt > this.getOptionsProp(this.options, 'retryAttempts')
    ) {
      return new Error('Retry time exhausted');
    }
    return this.getOptionsProp(this.options, 'retryDelay') || 0;
  }

  public createResponseCallback(): (channel: string, buffer: string) => void {
    return (channel: string, buffer: string) => {
      const packet = JSON.parse(buffer);
      const { err, response, isDisposed, id } = this.deserializer.deserialize(
        packet,
      );

      const callback = this.routingMap.get(id);
      if (!callback) {
        return;
      }
      if (isDisposed || err) {
        return callback({
          err,
          response,
          isDisposed: true,
        });
      }
      callback({
        err,
        response,
      });
    };
  }

  // Custom Redis method
  public sendList<TResult = any, TInput = any>(
    pattern: any,
    data: TInput,
  ): Observable<TResult> {
    if (isNil(pattern) || isNil(data)) {
      return _throw(new InvalidMessageException());
    }
    return defer(async () => this.connect()).pipe(
      mergeMap(
        () =>
          new Observable((observer: Observer<TResult>) => {
            const callback = this.createObserver(observer);
            return this.publishList({ pattern, data }, callback);
          }),
      ),
    );
  }

  protected publish(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => any,
  ): Function {
    try {
      const packet = this.assignPacketId(partialPacket);
      const pattern = this.normalizePattern(partialPacket.pattern);
      const serializedPacket = this.serializer.serialize(packet);
      const responseChannel = this.getResPatternName(pattern);
      let subscriptionsCount =
        this.subscriptionsCount.get(responseChannel) || 0;

      const publishPacket = () => {
        subscriptionsCount = this.subscriptionsCount.get(responseChannel) || 0;
        this.subscriptionsCount.set(responseChannel, subscriptionsCount + 1);
        this.routingMap.set(packet.id, callback);
        this.pubClient.publish(
          this.getAckPatternName(pattern),
          JSON.stringify(serializedPacket),
        );
      };

      if (subscriptionsCount <= 0) {
        this.subClient.subscribe(
          responseChannel,
          (err: any) => !err && publishPacket(),
        );
      } else {
        publishPacket();
      }

      return () => {
        this.unsubscribeFromChannel(responseChannel);
        this.routingMap.delete(packet.id);
      };
    } catch (err) {
      callback({ err });
    }
  }

  // Custom Redis method that invokes sendList()
  protected publishList(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => any,
  ) {
    try {
      const packet = this.assignPacketId(partialPacket);
      const pattern = this.normalizePattern(partialPacket.pattern);
      const serializedPacket = this.serializer.serialize(packet);
      const responseChannel = this.getResPatternName(pattern);
      let subscriptionsCount =
        this.subscriptionsCount.get(responseChannel) || 0;

      const publishPacket = () => {
        subscriptionsCount = this.subscriptionsCount.get(responseChannel) || 0;
        this.subscriptionsCount.set(responseChannel, subscriptionsCount + 1);
        this.routingMap.set(packet.id, callback);
        this.listClient.lpush(pattern, JSON.stringify(serializedPacket));
        this.pubClient.publish(
          this.getAckPatternName(pattern),
          REDIS_LPOP_PING_VALUE,
        );
      };

      if (subscriptionsCount <= 0) {
        this.subClient.subscribe(
          responseChannel,
          (err: any) => !err && publishPacket(),
        );
      } else {
        publishPacket();
      }

      return () => {
        this.unsubscribeFromChannel(responseChannel);
        this.routingMap.delete(packet.id);
      };
    } catch (err) {
      callback({ err });
    }
  }

  // Custom Redis method
  public emitList<TResult = any, TInput = any>(
    pattern: any,
    data: TInput,
  ): Observable<TResult> {
    if (isNil(pattern) || isNil(data)) {
      return _throw(new InvalidMessageException());
    }
    const source = defer(async () => this.connect()).pipe(
      mergeMap(() => this.dispatchEventList({ pattern, data })),
      // TODO: Check - this is rxjs operator, is it ok?
      publish(),
    );
    (source as ConnectableObservable<TResult>).connect();
    return source;
  }

  protected dispatchEvent(packet: ReadPacket): Promise<any> {
    const pattern = this.normalizePattern(packet.pattern);
    const serializedPacket = this.serializer.serialize(packet);

    return new Promise((resolve, reject) =>
      this.pubClient.publish(pattern, JSON.stringify(serializedPacket), err =>
        err ? reject(err) : resolve(),
      ),
    );
  }

  // Custom Redis method
  protected dispatchEventList(packet: ReadPacket): Promise<any> {
    const pattern = this.normalizePattern(packet.pattern);
    const serializedPacket = this.serializer.serialize(packet);

    return new Promise((resolve, reject) => {
      this.listClient.lpush(pattern, JSON.stringify(serializedPacket), err =>
        err ? reject(err) : resolve(),
      );
      this.pubClient.publish(pattern, REDIS_LPOP_PING_VALUE, err =>
        err ? reject(err) : resolve(),
      );
    });
  }

  protected unsubscribeFromChannel(channel: string) {
    const subscriptionCount = this.subscriptionsCount.get(channel);
    this.subscriptionsCount.set(channel, subscriptionCount - 1);

    if (subscriptionCount - 1 <= 0) {
      this.subClient.unsubscribe(channel);
    }
  }
}
