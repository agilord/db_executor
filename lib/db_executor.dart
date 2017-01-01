// Copyright (c) 2016, Agilord. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library db_executor;

import 'dart:async';
import 'dart:collection';

import 'package:async/async.dart' show StreamCompleter;
import 'package:executor/executor.dart';

/// An async database task that completes with a Future.
typedef Future<R> DBQueryTask<C, R>(C connection);

/// An async database  task that completes after the Stream is closed.
typedef Stream<R> DBStreamTask<C, R>(C connection);

/// Whether a given exception is transient (e.g. the transaction should retry).
typedef bool TransientClassifier(dynamic exception);

/// Transaction isolation levels.
enum DBIsolation {
  /// The transaction reads values from ongoing operations. No data consistency.
  readUncommitted,

  /// Keeps write locks at the end of transaction, but read locks are released.
  readCommitted,

  /// Keeps both read- and write locks, however range locks are not managed.
  /// Phantom reads can occur.
  repeatableRead,

  /// The highest transaction isolation.
  serializable,
}

/// Abstraction of the database pool (basic connection handling).
abstract class DBPool<C> {
  /// Acquire a database connection and optionally begin a transaction.
  Future<C> acquire({DBIsolation isolation, bool transaction: false});

  /// Close a transaction (if any) and release the database connection.
  Future release(C connection, {bool abort: false});
}

/// Abstraction of the database pool control interface.
abstract class DBPoolFactory<C> {
  /// Create a new database connection handler.
  Future<DBPool<C>> create({DBIsolation isolation, int limit: 1});

  /// Close all database connections in [handler].
  Future close(DBPool<C> handler);
}

/// A database query executor that controls the parallelism (number of
/// concurrently running queries) and handles the acquiring and release of the
/// connections (e.g. in case of processing failure).
abstract class DBExecutor<C> {
  ///
  factory DBExecutor(DBPoolFactory<C> poolFactory,
          {int limit: 1,
          int retry: 0,
          DBIsolation isolation: DBIsolation.serializable,
          List<TransientClassifier> transientClassifiers}) =>
      new _DBExecutor(poolFactory, limit, retry, isolation,
          new UnmodifiableListView(transientClassifiers ?? []));

  /// Schedules a task.
  Future<R> schedule<R>(DBQueryTask<C, R> task,
      {int retry, DBIsolation isolation, R orElse()});

  /// Schedules a streaming task.
  Stream<R> scheduleStream<R>(DBStreamTask<C, R> task, {DBIsolation isolation});

  /// Closes the executor and the database connections.
  Future close();
}

/// The database-independent part of the [DBExecutor].
class _DBExecutor<C> implements DBExecutor<C> {
  final DBPoolFactory<C> _poolFactory;
  final int _retry;
  final DBIsolation _isolation;
  final List<TransientClassifier> _transientClassifiers;
  final Executor _executor = new Executor();
  DBPool<C> _pool;
  bool _isClosing = false;

  _DBExecutor(this._poolFactory, int limit, this._retry, this._isolation,
      this._transientClassifiers) {
    _executor.limit = limit;
  }

  @override
  Future<R> schedule<R>(DBQueryTask<C, R> callback,
      {int retry, DBIsolation isolation, R orElse()}) async {
    final int taskRetry = retry ?? _retry;
    final DBIsolation taskIsolation = isolation ?? _isolation;
    return _executor.scheduleTask(() async {
      await _initializePool();
      for (int i = 0; i <= taskRetry; i++) {
        C connection;
        try {
          connection =
              await _pool.acquire(isolation: taskIsolation, transaction: true);
          final R result = await callback(connection);
          await _pool.release(connection);
          return result;
        } catch (e, st) {
          if (connection != null) {
            await _pool.release(connection, abort: true);
          }
          if (i == taskRetry || _isNonTransientException(e)) {
            if (orElse != null) return orElse();
            rethrow;
          }
          // TODO: delay before retry
          // TODO: proper logging
          print('Transient exception: $e $st');
        }
      }
      throw new Exception('Query failed ($taskRetry retries).');
    });
  }

  @override
  Stream<R> scheduleStream<R>(DBStreamTask<C, R> callback,
      {DBIsolation isolation}) {
    final DBIsolation taskIsolation = isolation ?? _isolation;
    C connection;
    final sinkCloser = (EventSink<R> sink) {
      sink.close();
      if (connection != null) {
        _pool.release(connection);
      }
    };
    return _executor.scheduleStream(() {
      final StreamCompleter<R> streamCompleter = new StreamCompleter<R>();
      _initializePool()
          .then((_) => _pool.acquire(isolation: taskIsolation))
          .then((c) {
        connection = c;
        final Stream<R> stream = callback(connection);
        if (stream != null) {
          streamCompleter.setEmpty();
        } else {
          streamCompleter.setSourceStream(stream);
        }
      }, onError: streamCompleter.setError);
      return streamCompleter.stream.transform(
          new StreamTransformer.fromHandlers(handleDone: sinkCloser));
    });
  }

  @override
  Future close() async {
    _isClosing = true;
    await _executor.close();
    await _poolFactory.close(_pool);
  }

  Future<DBPool<C>> _initializePool() async {
    if (_isClosing) throw new Exception('DB does not accept new tasks.');
    if (_pool == null) {
      _pool = await _poolFactory.create(
          isolation: _isolation, limit: _executor.limit);
    }
    return _pool;
  }

  bool _isNonTransientException(exception) {
    if (_transientClassifiers == null) return true;
    if (_transientClassifiers.isEmpty) return true;
    if (_transientClassifiers.any((fn) => fn(exception))) return false;
    return true;
  }
}
