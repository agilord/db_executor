// Copyright (c) 2016, Agilord. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

import 'dart:async';

import 'package:db_executor/db_executor.dart';
import 'package:postgresql/postgresql.dart' show Connection, TransactionState;
import 'package:postgresql/pool.dart' show Pool, PoolSettings;

/// Postgresql database pool factory.
class PostgresqlPoolFactory implements DBPoolFactory<Connection> {
  final PoolSettings _poolSettings;

  /// The pool's name (for debugging).
  final String poolName;

  /// Postgresql database pool factory.
  PostgresqlPoolFactory.fromPgSettings(this._poolSettings, {this.poolName});

  @override
  Future<DBPool<Connection>> create(
      {DBIsolation isolation, int limit: 1}) async {
    final Map map = _poolSettings.toMap();
    map['maxConnections'] = limit;
    if (poolName != null) {
      map['poolName'] = poolName;
    }
    final PoolSettings settings = new PoolSettings.fromMap(map);
    final Pool pool = new Pool.fromSettings(settings);
    await pool.start();
    return new _PGPool(pool);
  }

  @override
  Future close(DBPool<Connection> pool) async {
    final _PGPool dbpool = pool;
    await dbpool._pool.stop();
  }
}

class _PGPool implements DBPool<Connection> {
  final Pool _pool;

  _PGPool(this._pool);

  @override
  Future<Connection> acquire(
      {DBIsolation isolation, bool transaction: false}) async {
    final Connection connection = await _pool.connect();
    final List<String> queries = [];
    if (transaction) {
      queries.add('BEGIN;');
    }
    if (isolation != null) {
      switch (isolation) {
        case DBIsolation.serializable:
          queries.add('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;');
          break;
        case DBIsolation.repeatableRead:
          queries.add('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;');
          break;
        case DBIsolation.readCommitted:
          queries.add('SET TRANSACTION ISOLATION LEVEL READ COMMITTED;');
          break;
        case DBIsolation.readUncommitted:
          queries.add('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;');
          break;
      }
    }
    if (queries.isNotEmpty) {
      await connection.execute(queries.join());
    }
    return connection;
  }

  @override
  Future release(Connection connection, {bool abort: false}) async {
    if (connection.transactionState != TransactionState.none) {
      if (abort) {
        await connection.execute('ROLLBACK;');
      } else {
        await connection.execute('COMMIT;');
      }
    }
    connection.close();
  }
}
