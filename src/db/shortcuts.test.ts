import { describe, expect, test } from 'vitest';
import { Default, raw, sql } from './core';
import {
  constraint,
  count,
  deletes,
  doNothing,
  insert,
  select,
  selectOne,
  update,
  upsert
} from './shortcuts';

// Mock table names as simple strings for testing
const usersTable = 'users' as any;

describe('shortcuts.ts query builders', () => {
  describe('insert', () => {
    test('generates simple insert query', () => {
      const query = insert(usersTable, { name: 'John', age: 30 });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("age", "name") VALUES ($1, $2) RETURNING to_jsonb("users".*) AS result",
          "values": [
            30,
            "John",
          ],
        }
      `);
    });

    test('handles array of values', () => {
      const query = insert(usersTable, [
        { name: 'John', age: 30 },
        { name: 'Jane', age: 25 }
      ]);
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("age", "name") VALUES ($1, $2), ($3, $4) RETURNING to_jsonb("users".*) AS result",
          "values": [
            30,
            "John",
            25,
            "Jane",
          ],
        }
      `);
    });

    test('handles empty array', () => {
      const query = insert(usersTable, []);
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "/* marked no-op: won't hit DB unless forced -> */ INSERT INTO "users" SELECT null WHERE false",
          "values": [],
        }
      `);
      expect(query.noop).toBe(true);
    });

    test('supports returning specific columns', () => {
      const query = insert(usersTable, { name: 'John' }, {
        returning: ['id', 'name'] as any
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("name") VALUES ($1) RETURNING jsonb_build_object($2::text, "id", $3::text, "name") AS result",
          "values": [
            "John",
            "id",
            "name",
          ],
        }
      `);
    });
  });

  describe('update', () => {
    test('generates update query with where clause', () => {
      const query = update(
        usersTable,
        { name: 'Jane' },
        { id: 1 }
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "UPDATE "users" SET ("name") = ROW($1) WHERE ("id" = $2) RETURNING to_jsonb("users".*) AS result",
          "values": [
            "Jane",
            1,
          ],
        }
      `);
    });

    test('works with SQL fragment where clause', () => {
      const query = update(
        usersTable,
        { active: true },
        sql`age > ${raw(`18`)}`
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "UPDATE "users" SET ("active") = ROW($1) WHERE age > 18 RETURNING to_jsonb("users".*) AS result",
          "values": [
            true,
          ],
        }
      `);
    });
  });

  describe('deletes', () => {
    test('generates delete query', () => {
      const query = deletes(usersTable, { id: 1 });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "DELETE FROM "users" WHERE ("id" = $1) RETURNING to_jsonb("users".*) AS result",
          "values": [
            1,
          ],
        }
      `);
    });

    test('works with complex where conditions', () => {
      const query = deletes(usersTable, sql`age < ${raw(`18`)} OR active = ${raw(`false`)}`);
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "DELETE FROM "users" WHERE age < 18 OR active = false RETURNING to_jsonb("users".*) AS result",
          "values": [],
        }
      `);
    });
  });

  describe('select', () => {
    test('generates basic select query', () => {
      const query = select(usersTable, { active: true });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT coalesce(jsonb_agg(result), '[]') AS result FROM (SELECT to_jsonb("users".*) AS result FROM "users" WHERE ("active" = $1)) AS "sq_users"",
          "values": [
            true,
          ],
        }
      `);
    });

    test('supports ordering', () => {
      const query = select(usersTable, { active: true }, {
        order: { by: sql`created_at`, direction: 'DESC' }
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT coalesce(jsonb_agg(result), '[]') AS result FROM (SELECT to_jsonb("users".*) AS result FROM "users" WHERE ("active" = $1) ORDER BY created_at DESC) AS "sq_users"",
          "values": [
            true,
          ],
        }
      `);
    });

    test('supports limit and offset', () => {
      const query = select(usersTable, { active: true }, {
        limit: 10,
        offset: 20
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT coalesce(jsonb_agg(result), '[]') AS result FROM (SELECT to_jsonb("users".*) AS result FROM "users" WHERE ("active" = $1) LIMIT $2 OFFSET $3) AS "sq_users"",
          "values": [
            true,
            10,
            20,
          ],
        }
      `);
    });

    test('supports distinct', () => {
      const query = select(usersTable, { active: true }, {
        distinct: true
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT coalesce(jsonb_agg(result), '[]') AS result FROM (SELECT DISTINCT to_jsonb("users".*) AS result FROM "users" WHERE ("active" = $1)) AS "sq_users"",
          "values": [
            true,
          ],
        }
      `);
    });
  });

  describe('selectOne', () => {
    test('automatically adds LIMIT 1', () => {
      const query = selectOne(usersTable, { id: 1 });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT to_jsonb("users".*) AS result FROM "users" WHERE ("id" = $1) LIMIT $2",
          "values": [
            1,
            1,
          ],
        }
      `);
    });
  });

  describe('count', () => {
    test('generates count query', () => {
      const query = count(usersTable, { active: true });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT count("users".*) AS result FROM "users" WHERE ("active" = $1)",
          "values": [
            true,
          ],
        }
      `);
    });

    test('can count specific columns', () => {
      const query = count(usersTable, { active: true }, {
        columns: ['id'] as any
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "SELECT count("id") AS result FROM "users" WHERE ("active" = $1)",
          "values": [
            true,
          ],
        }
      `);
    });
  });

  describe('upsert', () => {
    test('generates upsert query with conflict target', () => {
      const query = upsert(
        usersTable,
        { email: 'john@example.com', name: 'John' },
        'email' as any
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("email", "name") VALUES ($1, $2) ON CONFLICT ("email") DO UPDATE SET ("email", "name") = ROW(EXCLUDED."email", EXCLUDED."name") RETURNING to_jsonb("users".*) || jsonb_build_object('$action', CASE xmax WHEN 0 THEN 'INSERT' ELSE 'UPDATE' END) AS result",
          "values": [
            "john@example.com",
            "John",
          ],
        }
      `);
    });

    test('works with constraint', () => {
      const query = upsert(
        usersTable,
        { email: 'john@example.com', name: 'John' },
        constraint('users_email_key' as any)
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("email", "name") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT "users_email_key" DO UPDATE SET ("email", "name") = ROW(EXCLUDED."email", EXCLUDED."name") RETURNING to_jsonb("users".*) || jsonb_build_object('$action', CASE xmax WHEN 0 THEN 'INSERT' ELSE 'UPDATE' END) AS result",
          "values": [
            "john@example.com",
            "John",
          ],
        }
      `);
    });

    test('supports doNothing for updates', () => {
      const query = upsert(
        usersTable,
        { email: 'john@example.com', name: 'John' },
        'email' as any,
        { updateColumns: doNothing }
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("email", "name") VALUES ($1, $2) ON CONFLICT ("email") DO NOTHING RETURNING to_jsonb("users".*) || jsonb_build_object('$action', CASE xmax WHEN 0 THEN 'INSERT' ELSE 'UPDATE' END) AS result",
          "values": [
            "john@example.com",
            "John",
          ],
        }
      `);
    });

    test('supports custom update values', () => {
      const query = upsert(
        usersTable,
        { email: 'john@example.com', name: 'John', count: 1 },
        'email' as any,
        { updateValues: { count: sql`users.count + 1` } }
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("count", "email", "name") VALUES ($1, $2, $3) ON CONFLICT ("email") DO UPDATE SET ("email", "name", "count") = ROW(EXCLUDED."email", EXCLUDED."name", users.count + 1) RETURNING to_jsonb("users".*) || jsonb_build_object('$action', CASE xmax WHEN 0 THEN 'INSERT' ELSE 'UPDATE' END) AS result",
          "values": [
            1,
            "john@example.com",
            "John",
          ],
        }
      `);
    });
  });

  describe('runResultTransform', () => {
    test('insert single value transform', () => {
      const query = insert(usersTable, { name: 'John' });
      const mockResult = {
        rows: [{ result: { id: 1, name: 'John' } }],
        command: 'INSERT',
        rowCount: 1,
        oid: 0,
        fields: []
      } as any;

      const transformed = query.runResultTransform(mockResult);
      expect(transformed).toEqual({ id: 1, name: 'John' });
    });

    test('insert array transform', () => {
      const query = insert(usersTable, [{ name: 'John' }, { name: 'Jane' }]);
      const mockResult = {
        rows: [
          { result: { id: 1, name: 'John' } },
          { result: { id: 2, name: 'Jane' } }
        ],
        command: 'INSERT',
        rowCount: 2,
        oid: 0,
        fields: []
      } as any;

      const transformed = query.runResultTransform(mockResult);
      expect(transformed).toEqual([
        { id: 1, name: 'John' },
        { id: 2, name: 'Jane' }
      ]);
    });

    test('count transform returns number', () => {
      const query = count(usersTable, {});
      const mockResult = {
        rows: [{ result: '42' }],
        command: 'SELECT',
        rowCount: 1,
        oid: 0,
        fields: []
      } as any;

      const transformed = query.runResultTransform(mockResult);
      expect(transformed).toBe(42);
      expect(typeof transformed).toBe('number');
    });
  });

  describe('edge cases', () => {
    test('handles Default values in insert', () => {
      const query = insert(usersTable, {
        name: 'John',
        created_at: Default
      });
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("created_at", "name") VALUES (DEFAULT, $1) RETURNING to_jsonb("users".*) AS result",
          "values": [
            "John",
          ],
        }
      `);
    });

    test('empty update columns in upsert creates DO NOTHING', () => {
      const query = upsert(
        usersTable,
        { email: 'test@example.com' },
        'email' as any,
        { updateColumns: [] }
      );
      const compiled = query.compile();

      expect(compiled).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "users" ("email") VALUES ($1) ON CONFLICT ("email") DO NOTHING RETURNING to_jsonb("users".*) || jsonb_build_object('$action', CASE xmax WHEN 0 THEN 'INSERT' ELSE 'UPDATE' END) AS result",
          "values": [
            "test@example.com",
          ],
        }
      `);
    });
  });
});