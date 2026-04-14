# supabase-collection

A [TanStack/db](https://tanstack.com/db/latest) collection backed by [Supabase](https://supabase.com/). It wires up Supabase PostgREST queries, mutations, and real-time subscriptions so you get a reactive state that stays in sync with your Postgres database.

## Quick test

1. Go to the block installer:

   <https://ui-library-git-feat-tanstack-db-gen-supabase.vercel.app/ui/docs/nextjs/tanstack-db>

2. Enter your Supabase project URL and anon key (from an existing **production** project)

3. Use the generated shadcn URL in your local project (it works even if you don't have a shadcn-initialized project)

4. Drop it into your Next.js app and run it.

---

## TanStack/db in a nutshell

TanStack/db gives you **local-first, reactive collections** that sync with a backend. You query them with plain JavaScript — no hooks or selectors needed — and every component that reads the data re-renders automatically when it changes.

### Defining a collection 

```ts
import { createCollection, supabaseCollectionOptions } from "supa-tdb-collection";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

const todos = createCollection(
  supabaseCollectionOptions({
    tableName: "todos",
    schema: todoSchema, // any Standard Schema (zod, valibot, …)
    getKey: (todo) => todo.id,
    where: (query, item) => query.eq("id", item.id),
    supabase,
    realtime: true,
  }),
);
```

### Select

```ts
import { useLiveQuery, eq } from "supa-tdb-collection";

// all rows
const allTodos = useLiveQuery((q) => q.from({ todo: todos }));

// with a filter
const done = useLiveQuery((q) =>
  q
    .from({ todo: todos })
    .where(({ todo }) => eq(todo.completed, true))
    .orderBy(({ todo }) => todo.created_at, "desc")
    .limit(10)
);
```

### Joins

TanStack/db supports live joins across collections:

```ts
import { useLiveQuery, eq } from "supa-tdb-collection";

const postsWithAuthor = useLiveQuery((q) =>
  q
    .from({ post: posts })
    .join({ author: users }, ({ post, author }) => eq(post.author_id, author.id))
);
// postsWithAuthor[0].author.name
```

### Insert

```ts
todos.insert({ title: "Buy milk", completed: false });
```

### Update

```ts
todos.update(todo, { completed: true });
```

### Delete

```ts
todos.delete(todo);
```

Mutations are **optimistic** — the local collection updates immediately and syncs to Supabase in the background. If the server rejects a change it rolls back automatically.

### One-shot queries

Use `queryOnce` when you need a non-reactive, single fetch — for example in server components, API routes, or form submissions where live updates aren't needed.

```ts
import { queryOnce, eq } from "supa-tdb-collection";

// fetch all matching rows
const completedTodos = await queryOnce(
  (q) => q.from({ todo: todos }).where(({ todo }) => eq(todo.completed, true)),
  supabase
);

// fetch a single row
const todo = await queryOnce(
  (q) =>
    q
      .from({ todo: todos })
      .where(({ todo }) => eq(todo.id, todoId))
      .findOne(),
  supabase
);
```

Unlike `useLiveQuery`, `queryOnce` does not subscribe to changes — it issues one request and resolves with the result.

### What gets pushed to PostgREST

| Feature                                              | Server-side | Notes                                                                                          |
| ---------------------------------------------------- | ----------- | ---------------------------------------------------------------------------------------------- |
| `FROM`                                               | Yes         | Maps to PostgREST table endpoint                                                               |
| `WHERE` (eq, gt, gte, lt, lte, inArray, not, isNull) | Yes         | Translated to PostgREST syntax                                                                 |
| `AND` (multiple conditions / chained `.where`)       | Yes         | Translated to PostgREST syntax                                                                 |
| `ORDER BY` (on source columns)                       | Yes         | Translated to PostgREST syntax                                                                 |
| `LIMIT`                                              | Yes         | Translated to PostgREST syntax                                                                 |
| `JOIN`                                               | Yes         | Each table is fetched separately; the join key is pushed as an `in` filter on the second query |

### What runs client-side

These operations always fetch the full table (`select=*`) and are evaluated in-memory:

- `SELECT` column subsets, renaming, and computed fields (`upper`, `lower`, `concat`, `length`, `add`, `coalesce`)
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `GROUP BY` and `HAVING`
- `DISTINCT`
- `ORDER BY` on computed fields

### Limitations and future development

- All columns are fetched for every row — specific column selection can't be pushed to PostgREST
- `GROUP BY`, aggregates, and computed `SELECT` expressions are evaluated client-side. All rows needed for the operation are fetched — `WHERE` filters are still pushed to PostgREST, so only filtered rows are transferred. These operations are inherently limited to a single collection and cannot be pushed across joins.
  - Exception: aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) are pushed to PostgREST when using `queryOnce`.
- `OR` conditions and nested `AND`/`OR` are not yet supported by this library (but doable)
- `OFFSET` is not yet supported (pagination must use `LIMIT` only for now), bug in Tanstack DB
- `findOne` fetches the whole table, doesn't use `LIMIT=1`, bug in Tanstack DB
