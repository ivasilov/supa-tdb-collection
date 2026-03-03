# supabase-collection

A [TanStack/db](https://tanstack.com/db/latest) collection backed by [Supabase](https://supabase.com/). It wires up Supabase PostgREST queries, mutations, and real-time subscriptions so you get a reactive state that stays in sync with your Postgres database.

## Quick test

1. Create a Next.js app with shadcn:

```bash
npx shadcn@latest init
```

2. Go to the block installer:

   <https://ui-library-git-feat-tanstack-db-gen-supabase.vercel.app/ui/docs/nextjs/tanstack-db>

3. Enter your Supabase project URL and anon key (from an existing **production** project)

4. Use the generated shadcn URL in your local project

4. Drop it into your Next.js app and run it.

---

## TanStack/db in a nutshell

TanStack/db gives you **local-first, reactive collections** that sync with a backend. You query them with plain JavaScript — no hooks or selectors needed — and every component that reads the data re-renders automatically when it changes.

### Defining a collection (is handled by the shadcn block)

```ts
import { collection } from "@tanstack/db";
import { supabaseCollectionOptions } from "supabase-collection";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

const todos = collection(
  supabaseCollectionOptions({
    tableName: "todos",
    schema: todoSchema, // any Standard Schema (zod, valibot, …)
    getKey: (todo) => todo.id,
    where: (query, item) => query.eq("id", item.id),
    supabase,
    realtime: true,
  })
);
```

### Select

```ts
import { useQuery } from "@tanstack/react-db";

// all rows
const allTodos = useQuery(todos);

// with a filter
const done = useQuery(todos, {
  where: { completed: { eq: true } },
  orderBy: { created_at: "desc" },
  limit: 10,
});
```

### Joins

TanStack/db supports live joins across collections:

```ts
const postsWithAuthor = useQuery(posts, {
  with: {
    author: {
      collection: users,
      field: "author_id",
      references: "id",
    },
  },
});
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
