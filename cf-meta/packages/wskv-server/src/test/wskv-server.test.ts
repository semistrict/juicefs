import {
  env,
  runInDurableObject,
} from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { WskvTestClient, b } from "./test-client.js";
import type { Storage } from "../schema.js";

function client(storage: DurableObjectStorage): WskvTestClient {
  return new WskvTestClient(storage as unknown as Storage);
}

/** Compute the exclusive upper bound for a single-key point read: append \x00. */
function pointKeyEnd(key: Uint8Array): Uint8Array {
  const end = new Uint8Array(key.length + 1);
  end.set(key);
  return end;
}

// ─── Basic CRUD ──────────────────────────────────────────────────────────────

describe("Basic CRUD", () => {
  it("get nonexistent key returns found=false", async () => {
    const id = env.WSKV_TEST.idFromName("crud-get-nonexistent");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      const resp = c.get(b("nokey"));
      expect(resp.found).toBe(false);
      expect(resp.ver).toBe(0);
    });
  });

  it("put then get returns correct value with ver=1", async () => {
    const id = env.WSKV_TEST.idFromName("crud-put-get");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      const putResp = c.put(b("key1"), b("val1"));
      expect(putResp.ok).toBe(true);

      const getResp = c.get(b("key1"));
      expect(getResp.found).toBe(true);
      expect(getResp.ver).toBe(1);
      expect(new TextDecoder().decode(getResp.value)).toBe("val1");
    });
  });

  it("put same key again increments to ver=2", async () => {
    const id = env.WSKV_TEST.idFromName("crud-put-twice");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key1"), b("val1"));
      c.put(b("key1"), b("val2"));

      const resp = c.get(b("key1"));
      expect(resp.found).toBe(true);
      expect(resp.ver).toBe(2);
      expect(new TextDecoder().decode(resp.value)).toBe("val2");
    });
  });

  it("delete then get returns found=false", async () => {
    const id = env.WSKV_TEST.idFromName("crud-delete");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key1"), b("val1"));
      c.del(b("key1"));

      const resp = c.get(b("key1"));
      expect(resp.found).toBe(false);
    });
  });

  it("reset wipes all data", async () => {
    const id = env.WSKV_TEST.idFromName("crud-reset");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));
      c.put(b("c"), b("3"));

      const resetResp = c.reset();
      expect(resetResp.ok).toBe(true);

      expect(c.get(b("a")).found).toBe(false);
      expect(c.get(b("b")).found).toBe(false);
      expect(c.get(b("c")).found).toBe(false);
    });
  });
});

// ─── Range Scans ─────────────────────────────────────────────────────────────

describe("Range Scans", () => {
  it("list empty range returns no entries", async () => {
    const id = env.WSKV_TEST.idFromName("range-empty");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      const resp = c.list(b("a"), b("z"));
      expect(resp.entries.length).toBe(0);
    });
  });

  it("list returns entries in sorted order", async () => {
    const id = env.WSKV_TEST.idFromName("range-sorted");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("c"), b("3"));
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));

      const resp = c.list(b("a"), b("d"));
      expect(resp.entries.length).toBe(3);
      const keys = resp.entries.map((e) => new TextDecoder().decode(e.key));
      expect(keys).toEqual(["a", "b", "c"]);
      const vals = resp.entries.map((e) => new TextDecoder().decode(e.value));
      expect(vals).toEqual(["1", "2", "3"]);
    });
  });

  it("keysOnly omits values", async () => {
    const id = env.WSKV_TEST.idFromName("range-keysonly");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));

      const resp = c.list(b("a"), b("c"), { keysOnly: true });
      expect(resp.entries.length).toBe(2);
      const keys = resp.entries.map((e) => new TextDecoder().decode(e.key));
      expect(keys).toEqual(["a", "b"]);
      // keysOnly: values should be empty
      for (const entry of resp.entries) {
        expect(entry.value.length).toBe(0);
      }
    });
  });

  it("limit truncates results", async () => {
    const id = env.WSKV_TEST.idFromName("range-limit");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));
      c.put(b("c"), b("3"));

      const resp = c.list(b("a"), b("d"), { limit: 2 });
      expect(resp.entries.length).toBe(2);
      const keys = resp.entries.map((e) => new TextDecoder().decode(e.key));
      expect(keys).toEqual(["a", "b"]);
    });
  });
});

// ─── OCC Basic ───────────────────────────────────────────────────────────────

describe("OCC - Basic", () => {
  it("read-then-write succeeds when nothing changed", async () => {
    const id = env.WSKV_TEST.idFromName("occ-basic-success");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key1"), b("val1"));

      // Read phase
      const getResp = c.get(b("key1"));
      expect(getResp.found).toBe(true);

      // Commit with the read recorded
      const commitResp = c.commit(
        [
          {
            start: b("key1"),
            end: pointKeyEnd(b("key1")),
            entries: [{ key: b("key1"), ver: getResp.ver }],
          },
        ],
        [{ key: b("key1"), value: b("val2") }],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });

  it("read key, sneaky write same key → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-basic-write-conflict");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key1"), b("val1"));

      // Read phase: ver=1
      const getResp = c.get(b("key1"));
      expect(getResp.ver).toBe(1);

      // Sneaky write bumps to ver=2
      c.put(b("key1"), b("sneaky"));

      // Commit with stale ver=1 → conflict
      const commitResp = c.commit(
        [
          {
            start: b("key1"),
            end: pointKeyEnd(b("key1")),
            entries: [{ key: b("key1"), ver: 1 }],
          },
        ],
        [{ key: b("key1"), value: b("val2") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("read key, sneaky delete → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-basic-delete-conflict");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key1"), b("val1"));

      const getResp = c.get(b("key1"));
      expect(getResp.ver).toBe(1);

      // Sneaky delete
      c.del(b("key1"));

      // Commit with stale read (entry existed at ver=1, now gone)
      const commitResp = c.commit(
        [
          {
            start: b("key1"),
            end: pointKeyEnd(b("key1")),
            entries: [{ key: b("key1"), ver: 1 }],
          },
        ],
        [{ key: b("key1"), value: b("val2") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("read nonexistent, sneaky insert → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-basic-insert-conflict");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);

      // Read phase: key doesn't exist (empty range)
      const getResp = c.get(b("key1"));
      expect(getResp.found).toBe(false);

      // Sneaky insert
      c.put(b("key1"), b("sneaky"));

      // Commit claiming the range was empty
      const commitResp = c.commit(
        [
          {
            start: b("key1"),
            end: pointKeyEnd(b("key1")),
            entries: [],
          },
        ],
        [{ key: b("key1"), value: b("val1") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });
});

// ─── OCC Phantom Reads (range-based) ────────────────────────────────────────

describe("OCC - Phantom Reads", () => {
  it("scan range, sneaky insert into range → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-phantom-insert");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("c"), b("3"));

      // Read phase: scan [a, d)
      const listResp = c.list(b("a"), b("d"));
      expect(listResp.entries.length).toBe(2);

      // Sneaky insert "b" into the range
      c.put(b("b"), b("2"));

      // Commit with stale range (only had a, c)
      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("d"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
          },
        ],
        [{ key: b("a"), value: b("updated") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("scan range, sneaky delete from range → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-phantom-delete");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));

      const listResp = c.list(b("a"), b("c"));
      expect(listResp.entries.length).toBe(2);

      // Sneaky delete "b"
      c.del(b("b"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("c"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
          },
        ],
        [{ key: b("a"), value: b("updated") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("scan empty range, sneaky insert → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-phantom-empty-insert");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);

      // Read empty range
      const listResp = c.list(b("a"), b("d"));
      expect(listResp.entries.length).toBe(0);

      // Sneaky insert into the range
      c.put(b("b"), b("sneaky"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("d"),
            entries: [],
          },
        ],
        [{ key: b("x"), value: b("outside") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("scan range, modification outside range → success", async () => {
    const id = env.WSKV_TEST.idFromName("occ-phantom-outside");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));

      // Read [a, c)
      const listResp = c.list(b("a"), b("c"));
      expect(listResp.entries.length).toBe(2);

      // Sneaky write outside the range
      c.put(b("z"), b("outside"));

      // Commit should succeed — z is outside [a, c)
      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("c"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
          },
        ],
        [{ key: b("a"), value: b("updated") }],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });
});

// ─── OCC keysOnly ────────────────────────────────────────────────────────────

describe("OCC - keysOnly", () => {
  it("keysOnly scan, value-only update → no conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-keysonly-value-update");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("b"), b("2"));

      // keysOnly scan
      const listResp = c.list(b("a"), b("c"), { keysOnly: true });
      expect(listResp.entries.length).toBe(2);

      // Sneaky value-only update (key set unchanged, ver bumps but keysOnly ignores ver)
      c.put(b("a"), b("updated"));

      // Commit with keysOnly: version changes should be ignored
      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("c"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
            keysOnly: true,
          },
        ],
        [{ key: b("b"), value: b("new-b") }],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });

  it("keysOnly scan, key insert → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-keysonly-key-insert");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));

      const listResp = c.list(b("a"), b("d"), { keysOnly: true });
      expect(listResp.entries.length).toBe(1);

      // Sneaky key insert — changes the key set
      c.put(b("b"), b("sneaky"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("d"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
            keysOnly: true,
          },
        ],
        [{ key: b("a"), value: b("new") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });
});

// ─── OCC limit ───────────────────────────────────────────────────────────────

describe("OCC - limit", () => {
  it("limit=1 read, insert before first key → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-limit-insert-before");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("b"), b("2"));
      c.put(b("c"), b("3"));

      // limit=1 scan [a, z) → only "b"
      const listResp = c.list(b("a"), b("z"), { limit: 1 });
      expect(listResp.entries.length).toBe(1);
      expect(new TextDecoder().decode(listResp.entries[0].key)).toBe("b");

      // Sneaky insert "a" — sorts before "b", would change limit=1 result
      c.put(b("a"), b("1"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("z"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
            limit: 1,
          },
        ],
        [{ key: b("d"), value: b("4") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("limit=1 read, insert after first key → no conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-limit-insert-after");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));

      // limit=1 scan [a, z) → only "a"
      const listResp = c.list(b("a"), b("z"), { limit: 1 });
      expect(listResp.entries.length).toBe(1);
      expect(new TextDecoder().decode(listResp.entries[0].key)).toBe("a");

      // Sneaky insert "b" — after "a", doesn't affect limit=1 result
      c.put(b("b"), b("2"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("z"),
            entries: listResp.entries.map((e) => ({
              key: e.key,
              ver: e.ver,
            })),
            limit: 1,
          },
        ],
        [{ key: b("c"), value: b("3") }],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });
});

// ─── OCC Point Read exactness ────────────────────────────────────────────────

describe("OCC - Point Read exactness", () => {
  it('get(key), insert key+"1" suffix → no conflict', async () => {
    const id = env.WSKV_TEST.idFromName("occ-point-suffix");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key"), b("val"));

      // Point read "key": range is [key, key\x00) via pointKeyEnd
      const getResp = c.get(b("key"));
      expect(getResp.found).toBe(true);
      expect(getResp.ver).toBe(1);

      // Insert "key1" — different key, outside [key, key\x00)
      c.put(b("key1"), b("other"));

      const commitResp = c.commit(
        [
          {
            start: b("key"),
            end: pointKeyEnd(b("key")),
            entries: [{ key: b("key"), ver: 1 }],
          },
        ],
        [{ key: b("key"), value: b("updated") }],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });

  it("get(key), update same key → conflict", async () => {
    const id = env.WSKV_TEST.idFromName("occ-point-update");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("key"), b("val"));

      const getResp = c.get(b("key"));
      expect(getResp.ver).toBe(1);

      // Sneaky update same key
      c.put(b("key"), b("sneaky"));

      const commitResp = c.commit(
        [
          {
            start: b("key"),
            end: pointKeyEnd(b("key")),
            entries: [{ key: b("key"), ver: 1 }],
          },
        ],
        [{ key: b("key"), value: b("val2") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });
});

// ─── Multiple Ranges ─────────────────────────────────────────────────────────

describe("OCC - Multiple Ranges", () => {
  it("conflict in one range → whole commit fails", async () => {
    const id = env.WSKV_TEST.idFromName("occ-multi-conflict");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("x"), b("9"));

      // Read two separate ranges
      const list1 = c.list(b("a"), b("b"));
      const list2 = c.list(b("x"), b("y"));

      // Sneaky write in second range
      c.put(b("x"), b("sneaky"));

      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("b"),
            entries: list1.entries.map((e) => ({ key: e.key, ver: e.ver })),
          },
          {
            start: b("x"),
            end: b("y"),
            entries: list2.entries.map((e) => ({ key: e.key, ver: e.ver })),
          },
        ],
        [{ key: b("a"), value: b("updated") }],
        [],
      );
      expect(commitResp.ok).toBe(false);
      expect(commitResp.error).toBe("write conflict");
    });
  });

  it("no conflicts across multiple ranges → success", async () => {
    const id = env.WSKV_TEST.idFromName("occ-multi-success");
    const stub = env.WSKV_TEST.get(id);
    await runInDurableObject(stub, (_, state) => {
      const c = client(state.storage);
      c.put(b("a"), b("1"));
      c.put(b("x"), b("9"));

      const list1 = c.list(b("a"), b("b"));
      const list2 = c.list(b("x"), b("y"));

      // No sneaky writes — everything should be fine
      const commitResp = c.commit(
        [
          {
            start: b("a"),
            end: b("b"),
            entries: list1.entries.map((e) => ({ key: e.key, ver: e.ver })),
          },
          {
            start: b("x"),
            end: b("y"),
            entries: list2.entries.map((e) => ({ key: e.key, ver: e.ver })),
          },
        ],
        [
          { key: b("a"), value: b("updated-a") },
          { key: b("x"), value: b("updated-x") },
        ],
        [],
      );
      expect(commitResp.ok).toBe(true);
    });
  });
});
