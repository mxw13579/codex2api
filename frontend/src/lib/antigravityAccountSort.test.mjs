import assert from "node:assert/strict";
import test from "node:test";
import { ACCOUNT_LIST_SORT_STORAGE_KEY } from "./accountListPreferences.ts";
import {
  ANTIGRAVITY_ACCOUNT_SORT_KEYS,
  ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY,
  readAntigravityAccountSort,
  writeAntigravityAccountSort,
} from "./antigravityAccountSort.ts";

function createStorage(initial = {}) {
  const values = new Map(Object.entries(initial));
  return {
    values,
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(key, String(value));
    },
    removeItem(key) {
      values.delete(key);
    },
  };
}

function createBrokenStorage() {
  return {
    getItem() {
      throw new Error("storage unavailable");
    },
    setItem() {
      throw new Error("storage unavailable");
    },
    removeItem() {
      throw new Error("storage unavailable");
    },
  };
}

test("antigravity sort has no memory by default", () => {
  assert.equal(readAntigravityAccountSort(createStorage()), null);
});

test("antigravity sort round-trips the id column and other keys", () => {
  assert.ok(ANTIGRAVITY_ACCOUNT_SORT_KEYS.includes("id"));
  const storage = createStorage();
  writeAntigravityAccountSort({ key: "id", dir: "desc" }, storage);
  assert.deepEqual(readAntigravityAccountSort(storage), { key: "id", dir: "desc" });
  writeAntigravityAccountSort({ key: "id", dir: "asc" }, storage);
  assert.deepEqual(readAntigravityAccountSort(storage), { key: "id", dir: "asc" });
  writeAntigravityAccountSort({ key: "group", dir: "asc" }, storage);
  assert.deepEqual(readAntigravityAccountSort(storage), { key: "group", dir: "asc" });
});

test("antigravity sort uses its own key and leaves the codex sort alone", () => {
  const codexSort = '{"key":"importTime","dir":"asc"}';
  const storage = createStorage({ [ACCOUNT_LIST_SORT_STORAGE_KEY]: codexSort });
  assert.notEqual(ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY, ACCOUNT_LIST_SORT_STORAGE_KEY);
  writeAntigravityAccountSort({ key: "id", dir: "desc" }, storage);
  writeAntigravityAccountSort(null, storage);
  assert.equal(storage.values.get(ACCOUNT_LIST_SORT_STORAGE_KEY), codexSort);
});

test("restoring the antigravity default sort clears the stored value", () => {
  const storage = createStorage();
  writeAntigravityAccountSort({ key: "id", dir: "desc" }, storage);
  writeAntigravityAccountSort(null, storage);
  assert.equal(storage.values.has(ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY), false);
  assert.equal(readAntigravityAccountSort(storage), null);
});

test("invalid antigravity sort values fall back to the page default", () => {
  for (const raw of ["not json", '{"key":"importTime","dir":"asc"}', "{}", "null"]) {
    const storage = createStorage({ [ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY]: raw });
    assert.equal(readAntigravityAccountSort(storage), null);
  }
  const storage = createStorage({
    [ANTIGRAVITY_ACCOUNT_SORT_STORAGE_KEY]: '{"key":"id","dir":"sideways"}',
  });
  assert.deepEqual(readAntigravityAccountSort(storage), { key: "id", dir: "desc" });
});

test("antigravity sort tolerates unavailable storage", () => {
  const storage = createBrokenStorage();
  assert.equal(readAntigravityAccountSort(storage), null);
  assert.doesNotThrow(() => writeAntigravityAccountSort({ key: "id", dir: "asc" }, storage));
  assert.doesNotThrow(() => writeAntigravityAccountSort(null, storage));
});
